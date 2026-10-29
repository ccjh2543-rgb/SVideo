// Package feed 是 Feed 相关应用服务，负责推拉结合写扩散和关注流聚合。
package feed

import (
	"context"
	"log"
	"time"

	domainfeed "feedsystem_video_go/internal/domain/feed"
	domainsocial "feedsystem_video_go/internal/domain/social"
)

const defaultFanoutBatchSize = 500

// defaultLargeFanoutThreshold 小 V 粉丝数达到该值时转独立队列扩散。
// 低于大 V 阈值，用来把“粉丝多但还没到大 V”的作者从普通写扩散里隔离出来。
const defaultLargeFanoutThreshold int64 = 10000

// HeavyPushPublisher 投递大 V 重度粉丝推送任务。
// 拆成独立队列后，大 V 发布的写扩散不再被重度粉丝推送拖慢。
type HeavyPushPublisher interface {
	PublishHeavyPush(ctx context.Context, eventID string, videoID, authorID uint, score int64) error
}

// LargeFanoutPublisher 投递大粉丝量作者的写扩散任务。
type LargeFanoutPublisher interface {
	PublishLargeFanout(ctx context.Context, eventID string, videoID, authorID uint, score int64) error
}

// FanoutService 视频发布后的写扩散服务。
//
// 小 V：向粉丝 Inbox 推送视频 ID；粉丝量超过阈值时转独立队列。
// 大 V：只写自己的 Outbox，由粉丝读时拉取；重度粉丝推送通过独立队列异步执行。
type FanoutService struct {
	creators             domainsocial.StatsRepository
	followers            domainfeed.FollowerReader
	timeline             domainfeed.TimelineRepository
	threshold            int64
	batchSize            int
	largeFanoutThreshold int64
	heavy                *HeavyPushService
	heavyPublisher       HeavyPushPublisher
	largePublisher       LargeFanoutPublisher
	progress             FanoutProgressStore
}

// NewFanoutService 创建写扩散服务。
func NewFanoutService(
	creators domainsocial.StatsRepository,
	followers domainfeed.FollowerReader,
	timeline domainfeed.TimelineRepository,
	threshold int64,
	batchSize int,
) *FanoutService {
	if threshold <= 0 {
		threshold = domainsocial.BigVFansThreshold
	}
	if batchSize <= 0 {
		batchSize = defaultFanoutBatchSize
	}
	return &FanoutService{
		creators:             creators,
		followers:            followers,
		timeline:             timeline,
		threshold:            threshold,
		batchSize:            batchSize,
		largeFanoutThreshold: defaultLargeFanoutThreshold,
	}
}

// SetHeavyPush 注入重度粉丝推送服务，作为 MQ 不可用时的同步兜底。
func (s *FanoutService) SetHeavyPush(heavy *HeavyPushService) {
	if s == nil {
		return
	}
	s.heavy = heavy
}

// SetHeavyPushPublisher 注入重度推送队列发布器。
// 注入后大 V 分支只投递任务，实际推送由 HeavyPushWorker 异步执行。
func (s *FanoutService) SetHeavyPushPublisher(publisher HeavyPushPublisher) {
	if s == nil {
		return
	}
	s.heavyPublisher = publisher
}

// SetLargeFanoutPublisher 注入大粉丝量写扩散队列发布器。
// 注入后粉丝数超过阈值的作者只投递任务，由 LargeFanoutWorker 异步扩散。
func (s *FanoutService) SetLargeFanoutPublisher(publisher LargeFanoutPublisher) {
	if s == nil {
		return
	}
	s.largePublisher = publisher
}

// SetFanoutProgress 注入写扩散进度存储，用于断点续传。
func (s *FanoutService) SetFanoutProgress(store FanoutProgressStore) {
	if s == nil {
		return
	}
	s.progress = store
}

// Handle 处理一条视频发布事件。
// eventID 为 outbox 消息 ID，作为重度推送任务的幂等键。
func (s *FanoutService) Handle(ctx context.Context, eventID string, videoID uint, authorID uint, createTime time.Time) error {
	if videoID == 0 || authorID == 0 {
		return nil
	}
	if s.creators == nil || s.followers == nil || s.timeline == nil {
		return nil
	}

	if err := s.creators.UpsertOnPublish(ctx, authorID, createTime); err != nil {
		return err
	}

	score := createTime.UnixMilli()
	// 所有作者都维护 Outbox：大 V 用于读时拉取，小 V 用于新关注回填。
	if err := s.timeline.PushOutbox(ctx, authorID, videoID, score); err != nil {
		return err
	}

	stats, err := s.creators.Get(ctx, authorID)
	if err != nil {
		return err
	}
	if stats.IsBig || stats.FansCount >= s.threshold {
		// 大 V 不做全量写扩散：任务投递到独立队列，由 HeavyPushWorker 推送重度粉丝。
		if s.heavyPublisher != nil {
			return s.heavyPublisher.PublishHeavyPush(ctx, eventID, videoID, authorID, score)
		}
		// 未接入队列时退回同步推送，保证功能不缺失。
		if s.heavy != nil {
			return s.heavy.Push(ctx, videoID, authorID, score)
		}
		return nil
	}

	// 粉丝量超过阈值的小 V 转到独立队列，避免一次大扩散占满普通写扩散的消费能力。
	if s.largePublisher != nil && stats.FansCount >= s.largeFanoutThreshold {
		return s.largePublisher.PublishLargeFanout(ctx, eventID, videoID, authorID, score)
	}

	return s.FanoutFollowers(ctx, eventID, videoID, authorID, score)
}

// FanoutFollowers 把视频分批写入作者全部粉丝的 Inbox，支持断点续传。
// score 为视频发布时间毫秒时间戳；eventID 用于读取/保存进度。
func (s *FanoutService) FanoutFollowers(ctx context.Context, eventID string, videoID, authorID uint, score int64) error {
	if videoID == 0 || authorID == 0 {
		return nil
	}
	if s.followers == nil || s.timeline == nil {
		return nil
	}

	var afterFollowerID uint
	if s.progress != nil && eventID != "" {
		cursor, done, err := s.progress.Load(ctx, eventID)
		if err != nil {
			// 进度读取失败不影响正确性，从头发送即可；ZADD 幂等。
			log.Printf("fanout: load progress failed event_id=%s: %v", eventID, err)
		} else {
			if done {
				return nil
			}
			afterFollowerID = cursor
		}
	}

	for {
		followerIDs, err := s.followers.ListFollowerIDsAfter(ctx, authorID, afterFollowerID, s.batchSize)
		if err != nil {
			return err
		}
		if len(followerIDs) == 0 {
			s.saveProgress(ctx, eventID, afterFollowerID, true)
			return nil
		}
		if err := s.timeline.PushInboxBatch(ctx, followerIDs, videoID, score); err != nil {
			return err
		}
		afterFollowerID = followerIDs[len(followerIDs)-1]
		if len(followerIDs) < s.batchSize {
			s.saveProgress(ctx, eventID, afterFollowerID, true)
			return nil
		}
		s.saveProgress(ctx, eventID, afterFollowerID, false)
	}
}

// saveProgress 保存写扩散进度，失败只记录日志：重投会从更早的断点重跑，结果仍然幂等。
func (s *FanoutService) saveProgress(ctx context.Context, eventID string, cursor uint, done bool) {
	if s.progress == nil || eventID == "" {
		return
	}
	if err := s.progress.Save(ctx, eventID, cursor, done); err != nil {
		log.Printf("fanout: save progress failed event_id=%s cursor=%d: %v", eventID, cursor, err)
	}
}
