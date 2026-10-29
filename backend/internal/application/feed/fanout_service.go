// Package feed 是 Feed 相关应用服务，负责推拉结合写扩散和关注流聚合。
package feed

import (
	"context"
	"time"

	domainfeed "feedsystem_video_go/internal/domain/feed"
	domainsocial "feedsystem_video_go/internal/domain/social"
)

const defaultFanoutBatchSize = 500

// FanoutService 视频发布后的写扩散服务。
//
// 小 V：向粉丝 Inbox 推送视频 ID。
// 大 V：只写自己的 Outbox，由粉丝读时拉取。
type FanoutService struct {
	creators  domainsocial.StatsRepository
	followers domainfeed.FollowerReader
	timeline  domainfeed.TimelineRepository
	threshold int64
	batchSize int
	heavy     *HeavyPushService
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
		creators:  creators,
		followers: followers,
		timeline:  timeline,
		threshold: threshold,
		batchSize: batchSize,
	}
}

// SetHeavyPush 注入重度粉丝推送服务；未注入时保持原有大 V 纯拉模式。
func (s *FanoutService) SetHeavyPush(heavy *HeavyPushService) {
	if s == nil {
		return
	}
	s.heavy = heavy
}

// Handle 处理一条视频发布事件。
func (s *FanoutService) Handle(ctx context.Context, videoID uint, authorID uint, createTime time.Time) error {
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
		// 大 V 不做全量写扩散，但向少数重度粉丝选择性推送。
		if s.heavy != nil {
			return s.heavy.Push(ctx, videoID, authorID, score)
		}
		return nil
	}

	var afterFollowerID uint
	for {
		followerIDs, err := s.followers.ListFollowerIDsAfter(ctx, authorID, afterFollowerID, s.batchSize)
		if err != nil {
			return err
		}
		if len(followerIDs) == 0 {
			return nil
		}
		if err := s.timeline.PushInboxBatch(ctx, followerIDs, videoID, score); err != nil {
			return err
		}
		afterFollowerID = followerIDs[len(followerIDs)-1]
		if len(followerIDs) < s.batchSize {
			return nil
		}
	}
}
