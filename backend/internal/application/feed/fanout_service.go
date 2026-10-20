// Package feed 是 Feed 相关应用服务，负责推拉结合写扩散和关注流聚合。
package feed

import (
	"context"
	"time"

	domaincreator "feedsystem_video_go/internal/domain/creator"
	domainfeed "feedsystem_video_go/internal/domain/feed"
)

const defaultFanoutBatchSize = 500

// FanoutService 视频发布后的写扩散服务。
//
// 小 V：向粉丝 Inbox 推送视频 ID。
// 大 V：只写自己的 Outbox，由粉丝读时拉取。
type FanoutService struct {
	creators  domaincreator.Repository
	followers domainfeed.FollowerReader
	timeline  domainfeed.TimelineRepository
	threshold int64
	batchSize int
}

// NewFanoutService 创建写扩散服务。
func NewFanoutService(
	creators domaincreator.Repository,
	followers domainfeed.FollowerReader,
	timeline domainfeed.TimelineRepository,
	threshold int64,
	batchSize int,
) *FanoutService {
	if threshold <= 0 {
		threshold = domaincreator.BigVFansThreshold
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
