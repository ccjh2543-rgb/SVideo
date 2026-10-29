package feed

import (
	"context"

	domainfeed "feedsystem_video_go/internal/domain/feed"
)

const defaultHeavyPushBatchSize = 500

// HeavyFollowerReader 读取某大 V 的重度粉丝集合。
type HeavyFollowerReader interface {
	ScanHeavyFollowers(ctx context.Context, bigV uint, cursor uint64, count int64) ([]uint, uint64, error)
}

// HeavyPushService 大 V 发布时向重度粉丝 Inbox 追加视频。
// 只服务少数重度用户，避免全量粉丝写扩散。
type HeavyPushService struct {
	followers HeavyFollowerReader
	timeline  domainfeed.TimelineRepository
	batchSize int
}

// NewHeavyPushService 创建重度粉丝推送服务。
func NewHeavyPushService(followers HeavyFollowerReader, timeline domainfeed.TimelineRepository, batchSize int) *HeavyPushService {
	if batchSize <= 0 {
		batchSize = defaultHeavyPushBatchSize
	}
	return &HeavyPushService{followers: followers, timeline: timeline, batchSize: batchSize}
}

// Push 分批把视频写入重度粉丝 Inbox；失败返回错误，由上游消息重试。
func (s *HeavyPushService) Push(ctx context.Context, videoID, authorID uint, score int64) error {
	if s == nil || s.followers == nil || s.timeline == nil || videoID == 0 || authorID == 0 {
		return nil
	}

	var cursor uint64
	for {
		followerIDs, next, err := s.followers.ScanHeavyFollowers(ctx, authorID, cursor, int64(s.batchSize))
		if err != nil {
			return err
		}
		if len(followerIDs) > 0 {
			if err := s.timeline.PushInboxBatch(ctx, followerIDs, videoID, score); err != nil {
				return err
			}
		}
		if next == 0 {
			return nil
		}
		cursor = next
	}
}
