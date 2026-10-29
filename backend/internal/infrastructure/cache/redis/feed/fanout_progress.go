package feed

import (
	"context"
	"encoding/json"
	"time"

	rediscache "feedsystem_video_go/internal/infrastructure/cache/redis"
)

const (
	// fanoutProgressTTL 写扩散进度保留时间，覆盖 MQ 重投与 DLQ 重放窗口。
	fanoutProgressTTL = 24 * time.Hour
)

// fanoutProgress 进度载荷。cursor 为已写入的最后一个粉丝 ID。
type fanoutProgress struct {
	Cursor uint `json:"cursor"`
	Done   bool `json:"done"`
}

// FanoutProgressStore 写扩散断点续传进度的 Redis 实现。
//
// feed:fanout:progress:{eventID} 记录 cursor/done，
// 写扩散中途失败后，消息重投可以从上次游标继续，而不是从第一个粉丝重跑。
type FanoutProgressStore struct {
	cache *rediscache.Client
}

// NewFanoutProgressStore 创建写扩散进度存储。
func NewFanoutProgressStore(cache *rediscache.Client) *FanoutProgressStore {
	return &FanoutProgressStore{cache: cache}
}

// Load 读取进度；不存在时返回零值，表示从第一个粉丝开始。
func (s *FanoutProgressStore) Load(ctx context.Context, eventID string) (uint, bool, error) {
	if s == nil || s.cache == nil || eventID == "" {
		return 0, false, nil
	}
	raw, err := s.cache.GetBytes(ctx, fanoutProgressKey(eventID))
	if err != nil || len(raw) == 0 {
		return 0, false, err
	}
	var progress fanoutProgress
	if err := json.Unmarshal(raw, &progress); err != nil {
		// 进度损坏按未开始处理，写扩散本身是幂等的（ZADD）。
		return 0, false, nil
	}
	return progress.Cursor, progress.Done, nil
}

// Save 保存进度。
func (s *FanoutProgressStore) Save(ctx context.Context, eventID string, cursor uint, done bool) error {
	if s == nil || s.cache == nil || eventID == "" {
		return nil
	}
	raw, err := json.Marshal(fanoutProgress{Cursor: cursor, Done: done})
	if err != nil {
		return err
	}
	return s.cache.SetBytes(ctx, fanoutProgressKey(eventID), raw, fanoutProgressTTL)
}

// fanoutProgressKey 生成进度 key。
func fanoutProgressKey(eventID string) string {
	return "feed:fanout:progress:" + eventID
}
