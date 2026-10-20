package cache

import (
	"context"
	"encoding/json"
	"time"

	rediscache "feedsystem_video_go/internal/infrastructure/cache/redis"
)

// LogicalEntry 逻辑过期缓存包装。
// Data 保存业务 JSON，ExpireAt 是逻辑过期时间（毫秒时间戳）。
type LogicalEntry struct {
	Data     json.RawMessage `json:"data"`
	ExpireAt int64           `json:"expire_at"`
}

// GetLogical 读取逻辑过期缓存。
// 返回值：
//   - data：业务 JSON
//   - expired：逻辑上是否已过期
//   - found：缓存是否存在且可解析
func GetLogical(ctx context.Context, client *rediscache.Client, key string) (data []byte, expired bool, found bool, err error) {
	if client == nil {
		return nil, false, false, nil
	}
	raw, err := client.GetBytes(ctx, key)
	if err != nil {
		if rediscache.IsMiss(err) {
			return nil, false, false, nil
		}
		return nil, false, false, err
	}

	var entry LogicalEntry
	if err := json.Unmarshal(raw, &entry); err != nil {
		return nil, false, false, nil
	}
	expired = time.Now().UnixMilli() >= entry.ExpireAt
	return entry.Data, expired, true, nil
}

// SetLogical 写入逻辑过期缓存。
// physicalTTL 必须大于 logicalTTL，保证逻辑过期后旧值仍然存在，可供降级返回。
func SetLogical(
	ctx context.Context,
	client *rediscache.Client,
	key string,
	data []byte,
	logicalTTL time.Duration,
	physicalTTL time.Duration,
) error {
	if client == nil {
		return nil
	}
	if physicalTTL <= logicalTTL {
		physicalTTL = logicalTTL * 6
	}
	entry := LogicalEntry{
		Data:     data,
		ExpireAt: time.Now().Add(logicalTTL).UnixMilli(),
	}
	raw, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	return client.SetBytes(ctx, key, raw, physicalTTL)
}
