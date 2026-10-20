package video

import (
	"context"
	"fmt"
	"strconv"
	"time"

	rediscache "feedsystem_video_go/internal/infrastructure/cache/redis"
)

// PopularityCache 热度缓存。
type PopularityCache struct {
	client *rediscache.Client
}

// NewPopularityCache 创建热度缓存。
func NewPopularityCache(client *rediscache.Client) *PopularityCache {
	return &PopularityCache{client: client}
}

// Update 更新小时级热榜并删除视频详情缓存。
func (c *PopularityCache) Update(ctx context.Context, videoID uint, change int64) error {
	if c == nil || c.client == nil || videoID == 0 || change == 0 {
		return nil
	}

	_ = c.client.Del(context.Background(), fmt.Sprintf("video:detail:id=%d", videoID))

	now := time.Now().UTC().Truncate(time.Hour)
	windowKey := "hot:video:1h:" + now.Format("2006010215")
	member := strconv.FormatUint(uint64(videoID), 10)

	_ = c.client.ZincrBy(ctx, windowKey, member, float64(change))
	_ = c.client.Expire(ctx, windowKey, 48*time.Hour)
	return nil
}
