// Package feed 提供 Feed 上下文的 Redis 缓存实现。
package feed

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	domainvideo "feedsystem_video_go/internal/domain/video"
	cachelib "feedsystem_video_go/internal/infrastructure/cache"
	rediscache "feedsystem_video_go/internal/infrastructure/cache/redis"
	"feedsystem_video_go/internal/infrastructure/resilience"

	gocache "github.com/patrickmn/go-cache"
)

const (
	localVideoTTL = 3 * time.Second
	localCleanup  = 5 * time.Second
	redisVideoTTL = time.Hour
)

// DetailCache 视频实体多级缓存实现：L1 进程内缓存 + L2 Redis。
type DetailCache struct {
	cache *rediscache.Client
	local *gocache.Cache
	guard *resilience.Guard
}

// NewDetailCache 创建视频实体缓存。
func NewDetailCache(cache *rediscache.Client, guard *resilience.Guard) *DetailCache {
	return &DetailCache{
		cache: cache,
		local: gocache.New(localVideoTTL, localCleanup),
		guard: guard,
	}
}

// GetLocal 读取 L1 本地缓存。
func (c *DetailCache) GetLocal(id uint) (domainvideo.Video, bool) {
	if c == nil || c.local == nil {
		return domainvideo.Video{}, false
	}
	value, found := c.local.Get(videoEntityKey(id))
	if !found {
		return domainvideo.Video{}, false
	}
	video, ok := value.(domainvideo.Video)
	if !ok {
		return domainvideo.Video{}, false
	}
	return video, true
}

// SetLocal 写入 L1 本地缓存。
func (c *DetailCache) SetLocal(id uint, video domainvideo.Video) {
	if c == nil || c.local == nil || id == 0 {
		return
	}
	c.local.Set(videoEntityKey(id), video, localVideoTTL)
}

// MGet 批量读取 L2 Redis；Redis 不可用时返回错误，由调用方降级到 MySQL。
func (c *DetailCache) MGet(ctx context.Context, ids []uint) (map[uint]domainvideo.Video, error) {
	if c == nil || c.cache == nil {
		return nil, fmt.Errorf("redis cache disabled")
	}
	if len(ids) == 0 {
		return map[uint]domainvideo.Video{}, nil
	}
	// 熔断打开时直接返回错误，由应用层降级到 MySQL。
	if c.guard != nil && !c.guard.Allow("redis-detail") {
		return nil, fmt.Errorf("redis detail circuit open")
	}

	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = videoEntityKey(id)
	}

	cacheCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	results, err := c.cache.MGet(cacheCtx, keys...)
	if err != nil {
		if c.guard != nil {
			c.guard.Record("redis-detail", err)
		}
		return nil, err
	}
	if c.guard != nil {
		c.guard.Record("redis-detail", nil)
	}

	hits := make(map[uint]domainvideo.Video, len(ids))
	for i, result := range results {
		if result == nil {
			continue
		}
		str, ok := result.(string)
		if !ok {
			continue
		}
		var video domainvideo.Video
		if err := json.Unmarshal([]byte(str), &video); err != nil {
			continue
		}
		hits[ids[i]] = video
	}
	return hits, nil
}

// Set 异步回填 L2 Redis，并给 TTL 加随机抖动，避免缓存雪崩。
func (c *DetailCache) Set(ctx context.Context, video domainvideo.Video) {
	if c == nil || c.cache == nil || video.ID == 0 {
		return
	}
	payload, err := json.Marshal(video)
	if err != nil {
		return
	}
	key := videoEntityKey(video.ID)
	go func() {
		setCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		_ = c.cache.SetBytes(setCtx, key, payload, cachelib.Jitter(redisVideoTTL, 0.2))
	}()
}

// videoEntityKey 视频实体缓存 key。
func videoEntityKey(id uint) string {
	return fmt.Sprintf("video:entity:%d", id)
}
