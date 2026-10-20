// Package video 提供视频上下文的 Redis 缓存实现。
package video

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	domainvideo "feedsystem_video_go/internal/domain/video"
	cachelib "feedsystem_video_go/internal/infrastructure/cache"
	rediscache "feedsystem_video_go/internal/infrastructure/cache/redis"
)

const detailLogicalTTL = 5 * time.Minute

// DetailCache 视频详情缓存。
type DetailCache struct {
	client *rediscache.Client
	bloom  *cachelib.BloomFilter
}

// NewDetailCache 创建视频详情缓存。
func NewDetailCache(client *rediscache.Client, bloom *cachelib.BloomFilter) *DetailCache {
	return &DetailCache{client: client, bloom: bloom}
}

// Get 读取逻辑过期缓存；空值缓存命中时返回 found=true、video=nil。
func (c *DetailCache) Get(ctx context.Context, id uint) (*domainvideo.Video, bool, bool) {
	if c == nil || c.client == nil {
		return nil, false, false
	}
	if b, err := c.client.GetBytes(ctx, nullKey(id)); err == nil && len(b) > 0 {
		return nil, false, true
	}
	data, expired, found, err := cachelib.GetLogical(ctx, c.client, detailKey(id))
	if err != nil || !found {
		return nil, false, false
	}
	var video domainvideo.Video
	if err := json.Unmarshal(data, &video); err != nil {
		return nil, false, false
	}
	return &video, expired, true
}

// Set 写入逻辑过期缓存。
func (c *DetailCache) Set(ctx context.Context, video *domainvideo.Video) {
	if c == nil || c.client == nil || video == nil {
		return
	}
	data, err := json.Marshal(video)
	if err != nil {
		return
	}
	_ = c.client.Del(ctx, nullKey(video.ID))
	physicalTTL := cachelib.Jitter(30*time.Minute, 0.2)
	_ = cachelib.SetLogical(ctx, c.client, detailKey(video.ID), data, detailLogicalTTL, physicalTTL)
}

// SetNull 写入空值缓存。
func (c *DetailCache) SetNull(ctx context.Context, id uint) {
	if c == nil || c.client == nil {
		return
	}
	_ = c.client.SetBytes(ctx, nullKey(id), []byte("1"), cachelib.Jitter(time.Minute, 0.2))
}

// Delete 删除详情缓存和空值缓存。
func (c *DetailCache) Delete(ctx context.Context, id uint) {
	if c == nil || c.client == nil {
		return
	}
	_ = c.client.Del(ctx, detailKey(id))
	_ = c.client.Del(ctx, nullKey(id))
}

// Lock 获取视频详情重建锁。
func (c *DetailCache) Lock(ctx context.Context, id uint) (string, bool) {
	if c == nil || c.client == nil {
		return "", false
	}
	lockCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	token, ok, err := c.client.Lock(lockCtx, lockKey(id), 2*time.Second)
	if err != nil {
		return "", false
	}
	return token, ok
}

// Unlock 释放视频详情重建锁。
func (c *DetailCache) Unlock(ctx context.Context, id uint, token string) {
	if c == nil || c.client == nil {
		return
	}
	_ = c.client.Unlock(ctx, lockKey(id), token)
}

// BloomExists 判断视频 ID 是否可能存在。
func (c *DetailCache) BloomExists(ctx context.Context, id uint) (bool, error) {
	if c == nil || c.bloom == nil {
		return true, nil
	}
	return c.bloom.Exists(ctx, uint64(id))
}

// BloomAdd 把视频 ID 加入布隆过滤器。
func (c *DetailCache) BloomAdd(ctx context.Context, id uint) {
	if c == nil || c.bloom == nil {
		return
	}
	_ = c.bloom.Add(ctx, uint64(id))
}

// RebuildBloom 重建布隆过滤器。
func (c *DetailCache) RebuildBloom(ctx context.Context, load func(afterID uint, limit int) ([]uint, error)) error {
	if c == nil || c.bloom == nil {
		return nil
	}
	return c.bloom.Rebuild(ctx, load)
}

func detailKey(id uint) string { return fmt.Sprintf("video:detail:id=%d", id) }
func nullKey(id uint) string   { return fmt.Sprintf("video:null:id=%d", id) }
func lockKey(id uint) string   { return fmt.Sprintf("lock:video:detail:id=%d", id) }
