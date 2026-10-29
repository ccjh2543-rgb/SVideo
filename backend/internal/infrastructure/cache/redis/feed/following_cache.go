// Package feed 提供 Feed 上下文的 Redis 缓存实现。
package feed

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	domainsocial "feedsystem_video_go/internal/domain/social"
	rediscache "feedsystem_video_go/internal/infrastructure/cache/redis"
)

const (
	// followingCacheTTL 关注列表缓存时间。
	// 关注/取关会主动失效，TTL 仅作为兜底。
	followingCacheTTL = 10 * time.Minute
	// followingCacheTimeout 关注列表缓存的单次读写超时，避免拖慢关注流。
	followingCacheTimeout = 30 * time.Millisecond
)

// followingStatDTO 关注列表缓存的序列化结构。
type followingStatDTO struct {
	AuthorID        uint  `json:"author_id"`
	FansCount       int64 `json:"fans_count"`
	IsBig           bool  `json:"is_big"`
	WasBig          bool  `json:"was_big"`
	LastPublishTime int64 `json:"last_publish_time"` // 毫秒时间戳，0 表示未发布
}

// FollowingCache 关注列表的 Redis 缓存实现。
//
// 读关注流时原本每页都要执行 socials JOIN creator_stats，
// 关注几千人时会产生大量重复查询；这里缓存整份关注列表，
// 关注/取关成功后主动删除，避免脏读。
type FollowingCache struct {
	cache *rediscache.Client
}

// NewFollowingCache 创建关注列表缓存。
func NewFollowingCache(cache *rediscache.Client) *FollowingCache {
	return &FollowingCache{cache: cache}
}

// Get 读取关注列表缓存。
// ok=false 表示未命中或缓存不可用，调用方应回源数据库。
func (c *FollowingCache) Get(ctx context.Context, followerID uint) ([]*domainsocial.Stats, bool) {
	if c == nil || c.cache == nil || followerID == 0 {
		return nil, false
	}
	cacheCtx, cancel := context.WithTimeout(ctx, followingCacheTimeout)
	defer cancel()

	raw, err := c.cache.GetBytes(cacheCtx, followingKey(followerID))
	if err != nil || len(raw) == 0 {
		return nil, false
	}
	var rows []followingStatDTO
	if err := json.Unmarshal(raw, &rows); err != nil {
		return nil, false
	}
	stats := make([]*domainsocial.Stats, 0, len(rows))
	for _, row := range rows {
		item := &domainsocial.Stats{
			AuthorID:  row.AuthorID,
			FansCount: row.FansCount,
			IsBig:     row.IsBig,
			WasBig:    row.WasBig,
		}
		if row.LastPublishTime > 0 {
			item.LastPublishTime = time.UnixMilli(row.LastPublishTime)
		}
		stats = append(stats, item)
	}
	return stats, true
}

// Set 写入关注列表缓存。
// 缓存写入失败不影响业务，仅记录为未命中。
func (c *FollowingCache) Set(ctx context.Context, followerID uint, stats []*domainsocial.Stats) {
	if c == nil || c.cache == nil || followerID == 0 {
		return
	}
	rows := make([]followingStatDTO, 0, len(stats))
	for _, item := range stats {
		if item == nil || item.AuthorID == 0 {
			continue
		}
		var lastPublish int64
		if !item.LastPublishTime.IsZero() {
			lastPublish = item.LastPublishTime.UnixMilli()
		}
		rows = append(rows, followingStatDTO{
			AuthorID:        item.AuthorID,
			FansCount:       item.FansCount,
			IsBig:           item.IsBig,
			WasBig:          item.WasBig,
			LastPublishTime: lastPublish,
		})
	}
	raw, err := json.Marshal(rows)
	if err != nil {
		return
	}
	cacheCtx, cancel := context.WithTimeout(ctx, followingCacheTimeout)
	defer cancel()
	_ = c.cache.SetBytes(cacheCtx, followingKey(followerID), raw, followingCacheTTL)
}

// Invalidate 删除关注列表缓存，关注/取关后调用。
func (c *FollowingCache) Invalidate(ctx context.Context, followerID uint) error {
	if c == nil || c.cache == nil || followerID == 0 {
		return nil
	}
	return c.cache.Del(ctx, followingKey(followerID))
}

// followingKey 关注列表缓存 key。
func followingKey(followerID uint) string {
	return fmt.Sprintf("feed:follow:%d", followerID)
}
