package video

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	domainfeed "feedsystem_video_go/internal/domain/feed"
	rediscache "feedsystem_video_go/internal/infrastructure/cache/redis"
)

const (
	// popularityHalfLife 热度半衰期，6 小时兼顾实时性与稳定性。
	popularityHalfLife = domainfeed.DefaultHotHalfLife
	// detailInvalidateInterval 同一视频详情缓存的合并失效间隔。
	detailInvalidateInterval = time.Second
)

// PopularityCache 热度缓存。
type PopularityCache struct {
	client *rediscache.Client

	mu             sync.Mutex
	lastInvalidate map[uint]time.Time
}

// NewPopularityCache 创建热度缓存。
func NewPopularityCache(client *rediscache.Client) *PopularityCache {
	return &PopularityCache{
		client:         client,
		lastInvalidate: make(map[uint]time.Time),
	}
}

// Update 使用当前时间更新小时级热榜。
func (c *PopularityCache) Update(ctx context.Context, videoID uint, change int64) error {
	return c.UpdateAt(ctx, videoID, change, time.Now())
}

// UpdateAt 使用事件发生时间更新小时级热榜。
// 事件时间用于计算桶内相对权重，避免消费延迟影响热度。
func (c *PopularityCache) UpdateAt(ctx context.Context, videoID uint, change int64, occurredAt time.Time) error {
	return c.UpdateBatch(ctx, []domainfeed.HotUpdate{{
		VideoID:    videoID,
		Change:     change,
		OccurredAt: occurredAt,
		Weight:     1,
	}})
}

// UpdateBatch 批量更新热度候选桶。
// 同一批次内按 (桶, 分片, 视频) 聚合后 pipeline 写入，降低 Redis 命令数。
func (c *PopularityCache) UpdateBatch(ctx context.Context, updates []domainfeed.HotUpdate) error {
	if c == nil || c.client == nil || len(updates) == 0 {
		return nil
	}

	items, invalidated := buildHotIncrements(updates)
	if len(items) == 0 {
		return nil
	}

	if err := c.client.ZIncrByBatch(ctx, items, domainfeed.HotCandidateTTL); err != nil {
		return err
	}

	c.invalidateDetails(ctx, invalidated)
	return nil
}

// buildHotIncrements 把热度变更聚合为 ZINCRBY 指令。
// 返回的 invalidated 是本次涉及的视频 ID，用于合并失效详情缓存。
func buildHotIncrements(updates []domainfeed.HotUpdate) ([]rediscache.ZIncrByItem, map[uint]struct{}) {
	aggregated := make(map[hotAggregateKey]float64, len(updates))
	invalidated := make(map[uint]struct{}, len(updates))

	for _, item := range updates {
		if item.VideoID == 0 || item.Change == 0 {
			continue
		}
		occurredAt := item.OccurredAt
		if occurredAt.IsZero() {
			occurredAt = time.Now()
		}
		bucket := domainfeed.HotBucketStart(occurredAt)
		key := domainfeed.HotCandidateKey(bucket, domainfeed.HotShard(item.VideoID))
		member := strconv.FormatUint(uint64(item.VideoID), 10)
		weight := item.Weight
		if weight <= 0 {
			weight = 1
		}
		score := domainfeed.HotIncrement(occurredAt, float64(item.Change)*weight, popularityHalfLife)
		aggregated[hotAggregateKey{key: key, member: member}] += score
		invalidated[item.VideoID] = struct{}{}
	}

	items := make([]rediscache.ZIncrByItem, 0, len(aggregated))
	for key, score := range aggregated {
		if score == 0 {
			continue
		}
		items = append(items, rediscache.ZIncrByItem{Key: key.key, Member: key.member, Score: score})
	}
	return items, invalidated
}

// hotAggregateKey 热度聚合键：同一个桶、同一个视频合并为一次 ZINCRBY。
type hotAggregateKey struct {
	key    string
	member string
}

// invalidateDetails 合并失效视频详情缓存，避免每次点赞都删除同一个热门 key。
func (c *PopularityCache) invalidateDetails(ctx context.Context, ids map[uint]struct{}) {
	if c == nil || c.client == nil || len(ids) == 0 {
		return
	}

	now := time.Now()
	toDelete := make([]uint, 0, len(ids))

	c.mu.Lock()
	if c.lastInvalidate == nil {
		c.lastInvalidate = make(map[uint]time.Time)
	}
	for id := range ids {
		last := c.lastInvalidate[id]
		if now.Sub(last) >= detailInvalidateInterval {
			c.lastInvalidate[id] = now
			toDelete = append(toDelete, id)
		}
	}
	// 防止长期运行后 map 无限增长。
	if len(c.lastInvalidate) > 10000 {
		for id, last := range c.lastInvalidate {
			if now.Sub(last) > 2*detailInvalidateInterval {
				delete(c.lastInvalidate, id)
			}
		}
	}
	c.mu.Unlock()

	for _, id := range toDelete {
		_ = c.client.Del(ctx, fmt.Sprintf("video:detail:id=%d", id))
	}
}
