package feed

import (
	"context"
	"strconv"
	"time"

	domainfeed "feedsystem_video_go/internal/domain/feed"
	domainvideo "feedsystem_video_go/internal/domain/video"
	rediscache "feedsystem_video_go/internal/infrastructure/cache/redis"

	goredis "github.com/redis/go-redis/v9"
)

const (
	// globalTimelineKey 全局最新视频时间线。
	globalTimelineKey = "feed:global_timeline"
	// snapshotTTL 热榜合并快照的存活时间。
	snapshotTTL = 10 * time.Minute
)

// RankingCache 基于 Redis ZSET 的时间线与热榜实现。
type RankingCache struct {
	cache *rediscache.Client
}

// NewRankingCache 创建时间线与热榜缓存。
func NewRankingCache(cache *rediscache.Client) *RankingCache {
	return &RankingCache{cache: cache}
}

// GlobalTimelineTail 返回全局时间线中最老一条的毫秒时间戳。
func (c *RankingCache) GlobalTimelineTail(ctx context.Context) (int64, bool, error) {
	if c == nil || c.cache == nil {
		return 0, false, nil
	}
	tail, err := c.cache.ZRangeWithScores(ctx, globalTimelineKey, 0, 0)
	if err != nil {
		return 0, false, err
	}
	if len(tail) == 0 {
		return 0, false, nil
	}
	return int64(tail[0].Score), true, nil
}

// RebuildGlobalTimeline 用给定视频重建全局时间线，分数为发布时间的毫秒时间戳。
func (c *RankingCache) RebuildGlobalTimeline(ctx context.Context, videos []*domainvideo.Video) error {
	if c == nil || c.cache == nil || len(videos) == 0 {
		return nil
	}
	elements := make([]goredis.Z, 0, len(videos))
	for _, video := range videos {
		if video == nil || video.ID == 0 {
			continue
		}
		elements = append(elements, goredis.Z{
			Score:  float64(video.CreateTime.UnixMilli()),
			Member: strconv.FormatUint(uint64(video.ID), 10),
		})
	}
	if len(elements) == 0 {
		return nil
	}
	return c.cache.ZAdd(ctx, globalTimelineKey, elements...)
}

// LatestIDs 返回时间线中分数不大于 maxScore 的视频 ID。
// maxScore <= 0 表示不限上界，用于第一页。
func (c *RankingCache) LatestIDs(ctx context.Context, maxScore int64, limit int) ([]uint, error) {
	if c == nil || c.cache == nil {
		return nil, nil
	}
	max := "+inf"
	if maxScore > 0 {
		max = strconv.FormatInt(maxScore, 10)
	}
	members, err := c.cache.ZRevRangeByScore(ctx, globalTimelineKey, max, "-inf", 0, int64(limit))
	if err != nil {
		return nil, err
	}
	return parseIDs(members), nil
}

// PromoteCandidates 将小时候选桶合并为 Top 桶，并裁剪到固定上限。
// 候选桶只负责累计，Top 桶只负责排序与快照，避免裁剪丢累计值。
func (c *RankingCache) PromoteCandidates(ctx context.Context, hour time.Time, shards int, topLimit int64) error {
	if c == nil || c.cache == nil {
		return nil
	}
	keys := domainfeed.HotCandidateKeys(hour, shards)
	if len(keys) == 0 {
		return nil
	}

	// 候选桶过期或为空时保留已有 Top 桶，避免重建把历史 Top 清空。
	hasCandidate := false
	for _, key := range keys {
		exists, err := c.cache.Exists(ctx, key)
		if err != nil {
			return err
		}
		if exists {
			hasCandidate = true
			break
		}
	}
	if !hasCandidate {
		return nil
	}

	topKey := domainfeed.HotTopKey(hour)
	if err := c.cache.ZUnionStore(ctx, topKey, keys, "SUM"); err != nil {
		return err
	}
	if topLimit > 0 {
		if err := c.cache.ZRemRangeByRank(ctx, topKey, 0, -(topLimit + 1)); err != nil {
			return err
		}
	}
	return c.cache.Expire(ctx, topKey, domainfeed.HotTopTTL)
}

// EnsureSnapshot 确保热榜合并快照存在。
// 快照不存在时按小时桶和权重合并，并设置固定 TTL，保证同一 as_of 下翻页稳定。
func (c *RankingCache) EnsureSnapshot(ctx context.Context, dest string, keys []string, weights []float64) error {
	if c == nil || c.cache == nil || dest == "" || len(keys) == 0 {
		return nil
	}
	exists, err := c.cache.Exists(ctx, dest)
	if err != nil || exists {
		return err
	}
	return c.RefreshSnapshot(ctx, dest, keys, weights)
}

// RefreshSnapshot 强制重建热榜快照。
// 用于离线构建任务按分钟刷新；请求路径只读，不调用本方法。
func (c *RankingCache) RefreshSnapshot(ctx context.Context, dest string, keys []string, weights []float64) error {
	if c == nil || c.cache == nil || dest == "" || len(keys) == 0 {
		return nil
	}

	lockKey := "lock:" + dest
	token, ok, err := c.cache.Lock(ctx, lockKey, 30*time.Second)
	if err != nil {
		return err
	}
	if !ok {
		// 其他实例正在构建，直接返回；调用方读不到会走降级路径。
		return nil
	}
	defer func() {
		// 解锁失败只会让锁在 TTL 后自动释放，不影响数据正确性。
		_ = c.cache.Unlock(context.Background(), lockKey, token)
	}()

	if err := c.cache.ZUnionStoreWeighted(ctx, dest, keys, weights, "SUM"); err != nil {
		return err
	}
	return c.cache.Expire(ctx, dest, snapshotTTL)
}

// SnapshotIDs 从热榜快照中按热度倒序取 [offset, offset+limit) 的视频 ID。
func (c *RankingCache) SnapshotIDs(ctx context.Context, dest string, offset int, limit int) ([]uint, error) {
	if c == nil || c.cache == nil {
		return nil, nil
	}
	start := int64(offset)
	stop := start + int64(limit) - 1
	members, err := c.cache.ZRevRange(ctx, dest, start, stop)
	if err != nil {
		return nil, err
	}
	return parseIDs(members), nil
}

// parseIDs 把 ZSET 成员转换为视频 ID，忽略非法成员。
func parseIDs(members []string) []uint {
	ids := make([]uint, 0, len(members))
	for _, member := range members {
		value, err := strconv.ParseUint(member, 10, 64)
		if err != nil || value == 0 {
			continue
		}
		ids = append(ids, uint(value))
	}
	return ids
}
