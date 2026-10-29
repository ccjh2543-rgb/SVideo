// Package feed 提供 Feed 相关后台任务：Outbox 补偿、死信重放与热榜快照预生成。
package feed

import (
	"context"
	"log"
	"time"

	domainfeed "feedsystem_video_go/internal/domain/feed"
	feedcache "feedsystem_video_go/internal/infrastructure/cache/redis/feed"
)

const (
	// hotSnapshotInterval 快照刷新间隔，需小于快照 TTL。
	hotSnapshotInterval = time.Minute
	// hotSnapshotBudget 单次候选合并与快照构建的时间预算。
	hotSnapshotBudget = 5 * time.Second
	// hotSnapshotHours 预生成的 as_of 个数：当前小时与上一小时。
	hotSnapshotHours = 2
	// hotSnapshotWindowHours 热榜窗口长度。
	hotSnapshotWindowHours = domainfeed.HotWindowHours
	// hotShardCount 候选桶分片数。
	hotShardCount = domainfeed.HotShardCount
	// hotBucketTopN 每个小时 Top 桶保留数量。
	hotBucketTopN = domainfeed.HotBucketTopN
	// hotHalfLife 热度指数衰减半衰期。
	hotHalfLife = domainfeed.DefaultHotHalfLife
)

// HotSnapshotBuilder 离线预生成热榜快照。
// 请求路径只读快照，绝不在线执行 ZUNIONSTORE。
type HotSnapshotBuilder struct {
	ranking *feedcache.RankingCache
	hours   int
}

// NewHotSnapshotBuilder 创建热榜快照构建任务。
func NewHotSnapshotBuilder(ranking *feedcache.RankingCache) *HotSnapshotBuilder {
	return &HotSnapshotBuilder{ranking: ranking, hours: hotSnapshotHours}
}

// Run 启动构建循环：先立即构建一次，之后按固定间隔刷新。
func (b *HotSnapshotBuilder) Run(ctx context.Context) error {
	if b == nil || b.ranking == nil {
		return nil
	}
	b.build(ctx)

	ticker := time.NewTicker(hotSnapshotInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			b.build(ctx)
		}
	}
}

// build 构建当前小时与上一小时的 Top 桶和合并快照。
func (b *HotSnapshotBuilder) build(ctx context.Context) {
	now := time.Now().UTC().Truncate(time.Hour)
	for i := 0; i < b.hours; i++ {
		asOf := now.Add(-time.Duration(i) * time.Hour)
		buildCtx, cancel := context.WithTimeout(ctx, hotSnapshotBudget)

		// 候选桶只累计，Top 桶负责裁剪；先合并候选，再构建 24 小时快照。
		if err := b.ranking.PromoteCandidates(buildCtx, asOf, hotShardCount, hotBucketTopN); err != nil {
			log.Printf("hot snapshot builder: promote %s failed: %v", asOf.Format("2006010215"), err)
		}

		dest := domainfeed.HotSnapshotKey(asOf)
		keys := domainfeed.HotTopWindowKeys(asOf, hotSnapshotWindowHours)
		weights := domainfeed.HotDecayWeights(asOf, hotSnapshotWindowHours, hotHalfLife)
		err := b.ranking.RefreshSnapshot(buildCtx, dest, keys, weights)
		cancel()
		if err != nil {
			log.Printf("hot snapshot builder: build %s failed: %v", dest, err)
		}
	}
}
