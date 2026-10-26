// Package feed 提供 Feed 相关后台任务：Outbox 补偿、死信重放与热榜快照预生成。
package feed

import (
	"context"
	"log"
	"time"

	feedapp "feedsystem_video_go/internal/application/feed"
	feedcache "feedsystem_video_go/internal/infrastructure/cache/redis/feed"
)

const (
	// hotSnapshotInterval 快照刷新间隔，需小于快照 TTL。
	hotSnapshotInterval = time.Minute
	// hotSnapshotBudget 单次合并的时间预算。
	hotSnapshotBudget = 5 * time.Second
	// hotSnapshotHours 预生成的 as_of 个数：当前小时与上一小时。
	hotSnapshotHours = 2
	// hotSnapshotWindowHours 热榜窗口长度。
	hotSnapshotWindowHours = 24
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

// build 构建当前小时与上一小时的快照。
func (b *HotSnapshotBuilder) build(ctx context.Context) {
	now := time.Now().UTC().Truncate(time.Hour)
	for i := 0; i < b.hours; i++ {
		asOf := now.Add(-time.Duration(i) * time.Hour)
		dest := "hot:video:merge:1h:" + asOf.Format("2006010215")
		keys := feedapp.HotWindowKeys(asOf, hotSnapshotWindowHours)
		weights := feedapp.LinearDecayWeights(hotSnapshotWindowHours)

		buildCtx, cancel := context.WithTimeout(ctx, hotSnapshotBudget)
		err := b.ranking.EnsureSnapshot(buildCtx, dest, keys, weights)
		cancel()
		if err != nil {
			log.Printf("hot snapshot builder: build %s failed: %v", dest, err)
		}
	}
}
