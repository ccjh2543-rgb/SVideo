package social

import (
	"context"
	"fmt"
	"log"
	"time"

	domainfeed "feedsystem_video_go/internal/domain/feed"
	domainsocial "feedsystem_video_go/internal/domain/social"
)

const (
	// demoteBackfillInterval 降级回填扫描间隔。
	demoteBackfillInterval = 30 * time.Second
	// demoteBackfillScanLimit 单轮最多处理的降级作者数。
	demoteBackfillScanLimit = 5
	// demoteBackfillEntries 回填到粉丝 Inbox 的作者历史条数，与 Outbox 保留上限一致。
	demoteBackfillEntries = 50
	// demoteFanBatch 粉丝分页批大小，避免一次加载全部粉丝。
	demoteFanBatch = 500
)

// DemoteStatsPort 降级回填所需的创作者统计能力。
type DemoteStatsPort interface {
	Get(ctx context.Context, authorID uint) (*domainsocial.Stats, error)
	ListDemotePending(ctx context.Context, limit int) ([]uint, error)
	MarkDemoteBackfilled(ctx context.Context, authorID uint) error
}

// DemoteTimelinePort 降级回填所需的时间线能力。
type DemoteTimelinePort interface {
	ReadOutbox(ctx context.Context, authorID uint, maxScore int64, limit int) ([]domainfeed.TimelineEntry, error)
	PushInboxEntriesWindow(ctx context.Context, followerID uint, entries []domainfeed.TimelineEntry, capacity int64) error
}

// DemoteBackfillWorker 处理“大 V 变成小 V”后的批量降级回填。
//
// 大 V 期间内容只写作者 Outbox，粉丝靠读时拉取；掉粉跌破退出阈值后不再走大 V 路径，
// 此时把 Outbox 最近内容补写进每个粉丝的 Inbox，完成后清除 was_big，
// 读路径就不再需要为历史内容额外拉 Outbox。
type DemoteBackfillWorker struct {
	stats     DemoteStatsPort
	followers domainfeed.FollowerReader
	timeline  DemoteTimelinePort
	interval  time.Duration
	scanLimit int
}

// NewDemoteBackfillWorker 创建降级回填任务。
// interval <= 0 时使用默认 30s 间隔。
func NewDemoteBackfillWorker(
	stats DemoteStatsPort,
	followers domainfeed.FollowerReader,
	timeline DemoteTimelinePort,
	interval time.Duration,
) *DemoteBackfillWorker {
	if interval <= 0 {
		interval = demoteBackfillInterval
	}
	return &DemoteBackfillWorker{
		stats:     stats,
		followers: followers,
		timeline:  timeline,
		interval:  interval,
		scanLimit: demoteBackfillScanLimit,
	}
}

// Run 启动回填循环：每轮扫描一批待回填作者，失败只记录日志，不影响后续周期。
func (w *DemoteBackfillWorker) Run(ctx context.Context) error {
	if w == nil || w.stats == nil || w.followers == nil || w.timeline == nil {
		return nil
	}
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		w.processPending(ctx)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// processPending 扫描并处理一轮待回填作者。
func (w *DemoteBackfillWorker) processPending(ctx context.Context) {
	limit := w.scanLimit
	if limit <= 0 {
		limit = demoteBackfillScanLimit
	}
	authors, err := w.stats.ListDemotePending(ctx, limit)
	if err != nil {
		log.Printf("demote backfill: list pending failed: %v", err)
		return
	}
	for _, authorID := range authors {
		if err := ctx.Err(); err != nil {
			return
		}
		if err := w.backfillAuthor(ctx, authorID); err != nil {
			// 中途失败保留 demote_pending，下一轮从第一批粉丝重跑。
			// ZADD 幂等 + 容量窗口过滤保证重跑不会产生重复或越界内容。
			log.Printf("demote backfill: author=%d failed: %v", authorID, err)
			continue
		}
	}
}

// backfillAuthor 把某个作者 Outbox 的最近内容批量写入其全部粉丝 Inbox。
func (w *DemoteBackfillWorker) backfillAuthor(ctx context.Context, authorID uint) error {
	if authorID == 0 {
		return nil
	}
	stats, err := w.stats.Get(ctx, authorID)
	if err != nil {
		return err
	}
	// 已经重新涨回大 V：不需要回填，直接清除待办标记。
	if stats != nil && (stats.IsBig || stats.FansCount >= domainsocial.BigVFansThreshold) {
		return w.stats.MarkDemoteBackfilled(ctx, authorID)
	}

	entries, err := w.timeline.ReadOutbox(ctx, authorID, 0, demoteBackfillEntries)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return w.stats.MarkDemoteBackfilled(ctx, authorID)
	}

	var cursor uint
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		followers, err := w.followers.ListFollowerIDsAfter(ctx, authorID, cursor, demoteFanBatch)
		if err != nil {
			return err
		}
		if len(followers) == 0 {
			break
		}
		for _, followerID := range followers {
			if err := w.timeline.PushInboxEntriesWindow(ctx, followerID, entries, domainfeed.DefaultInboxCapacity); err != nil {
				return fmt.Errorf("push inbox follower=%d: %w", followerID, err)
			}
		}
		cursor = followers[len(followers)-1]
		if len(followers) < demoteFanBatch {
			break
		}
	}

	// 全部粉丝写入成功后才清除标记与 was_big。
	return w.stats.MarkDemoteBackfilled(ctx, authorID)
}
