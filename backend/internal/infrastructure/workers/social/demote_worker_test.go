package social

import (
	"context"
	"errors"
	"testing"

	domainfeed "feedsystem_video_go/internal/domain/feed"
	domainsocial "feedsystem_video_go/internal/domain/social"
)

// fakeDemoteStats 降级回填用的统计仓储替身。
type fakeDemoteStats struct {
	stats   map[uint]*domainsocial.Stats
	pending []uint
	marked  []uint
}

func (f *fakeDemoteStats) Get(_ context.Context, authorID uint) (*domainsocial.Stats, error) {
	if stats, ok := f.stats[authorID]; ok {
		return stats, nil
	}
	return &domainsocial.Stats{AuthorID: authorID}, nil
}

func (f *fakeDemoteStats) ListDemotePending(_ context.Context, _ int) ([]uint, error) {
	return f.pending, nil
}

func (f *fakeDemoteStats) MarkDemoteBackfilled(_ context.Context, authorID uint) error {
	f.marked = append(f.marked, authorID)
	return nil
}

// fakeDemoteFollowers 粉丝分页替身。
type fakeDemoteFollowers struct {
	fans map[uint][]uint
}

func (f *fakeDemoteFollowers) ListFollowerIDsAfter(_ context.Context, vloggerID uint, afterFollowerID uint, limit int) ([]uint, error) {
	result := make([]uint, 0, limit)
	for _, id := range f.fans[vloggerID] {
		if id > afterFollowerID {
			result = append(result, id)
			if len(result) == limit {
				break
			}
		}
	}
	return result, nil
}

// fakeDemoteTimeline 时间线替身，记录回填写入。
type fakeDemoteTimeline struct {
	outbox map[uint][]domainfeed.TimelineEntry
	pushed map[uint][]domainfeed.TimelineEntry
	failOn uint
}

func (f *fakeDemoteTimeline) ReadOutbox(_ context.Context, authorID uint, _ int64, limit int) ([]domainfeed.TimelineEntry, error) {
	entries := f.outbox[authorID]
	if len(entries) > limit {
		entries = entries[:limit]
	}
	return entries, nil
}

func (f *fakeDemoteTimeline) PushInboxEntriesWindow(_ context.Context, followerID uint, entries []domainfeed.TimelineEntry, _ int64) error {
	if f.failOn != 0 && followerID == f.failOn {
		return errors.New("push failed")
	}
	if f.pushed == nil {
		f.pushed = make(map[uint][]domainfeed.TimelineEntry)
	}
	f.pushed[followerID] = append([]domainfeed.TimelineEntry(nil), entries...)
	return nil
}

func newDemoteFixture(authorID uint, fans []uint, entries []domainfeed.TimelineEntry) (*DemoteBackfillWorker, *fakeDemoteStats, *fakeDemoteTimeline) {
	stats := &fakeDemoteStats{
		stats: map[uint]*domainsocial.Stats{
			authorID: {AuthorID: authorID, FansCount: domainsocial.BigVExitThreshold - 1},
		},
		pending: []uint{authorID},
	}
	followers := &fakeDemoteFollowers{fans: map[uint][]uint{authorID: fans}}
	timeline := &fakeDemoteTimeline{outbox: map[uint][]domainfeed.TimelineEntry{authorID: entries}}
	return NewDemoteBackfillWorker(stats, followers, timeline, 0), stats, timeline
}

// TestDemoteBackfillPushesHistoryToAllFans 覆盖多页粉丝的完整回填。
func TestDemoteBackfillPushesHistoryToAllFans(t *testing.T) {
	fans := make([]uint, 0, demoteFanBatch+1)
	for i := uint(1); i <= demoteFanBatch+1; i++ {
		fans = append(fans, i)
	}
	entries := []domainfeed.TimelineEntry{{VideoID: 10, Score: 1700000000000}, {VideoID: 11, Score: 1700000001000}}
	worker, stats, timeline := newDemoteFixture(99, fans, entries)

	if err := worker.backfillAuthor(context.Background(), 99); err != nil {
		t.Fatalf("backfill author: %v", err)
	}
	if len(timeline.pushed) != len(fans) {
		t.Fatalf("pushed followers = %d, want %d", len(timeline.pushed), len(fans))
	}
	for _, fan := range fans {
		if len(timeline.pushed[fan]) != len(entries) {
			t.Fatalf("follower %d entries = %d, want %d", fan, len(timeline.pushed[fan]), len(entries))
		}
	}
	if len(stats.marked) != 1 || stats.marked[0] != 99 {
		t.Fatalf("marked = %v, want [99]", stats.marked)
	}
}

// TestDemoteBackfillPromotedAuthorClearsPending 作者重新涨回大 V 时只清标记、不回填。
func TestDemoteBackfillPromotedAuthorClearsPending(t *testing.T) {
	worker, stats, timeline := newDemoteFixture(7, []uint{1, 2}, []domainfeed.TimelineEntry{{VideoID: 1}})
	stats.stats[7] = &domainsocial.Stats{AuthorID: 7, FansCount: domainsocial.BigVFansThreshold}

	if err := worker.backfillAuthor(context.Background(), 7); err != nil {
		t.Fatalf("backfill author: %v", err)
	}
	if len(timeline.pushed) != 0 {
		t.Fatalf("pushed = %d, want 0", len(timeline.pushed))
	}
	if len(stats.marked) != 1 {
		t.Fatalf("marked = %v, want one mark", stats.marked)
	}
}

// TestDemoteBackfillEmptyOutboxClearsPending Outbox 为空时直接完成，避免反复扫描。
func TestDemoteBackfillEmptyOutboxClearsPending(t *testing.T) {
	worker, stats, timeline := newDemoteFixture(8, []uint{1, 2}, nil)

	if err := worker.backfillAuthor(context.Background(), 8); err != nil {
		t.Fatalf("backfill author: %v", err)
	}
	if len(timeline.pushed) != 0 {
		t.Fatalf("pushed = %d, want 0", len(timeline.pushed))
	}
	if len(stats.marked) != 1 {
		t.Fatalf("marked = %v, want one mark", stats.marked)
	}
}

// TestDemoteBackfillKeepsPendingOnFailure 中途写入失败时不能清标记，下一轮重试。
func TestDemoteBackfillKeepsPendingOnFailure(t *testing.T) {
	worker, stats, timeline := newDemoteFixture(9, []uint{1, 2, 3}, []domainfeed.TimelineEntry{{VideoID: 1}})
	timeline.failOn = 2

	if err := worker.backfillAuthor(context.Background(), 9); err == nil {
		t.Fatal("expected error, got nil")
	}
	if len(stats.marked) != 0 {
		t.Fatalf("marked = %v, want none", stats.marked)
	}
}
