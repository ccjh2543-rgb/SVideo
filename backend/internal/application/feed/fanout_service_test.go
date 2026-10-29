package feed

import (
	"context"
	"sort"
	"testing"
	"time"

	domainfeed "feedsystem_video_go/internal/domain/feed"
	domainsocial "feedsystem_video_go/internal/domain/social"
)

type fakeCreatorRepository struct {
	stats *domainsocial.Stats
}

func (f *fakeCreatorRepository) Get(_ context.Context, authorID uint) (*domainsocial.Stats, error) {
	if f.stats == nil {
		return &domainsocial.Stats{AuthorID: authorID}, nil
	}
	return f.stats, nil
}

func (f *fakeCreatorRepository) UpsertOnPublish(_ context.Context, _ uint, _ time.Time) error {
	return nil
}

func (f *fakeCreatorRepository) IncrementFans(_ context.Context, _ uint, _ int64) error {
	return nil
}

func (f *fakeCreatorRepository) ListFollowing(_ context.Context, _ uint) ([]*domainsocial.Stats, error) {
	return nil, nil
}

func (f *fakeCreatorRepository) ListDemotePending(_ context.Context, _ int) ([]uint, error) {
	return nil, nil
}

func (f *fakeCreatorRepository) MarkDemoteBackfilled(_ context.Context, _ uint) error {
	return nil
}

type fakeFollowerReader struct {
	ids []uint
}

func (f *fakeFollowerReader) ListFollowerIDsAfter(_ context.Context, _ uint, afterFollowerID uint, limit int) ([]uint, error) {
	result := make([]uint, 0, limit)
	for _, id := range f.ids {
		if id > afterFollowerID {
			result = append(result, id)
			if len(result) == limit {
				break
			}
		}
	}
	return result, nil
}

type fakeTimelineRepository struct {
	inbox         map[uint][]domainfeed.TimelineEntry
	outbox        map[uint][]domainfeed.TimelineEntry
	readAuthorIDs []uint
	readDelay     time.Duration
}

func newFakeTimelineRepository() *fakeTimelineRepository {
	return &fakeTimelineRepository{
		inbox:  make(map[uint][]domainfeed.TimelineEntry),
		outbox: make(map[uint][]domainfeed.TimelineEntry),
	}
}

func (f *fakeTimelineRepository) PushInbox(ctx context.Context, followerID uint, videoID uint, score int64) error {
	return f.PushInboxBatch(ctx, []uint{followerID}, videoID, score)
}

func (f *fakeTimelineRepository) PushInboxBatch(_ context.Context, followerIDs []uint, videoID uint, score int64) error {
	for _, id := range followerIDs {
		f.inbox[id] = append(f.inbox[id], domainfeed.TimelineEntry{VideoID: videoID, Score: score})
	}
	return nil
}

func (f *fakeTimelineRepository) PushInboxEntries(_ context.Context, followerID uint, entries []domainfeed.TimelineEntry) error {
	f.inbox[followerID] = append(f.inbox[followerID], entries...)
	return nil
}

func (f *fakeTimelineRepository) PushInboxEntriesWindow(_ context.Context, followerID uint, entries []domainfeed.TimelineEntry, _ int64) error {
	f.inbox[followerID] = append(f.inbox[followerID], entries...)
	return nil
}

func (f *fakeTimelineRepository) PushOutbox(_ context.Context, authorID uint, videoID uint, score int64) error {
	f.outbox[authorID] = append(f.outbox[authorID], domainfeed.TimelineEntry{VideoID: videoID, Score: score})
	return nil
}

func (f *fakeTimelineRepository) ReadInbox(_ context.Context, userID uint, maxScore int64, limit int) ([]domainfeed.TimelineEntry, error) {
	return filterFakeEntries(f.inbox[userID], maxScore, limit), nil
}

func (f *fakeTimelineRepository) ReadOutbox(_ context.Context, authorID uint, maxScore int64, limit int) ([]domainfeed.TimelineEntry, error) {
	return filterFakeEntries(f.outbox[authorID], maxScore, limit), nil
}

func (f *fakeTimelineRepository) ReadOutboxes(_ context.Context, authorIDs []uint, maxScore int64, limit int) ([][]domainfeed.TimelineEntry, error) {
	if f.readDelay > 0 {
		time.Sleep(f.readDelay)
	}
	f.readAuthorIDs = append(f.readAuthorIDs, authorIDs...)
	groups := make([][]domainfeed.TimelineEntry, len(authorIDs))
	for i, authorID := range authorIDs {
		groups[i] = filterFakeEntries(f.outbox[authorID], maxScore, limit)
	}
	return groups, nil
}

// filterFakeEntries 模拟 Redis ZSet 的读取语义：按 score、ID 倒序，最多返回 limit 条。
func filterFakeEntries(entries []domainfeed.TimelineEntry, maxScore int64, limit int) []domainfeed.TimelineEntry {
	result := make([]domainfeed.TimelineEntry, 0, len(entries))
	for _, entry := range entries {
		if maxScore > 0 && entry.Score > maxScore {
			continue
		}
		result = append(result, entry)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Score == result[j].Score {
			return result[i].VideoID > result[j].VideoID
		}
		return result[i].Score > result[j].Score
	})
	if limit > 0 && len(result) > limit {
		result = result[:limit]
	}
	return result
}

func (f *fakeTimelineRepository) RemoveFromInbox(_ context.Context, _ uint, _ []uint) error {
	return nil
}

func TestFanoutSmallCreatorWritesInboxAndOutbox(t *testing.T) {
	timeline := newFakeTimelineRepository()
	service := NewFanoutService(
		&fakeCreatorRepository{stats: &domainsocial.Stats{AuthorID: 1, FansCount: 3}},
		&fakeFollowerReader{ids: []uint{2, 3, 4}},
		timeline,
		100000,
		2, // 分两页：2,3 和 4
	)

	createTime := time.UnixMilli(1760000000000)
	if err := service.Handle(context.Background(), 9, 1, createTime); err != nil {
		t.Fatalf("fanout: %v", err)
	}

	if len(timeline.outbox[1]) != 1 || timeline.outbox[1][0].VideoID != 9 {
		t.Fatalf("expected outbox entry, got %+v", timeline.outbox[1])
	}
	for _, followerID := range []uint{2, 3, 4} {
		if len(timeline.inbox[followerID]) != 1 || timeline.inbox[followerID][0].VideoID != 9 {
			t.Fatalf("expected inbox entry for follower %d, got %+v", followerID, timeline.inbox[followerID])
		}
	}
}

func TestFanoutBigCreatorOnlyWritesOutbox(t *testing.T) {
	timeline := newFakeTimelineRepository()
	service := NewFanoutService(
		&fakeCreatorRepository{stats: &domainsocial.Stats{AuthorID: 1, FansCount: 100001, IsBig: true}},
		&fakeFollowerReader{ids: []uint{2, 3, 4}},
		timeline,
		100000,
		2,
	)

	if err := service.Handle(context.Background(), 9, 1, time.Now()); err != nil {
		t.Fatalf("fanout: %v", err)
	}
	if len(timeline.outbox[1]) != 1 {
		t.Fatalf("expected outbox entry, got %+v", timeline.outbox[1])
	}
	if len(timeline.inbox) != 0 {
		t.Fatalf("big creator should not write inbox, got %+v", timeline.inbox)
	}
}

type fakeHeavyFollowerReader struct {
	ids []uint
}

func (f *fakeHeavyFollowerReader) ScanHeavyFollowers(_ context.Context, _ uint, cursor uint64, count int64) ([]uint, uint64, error) {
	start := int(cursor)
	if start >= len(f.ids) {
		return nil, 0, nil
	}
	end := start + int(count)
	if end > len(f.ids) {
		end = len(f.ids)
	}
	next := uint64(end)
	if end >= len(f.ids) {
		next = 0
	}
	return f.ids[start:end], next, nil
}

func TestFanoutBigCreatorPushesHeavyFollowers(t *testing.T) {
	timeline := newFakeTimelineRepository()
	service := NewFanoutService(
		&fakeCreatorRepository{stats: &domainsocial.Stats{AuthorID: 1, FansCount: 100001, IsBig: true}},
		&fakeFollowerReader{ids: []uint{2, 3, 4}},
		timeline,
		100000,
		2,
	)
	service.SetHeavyPush(NewHeavyPushService(&fakeHeavyFollowerReader{ids: []uint{2, 3}}, timeline, 1))

	if err := service.Handle(context.Background(), 9, 1, time.Now()); err != nil {
		t.Fatalf("fanout: %v", err)
	}
	if len(timeline.outbox[1]) != 1 {
		t.Fatalf("expected outbox entry, got %+v", timeline.outbox[1])
	}
	for _, followerID := range []uint{2, 3} {
		if len(timeline.inbox[followerID]) != 1 || timeline.inbox[followerID][0].VideoID != 9 {
			t.Fatalf("expected heavy inbox entry for follower %d, got %+v", followerID, timeline.inbox[followerID])
		}
	}
	if len(timeline.inbox[4]) != 0 {
		t.Fatalf("normal follower should not receive heavy push, got %+v", timeline.inbox[4])
	}
}
