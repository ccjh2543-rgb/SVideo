package feed

import (
	"context"
	"testing"
	"time"

	domaincreator "feedsystem_video_go/internal/domain/creator"
	domainfeed "feedsystem_video_go/internal/domain/feed"
)

type fakeCreatorRepository struct {
	stats *domaincreator.Stats
}

func (f *fakeCreatorRepository) Get(_ context.Context, authorID uint) (*domaincreator.Stats, error) {
	if f.stats == nil {
		return &domaincreator.Stats{AuthorID: authorID}, nil
	}
	return f.stats, nil
}

func (f *fakeCreatorRepository) UpsertOnPublish(_ context.Context, _ uint, _ time.Time) error {
	return nil
}

func (f *fakeCreatorRepository) IncrementFans(_ context.Context, _ uint, _ int64) error {
	return nil
}

func (f *fakeCreatorRepository) ListFollowing(_ context.Context, _ uint) ([]*domaincreator.Stats, error) {
	return nil, nil
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
	inbox  map[uint][]domainfeed.TimelineEntry
	outbox map[uint][]domainfeed.TimelineEntry
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

func (f *fakeTimelineRepository) PushOutbox(_ context.Context, authorID uint, videoID uint, score int64) error {
	f.outbox[authorID] = append(f.outbox[authorID], domainfeed.TimelineEntry{VideoID: videoID, Score: score})
	return nil
}

func (f *fakeTimelineRepository) ReadInbox(_ context.Context, _ uint, _ int64, _ int) ([]domainfeed.TimelineEntry, error) {
	return nil, nil
}

func (f *fakeTimelineRepository) ReadOutbox(_ context.Context, _ uint, _ int64, _ int) ([]domainfeed.TimelineEntry, error) {
	return nil, nil
}

func (f *fakeTimelineRepository) ReadOutboxes(_ context.Context, _ []uint, _ int64, _ int) ([][]domainfeed.TimelineEntry, error) {
	return nil, nil
}

func (f *fakeTimelineRepository) RemoveFromInbox(_ context.Context, _ uint, _ []uint) error {
	return nil
}

func TestFanoutSmallCreatorWritesInboxAndOutbox(t *testing.T) {
	timeline := newFakeTimelineRepository()
	service := NewFanoutService(
		&fakeCreatorRepository{stats: &domaincreator.Stats{AuthorID: 1, FansCount: 3}},
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
		&fakeCreatorRepository{stats: &domaincreator.Stats{AuthorID: 1, FansCount: 100001, IsBig: true}},
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
