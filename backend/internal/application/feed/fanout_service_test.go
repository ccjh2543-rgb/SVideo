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
	if err := service.Handle(context.Background(), "evt-1", 9, 1, createTime); err != nil {
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

	if err := service.Handle(context.Background(), "evt-1", 9, 1, time.Now()); err != nil {
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

	if err := service.Handle(context.Background(), "evt-1", 9, 1, time.Now()); err != nil {
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

// heavyPushCall 记录一次重度推送任务投递。
type heavyPushCall struct {
	EventID  string
	VideoID  uint
	AuthorID uint
	Score    int64
}

// fakeHeavyPushPublisher 记录投递到独立队列的重度推送任务。
type fakeHeavyPushPublisher struct {
	calls []heavyPushCall
}

func (f *fakeHeavyPushPublisher) PublishHeavyPush(_ context.Context, eventID string, videoID, authorID uint, score int64) error {
	f.calls = append(f.calls, heavyPushCall{EventID: eventID, VideoID: videoID, AuthorID: authorID, Score: score})
	return nil
}

// TestFanoutBigCreatorPublishesHeavyPushTask 大 V 发布只投递任务，不在写扩散里同步推送。
func TestFanoutBigCreatorPublishesHeavyPushTask(t *testing.T) {
	timeline := newFakeTimelineRepository()
	service := NewFanoutService(
		&fakeCreatorRepository{stats: &domainsocial.Stats{AuthorID: 1, FansCount: 100001, IsBig: true}},
		&fakeFollowerReader{ids: []uint{2, 3, 4}},
		timeline,
		100000,
		2,
	)
	publisher := &fakeHeavyPushPublisher{}
	service.SetHeavyPushPublisher(publisher)

	createTime := time.UnixMilli(1760000000000)
	if err := service.Handle(context.Background(), "evt-7", 9, 1, createTime); err != nil {
		t.Fatalf("fanout: %v", err)
	}
	if len(timeline.outbox[1]) != 1 {
		t.Fatalf("expected outbox entry, got %+v", timeline.outbox[1])
	}
	if len(timeline.inbox) != 0 {
		t.Fatalf("heavy push should be async, got inbox %+v", timeline.inbox)
	}
	if len(publisher.calls) != 1 {
		t.Fatalf("publish calls = %d, want 1", len(publisher.calls))
	}
	call := publisher.calls[0]
	if call.EventID != "evt-7" || call.VideoID != 9 || call.AuthorID != 1 || call.Score != createTime.UnixMilli() {
		t.Fatalf("unexpected publish call: %+v", call)
	}
}

// largeFanoutCall 记录一次大粉丝量写扩散任务投递。
type largeFanoutCall struct {
	EventID  string
	VideoID  uint
	AuthorID uint
	Score    int64
}

// fakeLargeFanoutPublisher 记录大粉丝量写扩散任务。
type fakeLargeFanoutPublisher struct {
	calls []largeFanoutCall
}

func (f *fakeLargeFanoutPublisher) PublishLargeFanout(_ context.Context, eventID string, videoID, authorID uint, score int64) error {
	f.calls = append(f.calls, largeFanoutCall{EventID: eventID, VideoID: videoID, AuthorID: authorID, Score: score})
	return nil
}

// TestFanoutLargeCreatorPublishesTask 粉丝数超过阈值的小 V 转独立队列。
func TestFanoutLargeCreatorPublishesTask(t *testing.T) {
	timeline := newFakeTimelineRepository()
	service := NewFanoutService(
		&fakeCreatorRepository{stats: &domainsocial.Stats{AuthorID: 1, FansCount: 20000}},
		&fakeFollowerReader{ids: []uint{2, 3, 4}},
		timeline,
		100000,
		2,
	)
	publisher := &fakeLargeFanoutPublisher{}
	service.SetLargeFanoutPublisher(publisher)

	createTime := time.UnixMilli(1760000000000)
	if err := service.Handle(context.Background(), "evt-8", 9, 1, createTime); err != nil {
		t.Fatalf("fanout: %v", err)
	}
	if len(timeline.inbox) != 0 {
		t.Fatalf("large fanout should be async, got inbox %+v", timeline.inbox)
	}
	if len(publisher.calls) != 1 {
		t.Fatalf("publish calls = %d, want 1", len(publisher.calls))
	}
	if publisher.calls[0].EventID != "evt-8" || publisher.calls[0].Score != createTime.UnixMilli() {
		t.Fatalf("unexpected publish call: %+v", publisher.calls[0])
	}
}

// fakeProgressSave 记录一次进度保存。
type fakeProgressSave struct {
	Cursor uint
	Done   bool
}

// fakeFanoutProgress 内存进度存储。
type fakeFanoutProgress struct {
	cursor uint
	done   bool
	saves  []fakeProgressSave
}

func (f *fakeFanoutProgress) Load(_ context.Context, _ string) (uint, bool, error) {
	return f.cursor, f.done, nil
}

func (f *fakeFanoutProgress) Save(_ context.Context, _ string, cursor uint, done bool) error {
	f.saves = append(f.saves, fakeProgressSave{Cursor: cursor, Done: done})
	return nil
}

// TestFanoutFollowersResumesFromCursor 断点续传只写未完成的粉丝。
func TestFanoutFollowersResumesFromCursor(t *testing.T) {
	timeline := newFakeTimelineRepository()
	service := NewFanoutService(
		&fakeCreatorRepository{stats: &domainsocial.Stats{AuthorID: 1, FansCount: 3}},
		&fakeFollowerReader{ids: []uint{2, 3, 4}},
		timeline,
		100000,
		2,
	)
	progress := &fakeFanoutProgress{cursor: 3}
	service.SetFanoutProgress(progress)

	if err := service.FanoutFollowers(context.Background(), "evt-9", 9, 1, 1760000000000); err != nil {
		t.Fatalf("fanout followers: %v", err)
	}
	if len(timeline.inbox[2]) != 0 || len(timeline.inbox[3]) != 0 {
		t.Fatalf("should skip written followers, got %+v", timeline.inbox)
	}
	if len(timeline.inbox[4]) != 1 {
		t.Fatalf("expected follower 4 to receive video, got %+v", timeline.inbox[4])
	}
	if len(progress.saves) == 0 || !progress.saves[len(progress.saves)-1].Done {
		t.Fatalf("expected done progress, got %+v", progress.saves)
	}
}

// TestFanoutFollowersSkipsFinishedEvent 已完成的写扩散重投时直接返回。
func TestFanoutFollowersSkipsFinishedEvent(t *testing.T) {
	timeline := newFakeTimelineRepository()
	service := NewFanoutService(
		&fakeCreatorRepository{stats: &domainsocial.Stats{AuthorID: 1, FansCount: 3}},
		&fakeFollowerReader{ids: []uint{2, 3, 4}},
		timeline,
		100000,
		2,
	)
	progress := &fakeFanoutProgress{cursor: 4, done: true}
	service.SetFanoutProgress(progress)

	if err := service.FanoutFollowers(context.Background(), "evt-10", 9, 1, 1760000000000); err != nil {
		t.Fatalf("fanout followers: %v", err)
	}
	if len(timeline.inbox) != 0 {
		t.Fatalf("finished event should not write inbox, got %+v", timeline.inbox)
	}
}

// TestFanoutFollowersSavesPageCursor 每页成功后保存游标，供失败重投续传。
func TestFanoutFollowersSavesPageCursor(t *testing.T) {
	timeline := newFakeTimelineRepository()
	service := NewFanoutService(
		&fakeCreatorRepository{stats: &domainsocial.Stats{AuthorID: 1, FansCount: 4}},
		&fakeFollowerReader{ids: []uint{2, 3, 4, 5}},
		timeline,
		100000,
		2,
	)
	progress := &fakeFanoutProgress{}
	service.SetFanoutProgress(progress)

	if err := service.FanoutFollowers(context.Background(), "evt-11", 9, 1, 1760000000000); err != nil {
		t.Fatalf("fanout followers: %v", err)
	}
	if len(progress.saves) != 3 {
		t.Fatalf("saves = %+v, want 3", progress.saves)
	}
	if progress.saves[0].Cursor != 3 || progress.saves[0].Done {
		t.Fatalf("first save = %+v, want cursor 3 not done", progress.saves[0])
	}
	if progress.saves[1].Cursor != 5 || progress.saves[1].Done {
		t.Fatalf("second save = %+v, want cursor 5 not done", progress.saves[1])
	}
	last := progress.saves[2]
	if last.Cursor != 5 || !last.Done {
		t.Fatalf("last save = %+v, want cursor 5 done", last)
	}
}
