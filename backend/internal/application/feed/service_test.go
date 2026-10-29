package feed

import (
	"context"
	"testing"
	"time"

	domainfeed "feedsystem_video_go/internal/domain/feed"
	domainsocial "feedsystem_video_go/internal/domain/social"
	domainvideo "feedsystem_video_go/internal/domain/video"
)

func TestExponentialDecayWeights(t *testing.T) {
	asOf := time.Date(2026, 10, 19, 15, 0, 0, 0, time.UTC)
	weights := ExponentialDecayWeights(asOf, 24, domainfeed.DefaultHotHalfLife)
	if len(weights) != 24 {
		t.Fatalf("expected 24 weights, got %d", len(weights))
	}
	if weights[0] != 1 {
		t.Fatalf("expected newest weight 1, got %f", weights[0])
	}
	// 半衰期 6 小时，6 小时后的权重应为 1/2。
	if diff := weights[6] - 0.5; diff > 1e-9 || diff < -1e-9 {
		t.Fatalf("expected 6-hour weight 0.5, got %f", weights[6])
	}
	for i := 1; i < len(weights); i++ {
		if weights[i] > weights[i-1] {
			t.Fatalf("weights should be non-increasing at index %d", i)
		}
	}
}

func TestHotWindowKeys(t *testing.T) {
	asOf := time.Date(2026, 10, 19, 15, 0, 0, 0, time.UTC)
	keys := HotWindowKeys(asOf, 24)
	if len(keys) != 24 {
		t.Fatalf("expected 24 keys, got %d", len(keys))
	}
	if keys[0] != "hot:top:1h:2026101915" {
		t.Fatalf("unexpected newest key: %s", keys[0])
	}
	if keys[23] != "hot:top:1h:2026101816" {
		t.Fatalf("unexpected oldest key: %s", keys[23])
	}
}

func TestMergeTimelineEntries(t *testing.T) {
	entries := []domainfeed.TimelineEntry{
		{VideoID: 1, Score: 100},
		{VideoID: 2, Score: 300},
		{VideoID: 1, Score: 200}, // 重复视频，保留更高分
		{VideoID: 3, Score: 300},
		{VideoID: 4, Score: 50},
	}

	merged := mergeTimelineEntries(entries, 3)
	if len(merged) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(merged))
	}
	if merged[0].VideoID != 3 || merged[0].Score != 300 {
		t.Fatalf("unexpected first entry: %+v", merged[0])
	}
	if merged[1].VideoID != 2 || merged[1].Score != 300 {
		t.Fatalf("unexpected second entry: %+v", merged[1])
	}
	if merged[2].VideoID != 1 || merged[2].Score != 200 {
		t.Fatalf("unexpected third entry: %+v", merged[2])
	}
}

func TestFilterTimelineEntries(t *testing.T) {
	entries := []domainfeed.TimelineEntry{
		{VideoID: 60, Score: 1000},
		{VideoID: 50, Score: 1000},
		{VideoID: 40, Score: 1000},
		{VideoID: 99, Score: 999},
		{VideoID: 30, Score: 998},
	}

	tests := []struct {
		name     string
		cursorMs int64
		cursorID uint
		wantIDs  []uint
	}{
		{
			name:     "第一页不过滤",
			cursorMs: 0,
			cursorID: 0,
			wantIDs:  []uint{60, 50, 40, 99, 30},
		},
		{
			name:     "双游标过滤同毫秒",
			cursorMs: 1000,
			cursorID: 50,
			wantIDs:  []uint{40, 99, 30},
		},
		{
			name:     "只有时间游标时丢弃同毫秒",
			cursorMs: 1000,
			cursorID: 0,
			wantIDs:  []uint{99, 30},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filterTimelineEntries(entries, tt.cursorMs, tt.cursorID)
			if len(got) != len(tt.wantIDs) {
				t.Fatalf("expected %d entries, got %d: %+v", len(tt.wantIDs), len(got), got)
			}
			for i, wantID := range tt.wantIDs {
				if got[i].VideoID != wantID {
					t.Fatalf("index %d: expected video %d, got %d", i, wantID, got[i].VideoID)
				}
			}
		})
	}
}

func TestFilterVideosByAuthors(t *testing.T) {
	videos := []*domainvideo.Video{
		{ID: 1, AuthorID: 10},
		{ID: 2, AuthorID: 20},
		{ID: 3, AuthorID: 30},
		nil,
	}
	allowed := map[uint]struct{}{
		10: {},
		30: {},
	}

	got := filterVideosByAuthors(videos, allowed)
	if len(got) != 2 {
		t.Fatalf("expected 2 videos, got %d", len(got))
	}
	if got[0].ID != 1 || got[1].ID != 3 {
		t.Fatalf("unexpected videos: %+v", got)
	}
}

func TestAssembleFollowingPage(t *testing.T) {
	buildVideos := func(n int) []*domainvideo.Video {
		videos := make([]*domainvideo.Video, 0, n)
		for i := 0; i < n; i++ {
			videos = append(videos, &domainvideo.Video{ID: uint(i + 1)})
		}
		return videos
	}

	tests := []struct {
		name           string
		videoCount     int
		consumed       int
		candidateCount int
		limit          int
		wantLen        int
		wantHasMore    bool
	}{
		{
			name:           "候选刚好一页",
			videoCount:     10,
			consumed:       10,
			candidateCount: 10,
			limit:          10,
			wantLen:        10,
			wantHasMore:    false,
		},
		{
			name:           "跳过已删除视频后凑满一页",
			videoCount:     10,
			consumed:       12,
			candidateCount: 12,
			limit:          10,
			wantLen:        10,
			wantHasMore:    false,
		},
		{
			name:           "候选超过一页只返回 limit 条",
			videoCount:     15,
			consumed:       10,
			candidateCount: 15,
			limit:          10,
			wantLen:        10,
			wantHasMore:    true,
		},
		{
			name:           "候选不足一页",
			videoCount:     3,
			consumed:       3,
			candidateCount: 3,
			limit:          10,
			wantLen:        3,
			wantHasMore:    false,
		},
		{
			name:           "候选取完仍不足一页交给 DB 补页",
			videoCount:     8,
			consumed:       12,
			candidateCount: 12,
			limit:          10,
			wantLen:        8,
			wantHasMore:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			videos, hasMore := assembleFollowingPage(buildVideos(tt.videoCount), tt.consumed, tt.candidateCount, tt.limit)
			if len(videos) != tt.wantLen {
				t.Fatalf("expected %d videos, got %d", tt.wantLen, len(videos))
			}
			if hasMore != tt.wantHasMore {
				t.Fatalf("expected hasMore=%v, got %v", tt.wantHasMore, hasMore)
			}
		})
	}
}

func bigVStats(authorID uint, lastPublishMs int64) *domainsocial.Stats {
	stats := &domainsocial.Stats{
		AuthorID:  authorID,
		IsBig:     true,
		FansCount: domainsocial.BigVFansThreshold,
	}
	if lastPublishMs > 0 {
		stats.LastPublishTime = time.UnixMilli(lastPublishMs)
	}
	return stats
}

func TestMergeBigVOutboxesPrunesInactiveAuthors(t *testing.T) {
	timeline := newFakeTimelineRepository()
	stats := make([]*domainsocial.Stats, 0, 300)
	stats = append(stats, bigVStats(1, 2000), bigVStats(2, 1999))
	for i := 0; i < 298; i++ {
		stats = append(stats, bigVStats(uint(i+3), 100))
	}
	timeline.outbox[1] = []domainfeed.TimelineEntry{{VideoID: 1, Score: 2000}, {VideoID: 2, Score: 1900}}
	timeline.outbox[2] = []domainfeed.TimelineEntry{{VideoID: 3, Score: 1900}}
	timeline.outbox[3] = []domainfeed.TimelineEntry{{VideoID: 4, Score: 100}}

	service := &FeedService{
		timelineRepo:    timeline,
		bigVBatchSize:   32,
		bigVMergeBudget: time.Second,
	}
	entries, unreadUpper, partial, err := service.mergeBigVOutboxes(context.Background(), stats, 0, 0, 3)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}
	if partial {
		t.Fatal("should stop by pruning, not budget")
	}
	if unreadUpper != -1 {
		t.Fatalf("expected no watermark, got %d", unreadUpper)
	}
	if len(timeline.readAuthorIDs) > 32 {
		t.Fatalf("inactive authors should be pruned, read %d authors", len(timeline.readAuthorIDs))
	}
	seen := make(map[uint]bool, len(entries))
	for _, entry := range entries {
		seen[entry.VideoID] = true
	}
	for _, want := range []uint{1, 2, 3} {
		if !seen[want] {
			t.Fatalf("missing video %d in %+v", want, entries)
		}
	}
}

func TestMergeBigVOutboxesKeepsTieAuthors(t *testing.T) {
	timeline := newFakeTimelineRepository()
	// authorID 2 排在前面，但它只有 videoID=5；authorID 1 的同分 videoID=9 更大。
	timeline.outbox[2] = []domainfeed.TimelineEntry{{VideoID: 5, Score: 1000}}
	timeline.outbox[1] = []domainfeed.TimelineEntry{{VideoID: 9, Score: 1000}}

	service := &FeedService{
		timelineRepo:    timeline,
		bigVBatchSize:   1,
		bigVMergeBudget: time.Second,
	}
	entries, _, partial, err := service.mergeBigVOutboxes(
		context.Background(),
		[]*domainsocial.Stats{bigVStats(2, 1000), bigVStats(1, 1000)},
		0,
		0,
		1,
	)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if partial {
		t.Fatal("unexpected partial")
	}
	if len(entries) != 1 || entries[0].VideoID != 9 {
		t.Fatalf("same-score authors must both be read, got %+v", entries)
	}
	if len(timeline.readAuthorIDs) != 2 {
		t.Fatalf("expected both authors read, got %v", timeline.readAuthorIDs)
	}
}

func TestMergeBigVOutboxesReadsUnknownPublishTime(t *testing.T) {
	timeline := newFakeTimelineRepository()
	timeline.outbox[1] = []domainfeed.TimelineEntry{{VideoID: 1, Score: 100}}
	timeline.outbox[2] = []domainfeed.TimelineEntry{{VideoID: 2, Score: 1000}}

	service := &FeedService{
		timelineRepo:    timeline,
		bigVBatchSize:   1,
		bigVMergeBudget: time.Second,
	}
	entries, _, _, err := service.mergeBigVOutboxes(
		context.Background(),
		[]*domainsocial.Stats{bigVStats(1, 0), bigVStats(2, 1000)},
		0,
		0,
		1,
	)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if len(entries) != 1 || entries[0].VideoID != 2 {
		t.Fatalf("unexpected entries: %+v", entries)
	}
	if len(timeline.readAuthorIDs) == 0 || timeline.readAuthorIDs[0] != 1 {
		t.Fatalf("unknown publish time author must be read first, got %v", timeline.readAuthorIDs)
	}
}

func TestMergeBigVOutboxesBudgetWatermark(t *testing.T) {
	timeline := newFakeTimelineRepository()
	timeline.outbox[1] = []domainfeed.TimelineEntry{{VideoID: 1, Score: 3000}}
	timeline.outbox[2] = []domainfeed.TimelineEntry{{VideoID: 2, Score: 2000}}
	timeline.outbox[3] = []domainfeed.TimelineEntry{{VideoID: 3, Score: 1000}}

	service := &FeedService{
		timelineRepo:    timeline,
		bigVBatchSize:   1,
		bigVMergeBudget: time.Millisecond,
	}
	timeline.readDelay = 2 * time.Millisecond
	entries, unreadUpper, partial, err := service.mergeBigVOutboxes(
		context.Background(),
		[]*domainsocial.Stats{bigVStats(1, 3000), bigVStats(2, 2000), bigVStats(3, 1000)},
		0,
		0,
		10,
	)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if !partial {
		t.Fatal("expected partial result when budget is exhausted")
	}
	if unreadUpper != 2000 {
		t.Fatalf("expected watermark 2000, got %d", unreadUpper)
	}
	if len(entries) != 1 || entries[0].Score != 3000 {
		t.Fatalf("unexpected entries: %+v", entries)
	}

	safe := filterEntriesAbove(entries, unreadUpper)
	if len(safe) != 1 || safe[0].Score != 3000 {
		t.Fatalf("watermark filter should keep only score > 2000, got %+v", safe)
	}
}
