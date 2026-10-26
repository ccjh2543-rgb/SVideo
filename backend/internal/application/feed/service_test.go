package feed

import (
	"testing"
	"time"

	domainfeed "feedsystem_video_go/internal/domain/feed"
	domainvideo "feedsystem_video_go/internal/domain/video"
)

func TestLinearDecayWeights(t *testing.T) {
	weights := linearDecayWeights(24)
	if len(weights) != 24 {
		t.Fatalf("expected 24 weights, got %d", len(weights))
	}
	if weights[0] != 1 {
		t.Fatalf("expected newest weight 1, got %f", weights[0])
	}
	expectedOldest := 1.0 / 24.0
	if diff := weights[23] - expectedOldest; diff > 1e-9 || diff < -1e-9 {
		t.Fatalf("expected oldest weight %f, got %f", expectedOldest, weights[23])
	}
	for i := 1; i < len(weights); i++ {
		if weights[i] > weights[i-1] {
			t.Fatalf("weights should be non-increasing at index %d", i)
		}
	}
}

func TestHotWindowKeys(t *testing.T) {
	asOf := time.Date(2026, 10, 19, 15, 0, 0, 0, time.UTC)
	keys := hotWindowKeys(asOf, 24)
	if len(keys) != 24 {
		t.Fatalf("expected 24 keys, got %d", len(keys))
	}
	if keys[0] != "hot:video:1h:2026101915" {
		t.Fatalf("unexpected newest key: %s", keys[0])
	}
	if keys[23] != "hot:video:1h:2026101816" {
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
