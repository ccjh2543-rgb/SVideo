package feed

import (
	"testing"
	"time"

	domainfeed "feedsystem_video_go/internal/domain/feed"
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
