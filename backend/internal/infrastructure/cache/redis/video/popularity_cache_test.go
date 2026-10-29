package video

import (
	"math"
	"testing"
	"time"

	domainfeed "feedsystem_video_go/internal/domain/feed"
)

func TestBuildHotIncrementsAggregatesSameVideo(t *testing.T) {
	at := time.Date(2026, 10, 26, 10, 30, 0, 0, time.UTC)
	updates := []domainfeed.HotUpdate{
		{VideoID: 19, Change: 1, OccurredAt: at, Weight: 1},
		{VideoID: 19, Change: 1, OccurredAt: at, Weight: 1},
	}
	items, invalidated := buildHotIncrements(updates)
	if len(items) != 1 {
		t.Fatalf("expected 1 aggregated item, got %d", len(items))
	}
	expected := 2 * domainfeed.HotIncrement(at, 1, popularityHalfLife)
	if math.Abs(items[0].Score-expected) > 1e-9 {
		t.Fatalf("expected score %f, got %f", expected, items[0].Score)
	}
	if _, ok := invalidated[19]; !ok {
		t.Fatalf("expected video 19 to be invalidated")
	}
}

func TestBuildHotIncrementsCancelsLikeAndUnlike(t *testing.T) {
	at := time.Date(2026, 10, 26, 10, 30, 0, 0, time.UTC)
	updates := []domainfeed.HotUpdate{
		{VideoID: 19, Change: 1, OccurredAt: at, Weight: 1},
		{VideoID: 19, Change: -1, OccurredAt: at, Weight: 1},
	}
	items, _ := buildHotIncrements(updates)
	if len(items) != 0 {
		t.Fatalf("like and unlike should cancel out, got %d items", len(items))
	}
}

func TestBuildHotIncrementsSeparatesHours(t *testing.T) {
	hour1 := time.Date(2026, 10, 26, 10, 30, 0, 0, time.UTC)
	hour2 := time.Date(2026, 10, 26, 11, 30, 0, 0, time.UTC)
	updates := []domainfeed.HotUpdate{
		{VideoID: 19, Change: 1, OccurredAt: hour1, Weight: 1},
		{VideoID: 19, Change: 1, OccurredAt: hour2, Weight: 1},
	}
	items, _ := buildHotIncrements(updates)
	if len(items) != 2 {
		t.Fatalf("expected 2 bucket items, got %d", len(items))
	}
	if items[0].Key == items[1].Key {
		t.Fatalf("different hours should use different candidate keys")
	}
}

func TestBuildHotIncrementsIgnoresInvalid(t *testing.T) {
	items, invalidated := buildHotIncrements([]domainfeed.HotUpdate{
		{VideoID: 0, Change: 1},
		{VideoID: 19, Change: 0},
	})
	if len(items) != 0 {
		t.Fatalf("invalid updates should be ignored, got %d items", len(items))
	}
	if len(invalidated) != 0 {
		t.Fatalf("invalid updates should not invalidate cache")
	}
}
