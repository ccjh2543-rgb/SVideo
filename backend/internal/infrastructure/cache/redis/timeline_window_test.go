package redis

import (
	"testing"

	domainfeed "feedsystem_video_go/internal/domain/feed"
)

func TestSelectInboxWindowEntriesWhenNotFull(t *testing.T) {
	entries := []domainfeed.TimelineEntry{
		{VideoID: 1, Score: 100},
		{VideoID: 3, Score: 300},
		{VideoID: 2, Score: 200},
	}
	selected := selectInboxWindowEntries(entries, 298, 300, 0, 0)
	if len(selected) != 2 {
		t.Fatalf("expected 2 entries by remaining capacity, got %d", len(selected))
	}
	if selected[0].VideoID != 3 || selected[1].VideoID != 2 {
		t.Fatalf("expected newest entries [3,2], got %+v", selected)
	}
}

func TestSelectInboxWindowEntriesWhenFull(t *testing.T) {
	entries := []domainfeed.TimelineEntry{
		{VideoID: 1, Score: 1001},
		{VideoID: 51, Score: 1000},
		{VideoID: 50, Score: 1000},
		{VideoID: 9, Score: 999},
	}
	selected := selectInboxWindowEntries(entries, 300, 300, 1000, 50)
	if len(selected) != 2 {
		t.Fatalf("expected 2 entries newer than oldest, got %d", len(selected))
	}
	if selected[0].VideoID != 1 || selected[1].VideoID != 51 {
		t.Fatalf("expected [1,51], got %+v", selected)
	}
}

func TestSelectInboxWindowEntriesFullWithoutOldest(t *testing.T) {
	selected := selectInboxWindowEntries([]domainfeed.TimelineEntry{
		{VideoID: 1, Score: 100},
	}, 300, 300, 0, 0)
	if len(selected) != 0 {
		t.Fatalf("full inbox without oldest should select nothing, got %+v", selected)
	}
}

func TestSelectInboxWindowEntriesEmpty(t *testing.T) {
	if selected := selectInboxWindowEntries(nil, 0, 300, 0, 0); len(selected) != 0 {
		t.Fatalf("empty entries should select nothing, got %+v", selected)
	}
}
