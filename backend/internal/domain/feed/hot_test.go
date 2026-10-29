package feed

import (
	"math"
	"testing"
	"time"
)

func TestHotIncrementLaterEventHasHigherWeight(t *testing.T) {
	base := time.Date(2026, 10, 26, 10, 0, 0, 0, time.UTC)
	early := HotIncrement(base.Add(time.Minute), 1, DefaultHotHalfLife)
	late := HotIncrement(base.Add(59*time.Minute), 1, DefaultHotHalfLife)
	if late <= early {
		t.Fatalf("later event should have higher increment: early=%f late=%f", early, late)
	}
}

func TestHotDecayWeights(t *testing.T) {
	asOf := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	weights := HotDecayWeights(asOf, HotWindowHours, DefaultHotHalfLife)
	if len(weights) != HotWindowHours {
		t.Fatalf("expected %d weights, got %d", HotWindowHours, len(weights))
	}
	if math.Abs(weights[0]-1) > 1e-9 {
		t.Fatalf("newest weight should be 1, got %f", weights[0])
	}
	// 半衰期 6 小时，6 小时后权重应为 1/2。
	if math.Abs(weights[6]-0.5) > 1e-9 {
		t.Fatalf("expected 6-hour weight 0.5, got %f", weights[6])
	}
	for i := 1; i < len(weights); i++ {
		if weights[i] > weights[i-1] {
			t.Fatalf("weights should be non-increasing at index %d", i)
		}
	}
}

func TestHotRankingRatioStableOverTime(t *testing.T) {
	asOf := time.Date(2026, 10, 26, 11, 0, 0, 0, time.UTC)
	weights := HotDecayWeights(asOf, HotWindowHours, DefaultHotHalfLife)

	// A 在当前小时有 9 分，B 在上一小时有 15 分。
	scoreA := 9 * weights[0]
	scoreB := 15 * weights[1]
	before := scoreA / scoreB

	nextWeights := HotDecayWeights(asOf.Add(time.Hour), HotWindowHours, DefaultHotHalfLife)
	// 过一小时后，A 的桶变成上一小时，B 的桶变成两小时前。
	after := (9 * nextWeights[1]) / (15 * nextWeights[2])
	if math.Abs(before-after) > 1e-9 {
		t.Fatalf("relative ratio should be stable: before=%f after=%f", before, after)
	}
}

func TestHotBucketAndShardKeys(t *testing.T) {
	bucket := time.Date(2026, 10, 26, 15, 0, 0, 0, time.UTC)
	if got := HotCandidateKey(bucket, 3); got != "hot:candidate:1h:2026102615:3" {
		t.Fatalf("unexpected candidate key: %s", got)
	}
	if got := HotTopKey(bucket); got != "hot:top:1h:2026102615" {
		t.Fatalf("unexpected top key: %s", got)
	}
	if got := HotSnapshotKey(bucket); got != "hot:video:merge:1h:2026102615" {
		t.Fatalf("unexpected snapshot key: %s", got)
	}
	if got := HotShard(19); got != 3 {
		t.Fatalf("expected shard 3, got %d", got)
	}
}

func TestHotTopWindowKeys(t *testing.T) {
	asOf := time.Date(2026, 10, 26, 15, 0, 0, 0, time.UTC)
	keys := HotTopWindowKeys(asOf, 24)
	if len(keys) != 24 {
		t.Fatalf("expected 24 keys, got %d", len(keys))
	}
	if keys[0] != "hot:top:1h:2026102615" {
		t.Fatalf("unexpected newest key: %s", keys[0])
	}
	if keys[23] != "hot:top:1h:2026102516" {
		t.Fatalf("unexpected oldest key: %s", keys[23])
	}
}
