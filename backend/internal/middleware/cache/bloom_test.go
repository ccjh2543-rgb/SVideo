package cache

import "testing"

func TestBloomPositionsDeterministic(t *testing.T) {
	filter := NewBloomFilter(nil, "bloom:test", 1<<10, 7)

	first := filter.positions(42)
	second := filter.positions(42)
	if len(first) != 7 || len(second) != 7 {
		t.Fatalf("expected 7 positions, got %d/%d", len(first), len(second))
	}
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("positions should be deterministic at index %d", i)
		}
		if first[i] >= filter.bits {
			t.Fatalf("position %d out of range %d", first[i], filter.bits)
		}
	}
}

func TestBloomDifferentIDs(t *testing.T) {
	filter := NewBloomFilter(nil, "bloom:test", 1<<10, 7)
	first := filter.positions(1)
	second := filter.positions(2)

	same := 0
	for i := range first {
		if first[i] == second[i] {
			same++
		}
	}
	if same == len(first) {
		t.Fatal("expected different IDs to produce different positions")
	}
}
