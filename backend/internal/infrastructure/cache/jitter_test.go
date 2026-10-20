package cache

import (
	"testing"
	"time"
)

func TestJitterRange(t *testing.T) {
	base := 10 * time.Second
	ratio := 0.2
	min := time.Duration(float64(base) * (1 - ratio))
	max := time.Duration(float64(base) * (1 + ratio))

	for i := 0; i < 200; i++ {
		got := Jitter(base, ratio)
		if got < min || got > max {
			t.Fatalf("jitter out of range: %s, expected [%s, %s]", got, min, max)
		}
	}
}

func TestJitterZeroBase(t *testing.T) {
	if got := Jitter(0, 0.2); got != 0 {
		t.Fatalf("expected zero duration, got %s", got)
	}
	if got := Jitter(10*time.Second, 0); got != 10*time.Second {
		t.Fatalf("expected base duration, got %s", got)
	}
}
