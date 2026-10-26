package resilience

import (
	"errors"
	"testing"
	"time"
)

func TestBreakerTripsAndRecovers(t *testing.T) {
	now := time.Unix(0, 0)
	clock := func() time.Time { return now }
	breaker := newBreaker("redis-feed", Config{
		Window:         10 * time.Second,
		MinRequests:    5,
		FailureRatio:   0.5,
		OpenTimeout:    5 * time.Second,
		HalfOpenProbes: 1,
		CloseSuccesses: 1,
	}, clock)

	// 连续失败达到阈值后熔断。
	for i := 0; i < 5; i++ {
		if !breaker.Allow() {
			t.Fatalf("closed breaker should allow request %d", i)
		}
		breaker.Record(errors.New("redis timeout"))
	}
	if breaker.State() != StateOpen {
		t.Fatalf("expected open, got %s", breaker.State())
	}
	if breaker.Allow() {
		t.Fatal("open breaker should reject")
	}

	// 冷却结束后进入半开，探测成功则恢复。
	now = now.Add(6 * time.Second)
	if !breaker.Allow() {
		t.Fatal("half-open breaker should allow one probe")
	}
	if breaker.State() != StateHalfOpen {
		t.Fatalf("expected half-open, got %s", breaker.State())
	}
	breaker.Record(nil)
	if breaker.State() != StateClosed {
		t.Fatalf("expected closed, got %s", breaker.State())
	}
}

func TestBreakerReopensOnHalfOpenFailure(t *testing.T) {
	now := time.Unix(0, 0)
	clock := func() time.Time { return now }
	breaker := newBreaker("redis-detail", Config{
		Window:         10 * time.Second,
		MinRequests:    2,
		FailureRatio:   0.5,
		OpenTimeout:    5 * time.Second,
		HalfOpenProbes: 1,
		CloseSuccesses: 1,
	}, clock)

	breaker.Allow()
	breaker.Record(errors.New("timeout"))
	breaker.Allow()
	breaker.Record(errors.New("timeout"))
	if breaker.State() != StateOpen {
		t.Fatalf("expected open, got %s", breaker.State())
	}

	now = now.Add(6 * time.Second)
	if !breaker.Allow() {
		t.Fatal("expected half-open probe")
	}
	breaker.Record(errors.New("still down"))
	if breaker.State() != StateOpen {
		t.Fatalf("expected open after failed probe, got %s", breaker.State())
	}
}

func TestGuardIsolatesDependencies(t *testing.T) {
	guard := NewGuard("redis-feed", "redis-popularity")
	for i := 0; i < 20; i++ {
		guard.Allow("redis-feed")
		guard.Record("redis-feed", errors.New("timeout"))
	}
	if guard.State("redis-feed") != StateOpen {
		t.Fatalf("expected redis-feed open, got %s", guard.State("redis-feed"))
	}
	if guard.State("redis-popularity") != StateClosed {
		t.Fatalf("redis-popularity should stay closed, got %s", guard.State("redis-popularity"))
	}
	if !guard.Allow("redis-popularity") {
		t.Fatal("unrelated dependency should still be allowed")
	}
}
