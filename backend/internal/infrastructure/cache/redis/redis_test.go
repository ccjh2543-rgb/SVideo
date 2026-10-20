package redis

import (
	"context"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
)

func TestTakeTokenConsumesAndRefills(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer mr.Close()

	client := &Client{
		rdb: goredis.NewClient(&goredis.Options{Addr: mr.Addr()}),
	}
	defer client.Close()

	ctx := context.Background()
	key := "feedsystem:ratelimit:tokenbucket:test"

	// 容量 2，补充速率 100/s：先耗尽，再等待 30ms 让令牌恢复。
	for i := 0; i < 2; i++ {
		allowed, _, err := client.TakeToken(ctx, key, 2, 100, time.Minute)
		if err != nil {
			t.Fatalf("take token %d: %v", i, err)
		}
		if !allowed {
			t.Fatalf("expected token %d to be allowed", i)
		}
	}

	allowed, _, err := client.TakeToken(ctx, key, 2, 100, time.Minute)
	if err != nil {
		t.Fatalf("take token after exhaustion: %v", err)
	}
	if allowed {
		t.Fatal("expected token bucket to be exhausted")
	}

	time.Sleep(30 * time.Millisecond)
	allowed, _, err = client.TakeToken(ctx, key, 2, 100, time.Minute)
	if err != nil {
		t.Fatalf("take token after refill: %v", err)
	}
	if !allowed {
		t.Fatal("expected token bucket to refill")
	}
}

func TestIncrementWithExpireSetsTTLWithoutExtendingWindow(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer mr.Close()

	client := &Client{
		rdb: goredis.NewClient(&goredis.Options{Addr: mr.Addr()}),
	}
	defer client.Close()

	ctx := context.Background()
	key := "feedsystem:ratelimit:test"
	expire := 30 * time.Second

	count, err := client.IncrementWithExpire(ctx, key, expire)
	if err != nil {
		t.Fatalf("first increment: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected count 1, got %d", count)
	}

	firstTTL := mr.TTL(key)
	if firstTTL <= 0 || firstTTL > expire {
		t.Fatalf("expected ttl in (0, %s], got %s", expire, firstTTL)
	}

	mr.FastForward(5 * time.Second)
	ttlBeforeSecond := mr.TTL(key)

	count, err = client.IncrementWithExpire(ctx, key, expire)
	if err != nil {
		t.Fatalf("second increment: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected count 2, got %d", count)
	}

	ttlAfterSecond := mr.TTL(key)
	if ttlAfterSecond != ttlBeforeSecond {
		t.Fatalf("expected ttl to stay at %s, got %s", ttlBeforeSecond, ttlAfterSecond)
	}
}
