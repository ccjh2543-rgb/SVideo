package redis

import (
	"context"
	"testing"

	miniredis "github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
)

func TestSetNX(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer mr.Close()

	client := &Client{rdb: goredis.NewClient(&goredis.Options{Addr: mr.Addr()})}
	defer client.Close()

	ctx := context.Background()
	ok, err := client.SetNX(ctx, "idem:test", []byte("1"), 0)
	if err != nil || !ok {
		t.Fatalf("first SetNX should succeed: ok=%v err=%v", ok, err)
	}
	ok, err = client.SetNX(ctx, "idem:test", []byte("1"), 0)
	if err != nil {
		t.Fatalf("second SetNX: %v", err)
	}
	if ok {
		t.Fatal("second SetNX should fail")
	}
}

func TestSetBitsAndGetBits(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer mr.Close()

	client := &Client{rdb: goredis.NewClient(&goredis.Options{Addr: mr.Addr()})}
	defer client.Close()

	ctx := context.Background()
	key := "bloom:test"
	offsets := []uint64{1, 5, 100}

	if err := client.SetBits(ctx, key, offsets); err != nil {
		t.Fatalf("set bits: %v", err)
	}

	values, err := client.GetBits(ctx, key, []uint64{1, 5, 100, 2})
	if err != nil {
		t.Fatalf("get bits: %v", err)
	}
	expected := []bool{true, true, true, false}
	for i := range expected {
		if values[i] != expected[i] {
			t.Fatalf("bit %d: expected %v, got %v", i, expected[i], values[i])
		}
	}
}
