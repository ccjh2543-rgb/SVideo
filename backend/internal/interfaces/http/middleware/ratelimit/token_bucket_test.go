package ratelimit

import "testing"

func TestAllowLocalTokenBucket(t *testing.T) {
	key := "test:local:bucket"
	localBuckets.Delete(key)
	defer localBuckets.Delete(key)

	// 容量 2、不补充：前两次放行，第三次拒绝。
	if !allowLocal(key, 2, 0) {
		t.Fatal("first request should be allowed")
	}
	if !allowLocal(key, 2, 0) {
		t.Fatal("second request should be allowed")
	}
	if allowLocal(key, 2, 0) {
		t.Fatal("third request should be rejected")
	}
}

func TestCleanupLocalBuckets(t *testing.T) {
	key := "test:local:cleanup"
	localBuckets.Delete(key)
	defer localBuckets.Delete(key)

	allowLocal(key, 1, 1)
	// 手动把 lastSeen 提前，模拟长时间未使用。
	value, ok := localBuckets.Load(key)
	if !ok {
		t.Fatal("bucket should exist")
	}
	bucket := value.(*localBucket)
	bucket.mu.Lock()
	bucket.lastSeen = bucket.lastSeen.Add(-(localIdleTTL + 1))
	bucket.mu.Unlock()

	cleanupLocalBuckets()
	if _, ok := localBuckets.Load(key); ok {
		t.Fatal("idle bucket should be removed")
	}
}
