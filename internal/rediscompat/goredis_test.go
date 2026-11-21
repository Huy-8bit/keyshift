package rediscompat

import (
	"context"
	"testing"
	"time"
)

// TestGatedCacheRejectsFillOverlappingLocalWrite reproduces the race found by
// the local-hot benchmark: a fill that started before a local write must not
// be cached after the write's Forget.
func TestGatedCacheRejectsFillOverlappingLocalWrite(t *testing.T) {
	c := newGatedCache(CacheLimits{MaxEntries: 100, MaxMemoryBytes: 1 << 20, MaxStaleness: time.Minute}, 0, "", Hooks{})
	ns := c.prefix + "k"
	cacheKey := "get" + ns // opaque to the cache; any stable string works

	// Normal fill is cached.
	tok, should := c.Reserve(cacheKey, []string{ns})
	if !should || !c.FulfillOwned(cacheKey, tok, 1, []byte("v1")) {
		t.Fatal("plain fill should be cached")
	}
	if v, ok := c.Get(context.Background(), cacheKey); !ok || string(v) != "v1" {
		t.Fatal("expected cached v1")
	}

	// Local write: Forget drops v1. A fill that started before the write
	// completes afterwards with the old value and must be rejected.
	c.forget("k")
	tok, should = c.Reserve(cacheKey, []string{ns})
	if !should {
		t.Fatal("expected a fresh reservation")
	}
	c.forget("k") // the write completes while the fill is in flight
	if c.FulfillOwned(cacheKey, tok, 1, []byte("v1-stale")) {
		t.Fatal("fill overlapping a local write was cached")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, ok := c.Get(ctx, cacheKey); ok {
		t.Fatal("stale value is resident")
	}
	if c.staleFills.Load() != 1 {
		t.Fatalf("staleFills=%d", c.staleFills.Load())
	}

	// A fill that starts after the write is cached again.
	tok, should = c.Reserve(cacheKey, []string{ns})
	if !should || !c.FulfillOwned(cacheKey, tok, 1, []byte("v2")) {
		t.Fatal("post-write fill should be cached")
	}
	if len(c.guard.pending) != 0 {
		t.Fatal("pending reservations leaked")
	}
}
