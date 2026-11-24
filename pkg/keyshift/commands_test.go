package keyshift

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

// admitKey reads key often enough over two windows to admit it.
func admitKey(t *testing.T, c *Client, key string) {
	t.Helper()
	ctx := context.Background()
	now := time.Now()
	for range 2 {
		for range 500 {
			_, _ = c.Get(ctx, key)
		}
		now = now.Add(10 * time.Second)
		rotateAt(c, now)
	}
	if d := c.Explain(key); d.State != "admitted" {
		t.Fatalf("precondition: %s not admitted: %+v", key, d)
	}
}

func TestMGetCombinesCacheAndOnePipeline(t *testing.T) {
	c, fb := newTestClient(t, testConfig())
	ctx := context.Background()
	fb.data["hot"] = []byte("h")
	fb.data["a"] = []byte("1")
	fb.data["b"] = []byte("2")
	admitKey(t, c, "hot")
	_, _ = c.Get(ctx, "hot") // fill the local entry

	readsBefore, hitsBefore, batchesBefore := c.Stats().Reads, fb.hits.Load(), fb.batches.Load()
	got, err := c.MGet(ctx, "a", "hot", "missing", "b", "a")
	if err != nil {
		t.Fatal(err)
	}
	want := []KeyValue{
		{Key: "a", Value: "1", Found: true},
		{Key: "hot", Value: "h", Found: true},
		{Key: "missing"},
		{Key: "b", Value: "2", Found: true},
		{Key: "a", Value: "1", Found: true},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d results", len(got))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("result %d: got %+v, want %+v", i, got[i], want[i])
		}
	}
	if n := fb.batches.Load() - batchesBefore; n != 1 {
		t.Fatalf("uncached keys must be read in one pipeline, got %d", n)
	}
	if fb.hits.Load() != hitsBefore+1 {
		t.Fatal("the admitted key must be served from the local cache")
	}
	s := c.Stats()
	if s.Reads != readsBefore+5 {
		t.Fatalf("every key is a logical read: %d", s.Reads-readsBefore)
	}
}

func TestMGetCountsTowardDetection(t *testing.T) {
	c, fb := newTestClient(t, testConfig())
	ctx := context.Background()
	fb.data["k"] = []byte("v")
	now := time.Now()
	for range 2 {
		for range 500 {
			if _, err := c.MGet(ctx, "k"); err != nil {
				t.Fatal(err)
			}
		}
		now = now.Add(10 * time.Second)
		rotateAt(c, now)
	}
	if d := c.Explain("k"); d.State != "admitted" {
		t.Fatalf("keys read with MGet must be admitted like Get: %+v", d)
	}
}

func TestMGetReturnsFirstError(t *testing.T) {
	c, fb := newTestClient(t, testConfig())
	boom := errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	fb.directErr.Store(&boom)
	before := c.Stats().Errors
	if _, err := c.MGet(context.Background(), "a", "b"); !errors.Is(err, boom) {
		t.Fatalf("got %v", err)
	}
	if c.Stats().Errors != before+1 {
		t.Fatal("a failed MGet counts one error")
	}
}

func TestMGetEmptyAndClosed(t *testing.T) {
	c, _ := newTestClient(t, testConfig())
	if got, err := c.MGet(context.Background()); err != nil || len(got) != 0 {
		t.Fatalf("empty MGet: %v %v", got, err)
	}
	_ = c.Close()
	if _, err := c.MGet(context.Background(), "a"); !errors.Is(err, ErrClosed) {
		t.Fatalf("got %v", err)
	}
}

func TestMGetFallsBackWhenCachePathFails(t *testing.T) {
	c, fb := newTestClient(t, testConfig())
	ctx := context.Background()
	fb.data["hot"] = []byte("h")
	admitKey(t, c, "hot")
	var cut error = &net.OpError{Op: "read", Err: errors.New("connection reset by peer")}
	fb.cachedErr.Store(&cut)
	got, err := c.MGet(ctx, "hot")
	if err != nil || !got[0].Found || got[0].Value != "h" {
		t.Fatalf("transport failure on the cache path must fall back: %+v %v", got, err)
	}
	if c.Stats().CacheFallbacks != 1 {
		t.Fatal("fallback not counted")
	}
}

func TestNotifyWriteRestoresReadYourWrites(t *testing.T) {
	c, fb := newTestClient(t, testConfig())
	ctx := context.Background()
	fb.data["k"] = []byte("old")
	admitKey(t, c, "k")
	if v, _ := c.Get(ctx, "k"); v != "old" {
		t.Fatal("precondition: cached old value")
	}
	// A write through another path whose invalidation has not arrived yet.
	fb.mu.Lock()
	fb.data["k"] = []byte("new")
	fb.mu.Unlock()
	if v, _ := c.Get(ctx, "k"); v != "old" {
		t.Fatal("precondition: the local entry is still served")
	}
	writes := c.Stats().Writes
	c.NotifyWrite("k")
	if v, _ := c.Get(ctx, "k"); v != "new" {
		t.Fatalf("NotifyWrite must drop the local entry, got %q", v)
	}
	if s := c.Stats(); s.NotifiedWrites != 1 || s.Writes != writes {
		t.Fatalf("NotifyWrite is counted separately from Set: %+v", s)
	}
	_ = c.Close()
	c.NotifyWrite("k") // no-op after Close
}
