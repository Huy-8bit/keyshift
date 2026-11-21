package rediscompat

import (
	"sync"
	"testing"
)

func TestFillGuard(t *testing.T) {
	g := newFillGuard()
	g.track(1, "k")
	if g.finish(1) {
		t.Fatal("no write happened: fill must be accepted")
	}
	g.track(2, "k")
	g.forgot("k")
	if !g.finish(2) {
		t.Fatal("fill overlapping a local write must be rejected")
	}
	g.forgot("k")
	g.track(3, "k") // starts after the write
	if g.finish(3) {
		t.Fatal("fill that started after the write must be accepted")
	}
	if g.finish(99) {
		t.Fatal("unknown token must not be rejected")
	}
	start := g.begin()
	if g.stale("other", start) {
		t.Fatal("unrelated key")
	}
	g.forgot("other")
	if !g.stale("other", start) {
		t.Fatal("stale check")
	}
	if len(g.pending) != 0 {
		t.Fatal("pending tokens leaked")
	}
}

func TestFillGuardConcurrent(t *testing.T) {
	g := newFillGuard()
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Go(func() {
			for i := range 1000 {
				tok := uint64(w*1000 + i)
				g.track(tok, "k")
				if i%3 == 0 {
					g.forgot("k")
				}
				g.finish(tok)
			}
		})
	}
	wg.Wait()
	if len(g.pending) != 0 {
		t.Fatal("pending tokens leaked")
	}
}
