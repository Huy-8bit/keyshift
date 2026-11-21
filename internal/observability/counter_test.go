package observability

import (
	"sync"
	"testing"
)

func TestCounter(t *testing.T) {
	var c Counter
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 1000 {
				c.Inc()
			}
			c.Add(10)
		})
	}
	wg.Wait()
	if got := c.Load(); got != 8*1010 {
		t.Fatalf("got %d", got)
	}
}

func BenchmarkCounterParallel(b *testing.B) {
	var c Counter
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Inc()
		}
	})
}
