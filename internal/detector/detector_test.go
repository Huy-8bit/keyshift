package detector

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"
)

func mustNew(t testing.TB, cfg Config) *Detector {
	t.Helper()
	d, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestConfigValidate(t *testing.T) {
	bad := []Config{
		{Capacity: 0, TopK: 1, SampleRate: 1},
		{Capacity: 10, TopK: 0, SampleRate: 1},
		{Capacity: 10, TopK: 11, SampleRate: 1},
		{Capacity: 10, TopK: 5, SampleRate: 0},
		{Capacity: 10, TopK: 5, SampleRate: 1.5},
		{Capacity: 10, TopK: 5, SampleRate: 1, DecayShift: 17},
	}
	for i, c := range bad {
		if _, err := New(c); err == nil {
			t.Errorf("case %d: expected error for %+v", i, c)
		}
	}
}

func TestExactWhenUnderCapacity(t *testing.T) {
	d := mustNew(t, Config{Capacity: 100, TopK: 10, SampleRate: 1})
	for i := range 10 {
		for range (i + 1) * 10 {
			d.Observe("k"+strconv.Itoa(i), int64(i))
		}
	}
	s := d.Rotate(time.Now())
	if len(s.Top) != 10 {
		t.Fatalf("top=%d", len(s.Top))
	}
	for rank, c := range s.Top {
		want := 9 - rank
		if c.Key != "k"+strconv.Itoa(want) || c.Window != uint64((want+1)*10) || c.Err != 0 {
			t.Fatalf("rank %d: got %+v", rank, c)
		}
		if c.SizeBytes != int64(want) {
			t.Fatalf("size not tracked: %+v", c)
		}
	}
	if s.Observed != 550 || s.Recorded != 550 || s.Dropped != 0 {
		t.Fatalf("totals: %+v", s)
	}
	if cov := s.Coverage(); cov != 1 {
		t.Fatalf("coverage=%v", cov)
	}
}

// zipfStream returns a deterministic Zipf-distributed key stream.
func zipfStream(n int, keys uint64, s float64, seed uint64) []string {
	r := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	z := rand.NewZipf(r, s, 1, keys-1)
	out := make([]string, n)
	for i := range out {
		out[i] = "key:" + strconv.FormatUint(z.Uint64(), 10)
	}
	return out
}

func TestTopKRecallZipf(t *testing.T) {
	const topK = 50
	stream := zipfStream(500_000, 1_000_000, 1.1, 42)
	exact := map[string]uint64{}
	for _, k := range stream {
		exact[k]++
	}
	type kv struct {
		k string
		v uint64
	}
	var all []kv
	for k, v := range exact {
		all = append(all, kv{k, v})
	}
	slices.SortFunc(all, func(a, b kv) int { return int(b.v) - int(a.v) })
	truth := map[string]bool{}
	for _, e := range all[:topK] {
		truth[e.k] = true
	}

	for _, capacity := range []int{500, 1000, 4000} {
		t.Run(fmt.Sprintf("capacity=%d", capacity), func(t *testing.T) {
			d := mustNew(t, Config{Capacity: capacity, TopK: topK, SampleRate: 1})
			for _, k := range stream {
				d.Observe(k, -1)
			}
			s := d.Rotate(time.Now())
			hit := 0
			for _, c := range s.Top {
				if truth[c.Key] {
					hit++
				}
				// Space-Saving guarantee: count - err <= true <= count.
				if c.Score < exact[c.Key] || c.Score-c.Err > exact[c.Key] {
					t.Fatalf("bound violated for %s: score=%d err=%d exact=%d", c.Key, c.Score, c.Err, exact[c.Key])
				}
				if c.Window > exact[c.Key] {
					t.Fatalf("window count %d exceeds exact %d", c.Window, exact[c.Key])
				}
			}
			recall := float64(hit) / topK
			t.Logf("capacity=%d recall@%d=%.2f tracked=%d coverage=%.3f", capacity, topK, recall, s.Tracked, s.Coverage())
			if s.Tracked > capacity {
				t.Fatalf("tracked %d exceeds capacity %d", s.Tracked, capacity)
			}
			if recall < 0.9 {
				t.Fatalf("recall %.2f below 0.9", recall)
			}
		})
	}
}

func TestRotationDecaysAndForgets(t *testing.T) {
	d := mustNew(t, Config{Capacity: 64, TopK: 8, SampleRate: 1})
	for range 8 {
		d.Observe("hot", -1)
	}
	d.Observe("once", -1)
	s1 := d.Rotate(time.Now())
	if len(s1.Top) != 2 || s1.Top[0].Key != "hot" {
		t.Fatalf("unexpected first window: %+v", s1.Top)
	}
	if c, _, ok := d.Estimate("hot"); !ok || c != 4 {
		t.Fatalf("expected decayed count 4, got %d ok=%v", c, ok)
	}
	if _, _, ok := d.Estimate("once"); ok {
		t.Fatal("count 1 should decay to zero and be removed")
	}
	// Empty window: nothing observed, top is empty but counters survive decay.
	s2 := d.Rotate(time.Now())
	if len(s2.Top) != 0 || s2.Observed != 0 {
		t.Fatalf("empty window reported %+v", s2)
	}
	for range 3 {
		d.Rotate(time.Now())
	}
	if s := d.Rotate(time.Now()); s.Tracked != 0 {
		t.Fatalf("expected all counters to decay away, tracked=%d", s.Tracked)
	}
}

func TestDroppedLongKeys(t *testing.T) {
	d := mustNew(t, Config{Capacity: 8, TopK: 4, SampleRate: 1, MaxKeyBytes: 4})
	d.Observe("12345", -1)
	d.Observe("", -1)
	d.Observe("ok", -1)
	s := d.Rotate(time.Now())
	if s.Dropped != 2 || s.Recorded != 1 || len(s.Top) != 1 {
		t.Fatalf("unexpected snapshot %+v", s)
	}
}

func TestSamplingWeights(t *testing.T) {
	d := mustNew(t, Config{Capacity: 16, TopK: 1, SampleRate: 0.1})
	const n = 200_000
	for range n {
		d.Observe("k", -1)
	}
	s := d.Rotate(time.Now())
	if s.Observed != n {
		t.Fatalf("observed=%d", s.Observed)
	}
	got := float64(s.Top[0].Window)
	if got < n*0.9 || got > n*1.1 {
		t.Fatalf("sampled estimate %v too far from %d", got, n)
	}
}

func TestConcurrentObserveAndRotate(t *testing.T) {
	d := mustNew(t, Config{Capacity: 256, TopK: 16, SampleRate: 1})
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(g), 1))
			for {
				select {
				case <-stop:
					return
				default:
				}
				d.Observe("k"+strconv.Itoa(r.IntN(2000)), 10)
			}
		}()
	}
	var total uint64
	deadline := time.Now().Add(5 * time.Second)
	for i := 0; i < 50 || (total == 0 && time.Now().Before(deadline)); i++ {
		s := d.Rotate(time.Now())
		total += s.Observed
		if s.Tracked > 256 || len(s.Top) > 16 {
			t.Fatalf("bounds violated: %+v", s)
		}
		time.Sleep(100 * time.Microsecond) // let observers run between rotations
	}
	close(stop)
	wg.Wait()
	if total == 0 {
		t.Fatal("no observations recorded")
	}
}

func TestShardCountNeverExceedsCapacity(t *testing.T) {
	for _, c := range []int{1, 2, 3, 7, 63, 64, 65, 1000, 100000} {
		d := mustNew(t, Config{Capacity: c, TopK: 1, SampleRate: 1, Shards: 128})
		total := 0
		for i := range d.shards {
			if d.shards[i].sum.capacity < 1 {
				t.Fatalf("capacity=%d produced empty shard", c)
			}
			total += d.shards[i].sum.capacity
		}
		if total != c {
			t.Fatalf("capacity=%d distributed as %d", c, total)
		}
	}
}

func FuzzDetector(f *testing.F) {
	f.Add([]byte("a\x00b\x00a\x00c"), uint8(3), uint8(2))
	f.Add([]byte{}, uint8(1), uint8(1))
	f.Fuzz(func(t *testing.T, data []byte, capacity, topK uint8) {
		c := int(capacity%32) + 1
		k := int(topK)%c + 1
		d, err := New(Config{Capacity: c, TopK: k, SampleRate: 1, MaxKeyBytes: 8})
		if err != nil {
			t.Fatal(err)
		}
		exact := map[string]uint64{}
		start := 0
		for i := 0; i <= len(data); i++ {
			if i == len(data) || data[i] == 0 {
				key := string(data[start:i])
				d.Observe(key, int64(i-start))
				if len(key) > 0 && len(key) <= 8 {
					exact[key]++
				}
				start = i + 1
			}
		}
		s := d.Rotate(time.Now())
		if len(s.Top) > k || s.Tracked > c {
			t.Fatalf("bounds: %+v", s)
		}
		for _, cand := range s.Top {
			if cand.Score < exact[cand.Key] || cand.Score-cand.Err > exact[cand.Key] {
				t.Fatalf("space-saving bound violated: %+v exact=%d", cand, exact[cand.Key])
			}
		}
	})
}

func BenchmarkObserveZipf(b *testing.B) {
	stream := zipfStream(1<<16, 1_000_000, 1.1, 7)
	d := mustNew(b, Config{Capacity: 1000, TopK: 100, SampleRate: 1})
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := rand.IntN(len(stream))
		for pb.Next() {
			d.Observe(stream[i&(len(stream)-1)], 100)
			i++
		}
	})
}

func BenchmarkObserveSampled(b *testing.B) {
	stream := zipfStream(1<<16, 1_000_000, 1.1, 7)
	d := mustNew(b, Config{Capacity: 1000, TopK: 100, SampleRate: 0.1})
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := rand.IntN(len(stream))
		for pb.Next() {
			d.Observe(stream[i&(len(stream)-1)], 100)
			i++
		}
	})
}

func BenchmarkRotate(b *testing.B) {
	d := mustNew(b, Config{Capacity: 1000, TopK: 100, SampleRate: 1})
	stream := zipfStream(1<<16, 1_000_000, 1.1, 7)
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		for _, k := range stream[:4096] {
			d.Observe(k, -1)
		}
		d.Rotate(time.Now())
	}
}
