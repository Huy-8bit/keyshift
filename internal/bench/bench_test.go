package bench

import (
	"math/rand/v2"
	"slices"
	"testing"
	"time"
)

func TestHistogramQuantiles(t *testing.T) {
	var h histogram
	var all []time.Duration
	r := rand.New(rand.NewPCG(1, 2))
	for range 100_000 {
		d := time.Duration(r.ExpFloat64() * float64(200*time.Microsecond))
		h.record(d)
		all = append(all, d)
	}
	slices.Sort(all)
	for _, q := range []float64{0.5, 0.9, 0.99, 0.999} {
		exact := float64(all[int(q*float64(len(all)))-1])
		got := h.quantile(q)
		if got < exact*0.95 || got > exact*1.01 {
			t.Errorf("q%.3f: got %.0f exact %.0f", q, got, exact)
		}
	}
	var h2 histogram
	h2.record(time.Second)
	h.merge(&h2)
	if h.n != 100_001 || h.max != uint64(time.Second) {
		t.Fatal("merge")
	}
}

func TestBucketRoundTrip(t *testing.T) {
	for _, v := range []uint64{0, 1, 31, 32, 33, 1000, 1 << 20, 1<<40 + 12345} {
		b := bucketOf(v)
		lo := bucketLow(b)
		if lo > v || (v > 64 && float64(v-lo)/float64(v) > 0.04) {
			t.Fatalf("v=%d bucket=%d low=%d", v, b, lo)
		}
	}
}

func TestPickerDistributions(t *testing.T) {
	perm := make([]int32, 1000)
	for i := range perm {
		perm[i] = int32(i)
	}
	now := time.Now()
	for _, s := range Scenarios() {
		if s.Keys > 1000 {
			s.Keys = 1000
		}
		p := newPicker(s, 1, 2, perm, now)
		for range 1000 {
			if k := p.next(now); k < 0 || k >= s.Keys {
				t.Fatalf("%s: key %d out of range", s.Name, k)
			}
			if k := p.nextWrite(now); k < 0 || k >= s.Keys {
				t.Fatalf("%s: write key %d out of range", s.Name, k)
			}
		}
	}
	s := Scenario{Dist: SingleHot, Keys: 1000}
	p := newPicker(s, 1, 0, perm, now)
	zero := 0
	for range 10_000 {
		if p.next(now) == 0 {
			zero++
		}
	}
	if zero < 4500 {
		t.Fatalf("single-hot key chosen %d/10000", zero)
	}
}

func TestValueVersionRoundTrip(t *testing.T) {
	if versionOf(value(42, 128)) != 42 || len(value(1, 4)) != 8 || versionOf([]byte("x")) != 0 {
		t.Fatal("value encoding")
	}
}

func TestParseStat(t *testing.T) {
	if parseStat("calls=123,usec=4,usec_per_call=0.1", "calls") != 123 {
		t.Fatal("parse")
	}
}
