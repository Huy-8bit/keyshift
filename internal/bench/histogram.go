// Package bench implements KeyShift's reproducible benchmark and
// fault-injection harness ("KeyShift Lab"). It compares:
//
//   - none:     the application's Redis client with no local cache;
//   - csc:      conventional client-side caching (every cacheable read is
//     cached through the client library's tracking-backed cache);
//   - keyshift: KeyShift adaptive admission on top of the same cache.
//
// Every mode runs the same workload against the same Redis server, and
// results include both benefits (Redis load avoided, latency) and costs
// (memory, CPU, allocations, invalidations, server tracking state, stale
// reads).
package bench

import (
	"math"
	"math/bits"
	"time"
)

// histogram is a log-linear latency histogram with ~3% relative precision.
// It is not safe for concurrent use; each worker owns one and they are
// merged at the end.
type histogram struct {
	counts [64 * 32]uint64
	n      uint64
	max    uint64
}

const subBits = 5

func bucketOf(v uint64) int {
	if v < 1<<subBits {
		return int(v)
	}
	e := bits.Len64(v) - 1 - subBits
	return (e+1)<<subBits | int((v>>uint(e))&(1<<subBits-1))
}

func bucketLow(b int) uint64 {
	if b < 1<<subBits {
		return uint64(b)
	}
	e := b>>subBits - 1
	return (uint64(1<<subBits) | uint64(b&(1<<subBits-1))) << uint(e)
}

func (h *histogram) record(d time.Duration) {
	v := uint64(max(d, 0))
	h.counts[bucketOf(v)]++
	h.n++
	h.max = max(h.max, v)
}

func (h *histogram) merge(o *histogram) {
	for i := range h.counts {
		h.counts[i] += o.counts[i]
	}
	h.n += o.n
	h.max = max(h.max, o.max)
}

// quantile returns the q-quantile in nanoseconds (bucket lower bound).
func (h *histogram) quantile(q float64) float64 {
	if h.n == 0 {
		return 0
	}
	target := uint64(math.Ceil(q * float64(h.n)))
	var seen uint64
	for i, c := range h.counts {
		seen += c
		if seen >= target {
			return float64(bucketLow(i))
		}
	}
	return float64(h.max)
}
