// Package detector implements a bounded-memory streaming heavy-hitter
// detector used to find locally hot Redis keys inside an application process.
//
// The detector is a sharded Space-Saving summary (Metwally et al., 2005) with
// per-window exponential decay. Each shard owns a fixed-capacity min-heap of
// counters protected by its own mutex, so memory is bounded by the configured
// capacity regardless of key cardinality, and contention is spread across
// shards. See docs/adr/0001-hotkey-detection-algorithm.md for the rationale.
package detector

// entry is a single Space-Saving counter.
//
// count is the decayed Space-Saving estimate (an over-estimate by at most err).
// window is the number of observations recorded for this key during the
// current window since the key (re)entered the summary, so it is a lower
// bound on the true per-window frequency.
type entry struct {
	key    string
	count  uint64
	err    uint64
	window uint64
	size   int64 // last observed value size in bytes, -1 when unknown
}

// summary is a fixed-capacity Space-Saving structure. It is not safe for
// concurrent use; callers must hold the owning shard's lock.
type summary struct {
	capacity int
	heap     []entry          // min-heap ordered by count
	index    map[string]int32 // key -> position in heap
}

func newSummary(capacity int) *summary {
	if capacity < 1 {
		capacity = 1
	}
	return &summary{
		capacity: capacity,
		heap:     make([]entry, 0, capacity),
		index:    make(map[string]int32, capacity),
	}
}

// add records weight observations of key. size is the observed value size
// (or -1 when unknown).
func (s *summary) add(key string, weight uint64, size int64) {
	if i, ok := s.index[key]; ok {
		e := &s.heap[i]
		e.count += weight
		e.window += weight
		if size >= 0 {
			e.size = size
		}
		s.down(int(i))
		return
	}
	if len(s.heap) < s.capacity {
		s.heap = append(s.heap, entry{key: key, count: weight, window: weight, size: size})
		i := len(s.heap) - 1
		s.index[key] = int32(i)
		s.up(i)
		return
	}
	// Replace the minimum counter (Space-Saving eviction). The new key
	// inherits the evicted count as its error bound.
	root := &s.heap[0]
	delete(s.index, root.key)
	minCount := root.count
	*root = entry{key: key, count: minCount + weight, err: minCount, window: weight, size: size}
	s.index[key] = 0
	s.down(0)
}

// estimate returns the decayed count and error bound for key.
func (s *summary) estimate(key string) (count, err uint64, ok bool) {
	i, ok := s.index[key]
	if !ok {
		return 0, 0, false
	}
	e := s.heap[i]
	return e.count, e.err, true
}

// rotate appends all entries observed in the current window to dst, then
// decays every counter by the given right shift and drops counters that
// decayed to zero. It performs no allocation beyond growing dst.
func (s *summary) rotate(dst []Candidate, shift uint) []Candidate {
	for i := range s.heap {
		e := &s.heap[i]
		if e.window > 0 {
			dst = append(dst, Candidate{Key: e.key, Window: e.window, Score: e.count, Err: e.err, SizeBytes: e.size})
		}
		e.count >>= shift
		e.err >>= shift
		e.window = 0
	}
	// Compact zero-count entries in place.
	n := 0
	for i := range s.heap {
		if s.heap[i].count == 0 {
			delete(s.index, s.heap[i].key)
			continue
		}
		s.heap[n] = s.heap[i]
		n++
	}
	for i := n; i < len(s.heap); i++ {
		s.heap[i] = entry{} // release key strings
	}
	s.heap = s.heap[:n]
	// Decay is monotonic so heap order is preserved, but compaction moved
	// entries; rebuild positions and re-heapify (O(n)).
	for i := range s.heap {
		s.index[s.heap[i].key] = int32(i)
	}
	for i := n/2 - 1; i >= 0; i-- {
		s.down(i)
	}
	return dst
}

func (s *summary) less(i, j int) bool { return s.heap[i].count < s.heap[j].count }

func (s *summary) swap(i, j int) {
	s.heap[i], s.heap[j] = s.heap[j], s.heap[i]
	s.index[s.heap[i].key] = int32(i)
	s.index[s.heap[j].key] = int32(j)
}

func (s *summary) up(i int) {
	for i > 0 {
		p := (i - 1) / 2
		if !s.less(i, p) {
			return
		}
		s.swap(i, p)
		i = p
	}
}

func (s *summary) down(i int) {
	n := len(s.heap)
	for {
		l := 2*i + 1
		if l >= n {
			return
		}
		m := l
		if r := l + 1; r < n && s.less(r, l) {
			m = r
		}
		if !s.less(m, i) {
			return
		}
		s.swap(i, m)
		i = m
	}
}
