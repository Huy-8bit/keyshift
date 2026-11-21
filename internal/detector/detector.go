package detector

import (
	"errors"
	"hash/maphash"
	"math"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// Config configures a Detector.
type Config struct {
	// Capacity is the total number of counters across all shards
	// (max_tracking_candidates). Memory is O(Capacity).
	Capacity int
	// TopK is the number of candidates returned per window.
	TopK int
	// SampleRate in (0, 1] is the fraction of observations recorded. Sampled
	// observations are weighted by 1/SampleRate.
	SampleRate float64
	// Shards is the number of independently locked shards. It is rounded up
	// to a power of two. Zero selects a default based on Capacity.
	Shards int
	// MaxKeyBytes is the longest key that is tracked. Longer keys are counted
	// as dropped observations. Zero selects 1024.
	MaxKeyBytes int
	// DecayShift is the right shift applied to every counter at rotation
	// (1 = halve). Zero selects 1.
	DecayShift uint
}

// Candidate is a key reported by the detector for a window.
type Candidate struct {
	Key string
	// Window is the number of (weighted) observations of Key during the
	// window. It is a lower bound for the true count within the window.
	Window uint64
	// Score is the decayed Space-Saving count, an over-estimate by at most Err.
	Score uint64
	Err   uint64
	// SizeBytes is the most recently observed value size, or -1 if unknown.
	SizeBytes int64
}

// Snapshot is the result of closing an observation window.
type Snapshot struct {
	Start, End time.Time
	// Observed counts every Observe call, including unsampled and dropped ones.
	Observed uint64
	// Recorded counts weighted observations that entered the summary.
	Recorded uint64
	// Dropped counts observations rejected because of key length limits.
	Dropped uint64
	// Top holds up to TopK candidates ordered by Window descending.
	Top []Candidate
	// Tracked is the number of counters currently held.
	Tracked int
}

// Coverage reports the fraction of recorded observations attributed to the
// returned top candidates. Low coverage means the workload is spread across
// many keys and local caching is unlikely to pay off.
func (s Snapshot) Coverage() float64 {
	if s.Recorded == 0 {
		return 0
	}
	var sum uint64
	for _, c := range s.Top {
		sum += c.Window
	}
	return math.Min(1, float64(sum)/float64(s.Recorded))
}

type shard struct {
	mu       sync.Mutex
	sum      *summary
	recorded uint64   // guarded by mu
	_        [32]byte // reduce false sharing between adjacent shard locks
}

// stripe is a cache-line padded counter used to avoid a single contended
// atomic on the hot path.
type stripe struct {
	n atomic.Uint64
	_ [56]byte
}

const numStripes = 16

// Detector is a concurrent, bounded heavy-hitter detector. All methods are
// safe for concurrent use.
type Detector struct {
	cfg       Config
	seed      maphash.Seed
	shards    []shard
	mask      uint64
	threshold uint64 // sample when rand < threshold; MaxUint64 means always
	weight    uint64

	observed [numStripes]stripe
	dropped  atomic.Uint64

	rotMu       sync.Mutex
	windowStart time.Time
	scratch     []Candidate
}

// Validate reports configuration errors.
func (c Config) Validate() error {
	if c.Capacity < 1 {
		return errors.New("detector: capacity must be >= 1")
	}
	if c.TopK < 1 {
		return errors.New("detector: top_k must be >= 1")
	}
	if c.TopK > c.Capacity {
		return errors.New("detector: top_k must not exceed capacity")
	}
	if !(c.SampleRate > 0 && c.SampleRate <= 1) {
		return errors.New("detector: sample_rate must be in (0, 1]")
	}
	if c.Shards < 0 || c.MaxKeyBytes < 0 {
		return errors.New("detector: negative shards or max_key_bytes")
	}
	if c.DecayShift > 16 {
		return errors.New("detector: decay_shift must be <= 16")
	}
	return nil
}

// New creates a detector.
func New(cfg Config) (*Detector, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if cfg.MaxKeyBytes == 0 {
		cfg.MaxKeyBytes = 1024
	}
	if cfg.DecayShift == 0 {
		cfg.DecayShift = 1
	}
	n := cfg.Shards
	if n == 0 {
		// Aim for ~64 counters per shard, between 1 and 64 shards.
		n = max(1, min(64, cfg.Capacity/64))
	}
	n = nextPow2(n)
	if n > cfg.Capacity {
		n = prevPow2(cfg.Capacity)
	}
	d := &Detector{
		cfg:         cfg,
		seed:        maphash.MakeSeed(),
		shards:      make([]shard, n),
		mask:        uint64(n - 1),
		weight:      uint64(math.Round(1 / cfg.SampleRate)),
		windowStart: time.Now(),
	}
	if cfg.SampleRate >= 1 {
		d.threshold = math.MaxUint64
	} else {
		d.threshold = uint64(cfg.SampleRate * float64(math.MaxUint64))
	}
	per := cfg.Capacity / n
	extra := cfg.Capacity % n
	for i := range d.shards {
		c := per
		if i < extra {
			c++
		}
		d.shards[i].sum = newSummary(c)
	}
	return d, nil
}

// Config returns the effective configuration.
func (d *Detector) Config() Config { return d.cfg }

// Observe records one access to key with an optional value size (-1 if
// unknown). It never blocks on anything other than a short shard lock.
func (d *Detector) Observe(key string, size int64) {
	r := rand.Uint64()
	d.observed[r&(numStripes-1)].n.Add(1)
	if d.threshold != math.MaxUint64 && r >= d.threshold {
		return
	}
	d.add(key, d.weight, size)
}

// Add records n exact observations of key, bypassing sampling. It is used to
// fold counts gathered on other fast paths back into the summary.
func (d *Detector) Add(key string, n uint64, size int64) {
	if n == 0 {
		return
	}
	d.observed[rand.Uint64()&(numStripes-1)].n.Add(n)
	d.add(key, n, size)
}

func (d *Detector) add(key string, w uint64, size int64) {
	if len(key) == 0 || len(key) > d.cfg.MaxKeyBytes {
		d.dropped.Add(w)
		return
	}
	sh := &d.shards[maphash.String(d.seed, key)&d.mask]
	sh.mu.Lock()
	sh.recorded += w
	sh.sum.add(key, w, size)
	sh.mu.Unlock()
}

// Estimate returns the decayed count of key and its error bound.
func (d *Detector) Estimate(key string) (count, errBound uint64, ok bool) {
	sh := &d.shards[maphash.String(d.seed, key)&d.mask]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	return sh.sum.estimate(key)
}

// Rotate closes the current window and returns its snapshot. Counters are
// decayed in place, so rotation does not allocate proportionally to the
// key space. Rotate is safe to call concurrently with Observe.
func (d *Detector) Rotate(now time.Time) Snapshot { return d.RotateK(now, d.cfg.TopK) }

// RotateK is Rotate returning up to k candidates instead of TopK.
func (d *Detector) RotateK(now time.Time, k int) Snapshot {
	d.rotMu.Lock()
	defer d.rotMu.Unlock()

	all := d.scratch[:0]
	tracked := 0
	var recorded, observed uint64
	for i := range d.shards {
		sh := &d.shards[i]
		sh.mu.Lock()
		all = sh.sum.rotate(all, d.cfg.DecayShift)
		tracked += len(sh.sum.heap)
		recorded += sh.recorded
		sh.recorded = 0
		sh.mu.Unlock()
	}
	for i := range d.observed {
		observed += d.observed[i].n.Swap(0)
	}
	slices.SortFunc(all, func(a, b Candidate) int {
		switch {
		case a.Window > b.Window:
			return -1
		case a.Window < b.Window:
			return 1
		case a.Score > b.Score:
			return -1
		case a.Score < b.Score:
			return 1
		default:
			return 0
		}
	})
	k = min(max(k, 0), len(all))
	top := make([]Candidate, k)
	copy(top, all[:k])
	clear(all) // drop key references held by scratch
	d.scratch = all[:0]

	snap := Snapshot{
		Start:    d.windowStart,
		End:      now,
		Observed: observed,
		Recorded: recorded,
		Dropped:  d.dropped.Swap(0),
		Top:      top,
		Tracked:  tracked,
	}
	d.windowStart = now
	return snap
}

func nextPow2(n int) int {
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}

func prevPow2(n int) int {
	p := 1
	for p*2 <= n {
		p <<= 1
	}
	return p
}
