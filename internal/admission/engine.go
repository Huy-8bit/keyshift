package admission

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Huy-8bit/keyshift/internal/detector"
	"github.com/Huy-8bit/keyshift/internal/policy"
)

// EntryOverheadBytes approximates per-entry bookkeeping in the local cache in
// addition to key and value bytes. It is used for budget planning only.
const EntryOverheadBytes = 160

// rateAlpha is the EWMA weight of the newest window in rate estimates.
const rateAlpha = 0.5

// minSampleReads is the minimum number of reads in a window before an
// observed hit ratio is trusted for demotion decisions.
const minSampleReads = 20

// Config configures an Engine.
type Config struct {
	// Detector configures the read detector (top_k, capacity, sampling).
	Detector detector.Config
	// WriteCapacity bounds the write-frequency detector. Zero uses
	// Detector.Capacity.
	WriteCapacity int
	// MaxStaleness is the client-side cache staleness backstop. Each cached
	// entry is refetched at least this often, which bounds the best possible
	// hit ratio.
	MaxStaleness time.Duration
	// MaxReportKeys bounds the number of keys in a window report.
	MaxReportKeys int
	// Fingerprint computes key fingerprints for deny-by-fingerprint rules.
	// It is called only during window rotation, never on the request path.
	Fingerprint func(key string) string
	// OnDemote is called (outside the request path) for every key that
	// leaves the admitted set, so the adapter can drop its cached entry.
	OnDemote func(key string)
	// OnSuspend is called when caching is suspended (tracking unhealthy),
	// so the adapter can flush its local cache.
	OnSuspend func()
	// InstanceID selects the rollout bucket.
	InstanceID string
}

// KeyStats holds hot-path counters for an admitted key.
type KeyStats struct {
	reads         atomic.Uint64
	fills         atomic.Uint64
	invalidations atomic.Uint64
	size          atomic.Int64
}

// AddRead records a read served through the cache path.
func (k *KeyStats) AddRead(size int64) {
	k.reads.Add(1)
	// Hot keys are read concurrently from many goroutines: store only when
	// the size changed so that hits do not keep invalidating the cache line.
	if size >= 0 && k.size.Load() != size {
		k.size.Store(size)
	}
}

// admittedSet is an immutable snapshot of the admitted keys. The request
// path reads it with one atomic load; rotations publish a new one.
type admittedSet struct {
	m map[string]*KeyStats
}

var emptySet = &admittedSet{m: map[string]*KeyStats{}}

// record is the engine's per-key state, guarded by Engine.mu. stats is
// non-nil exactly while the key is admitted.
type record struct {
	key   string
	state State

	// last closed window measurements
	reads, writes, fills, invalidations uint64
	readRate, writeRate                 float64
	fillRate, invRate                   float64
	hitRatio                            float64
	hitObserved                         bool
	savedRate                           float64
	size                                int64

	hasRate       bool    // readRate/writeRate hold a smoothed estimate
	scoreRate     float64 // decayed detector estimate for non-admitted keys
	qualifying    int
	coldWindows   int
	strikes       int
	cooldownUntil time.Time
	pinned        bool
	reason        string
	seen          bool // appeared in this window
	stats         *KeyStats
}

// Counters are cumulative engine counters.
type Counters struct {
	Admissions     uint64
	Demotions      uint64
	Cooldowns      uint64
	BudgetRejected uint64
	Rotations      uint64
	Suspensions    uint64
}

// KeyReport is a per-key line in a window report.
type KeyReport struct {
	Key           string
	State         State
	Reason        string
	Reads         uint64
	Writes        uint64
	Fills         uint64
	Invalidations uint64
	SizeBytes     int64
	ReadRate      float64
	SavedRate     float64
	HitRatio      float64
}

// WindowReport summarizes a closed observation window.
type WindowReport struct {
	Start, End    time.Time
	Reads         uint64 // logical reads observed (cached + uncached)
	Recorded      uint64 // weighted reads recorded by the detector
	Dropped       uint64
	Writes        uint64
	CachedReads   uint64 // reads routed to the cache path
	Fills         uint64 // cache misses that went to Redis
	Invalidations uint64
	Admitted      int
	Candidates    int
	Cooldown      int
	Tracked       int
	Coverage      float64
	Keys          []KeyReport
	PolicyVersion int64
	PolicySource  string
}

// Engine is the adaptive admission engine.
type Engine struct {
	cfg    Config
	reads  *detector.Detector
	writes *detector.Detector

	pol      atomic.Pointer[policy.Effective]
	healthy  atomic.Bool
	admitted atomic.Pointer[admittedSet]
	inRoll   atomic.Bool // instance is inside the rollout of the active policy
	// unavailable, when set, explains why local caching can never be used
	// (unsupported driver/topology/permissions or disabled by configuration).
	unavailable atomic.Pointer[string]

	mu       sync.Mutex
	table    map[string]*record
	maxTable int
	last     WindowReport
	counters Counters
}

// New creates an engine with the given initial effective policy.
func New(cfg Config, eff *policy.Effective) (*Engine, error) {
	rd, err := detector.New(cfg.Detector)
	if err != nil {
		return nil, err
	}
	wc := cfg.WriteCapacity
	if wc == 0 {
		wc = cfg.Detector.Capacity
	}
	wd, err := detector.New(detector.Config{Capacity: wc, TopK: wc, SampleRate: 1})
	if err != nil {
		return nil, err
	}
	if cfg.MaxStaleness <= 0 {
		return nil, fmt.Errorf("admission: max_staleness must be > 0")
	}
	if cfg.MaxReportKeys == 0 {
		cfg.MaxReportKeys = 50
	}
	if eff == nil {
		eff = policy.Resolve(nil, nil)
	}
	e := &Engine{
		cfg:      cfg,
		reads:    rd,
		writes:   wd,
		table:    make(map[string]*record),
		maxTable: cfg.Detector.Capacity,
	}
	e.pol.Store(eff)
	e.admitted.Store(emptySet)
	e.inRoll.Store(true)
	return e, nil
}

// Lookup returns the stats handle when key is admitted, or nil. It is the
// only admission call on the cached read path: one atomic load and one map
// lookup.
func (e *Engine) Lookup(key string) *KeyStats {
	if !e.healthy.Load() {
		return nil
	}
	return e.admitted.Load().m[key]
}

// ObserveRead records a read of a key that was not served through the cache
// path.
func (e *Engine) ObserveRead(key string, size int64) { e.reads.Observe(key, size) }

// ObserveWrite records a write (SET/DEL) issued by this process.
func (e *Engine) ObserveWrite(key string) { e.writes.Observe(key, -1) }

// RecordFill records a cache miss for an admitted key.
func (e *Engine) RecordFill(key string) {
	if ks := e.admitted.Load().m[key]; ks != nil {
		ks.fills.Add(1)
	}
}

// RecordInvalidation records a server invalidation for a key.
func (e *Engine) RecordInvalidation(key string) {
	if ks := e.admitted.Load().m[key]; ks != nil {
		ks.invalidations.Add(1)
	}
}

// AdmittedCount returns the number of admitted keys.
func (e *Engine) AdmittedCount() int { return len(e.admitted.Load().m) }

// Policy returns the active effective policy.
func (e *Engine) Policy() *policy.Effective { return e.pol.Load() }

// Healthy reports whether caching is allowed by tracking health.
func (e *Engine) Healthy() bool { return e.healthy.Load() }

// SetUnavailable marks local caching as permanently unavailable with a
// reason shown by Explain. Detection and reporting continue.
func (e *Engine) SetUnavailable(reason string) { e.unavailable.Store(&reason) }

// SetHealthy updates tracking health. Becoming unhealthy clears the admitted
// set immediately so no further reads are served from the local cache.
func (e *Engine) SetHealthy(ok bool) {
	if e.healthy.Swap(ok) == ok {
		return
	}
	if ok {
		return // admission resumes at the next rotation
	}
	e.mu.Lock()
	prev := e.admitted.Swap(emptySet)
	for key := range prev.m {
		if r := e.table[key]; r != nil {
			r.state = Unhealthy
			r.reason = "client tracking is unhealthy; caching suspended"
			r.stats = nil
		}
	}
	e.counters.Suspensions++
	e.mu.Unlock()
	if e.cfg.OnSuspend != nil {
		e.cfg.OnSuspend()
	}
}

// SetPolicy installs a new effective policy and immediately removes admitted
// keys that the new policy no longer allows.
func (e *Engine) SetPolicy(eff *policy.Effective, inRollout bool) {
	e.mu.Lock()
	e.pol.Store(eff)
	e.inRoll.Store(inRollout)
	cur := e.admitted.Load()
	var demoted []string
	next := make(map[string]*KeyStats, len(cur.m))
	for key, ks := range cur.m {
		r := e.table[key]
		if st, reason, blocked := e.blocked(key, r, eff); blocked {
			demoted = append(demoted, key)
			if r != nil {
				r.state, r.reason, r.stats = st, reason, nil
			}
			continue
		}
		next[key] = ks
	}
	if len(demoted) > 0 {
		e.admitted.Store(&admittedSet{m: next})
		e.counters.Demotions += uint64(len(demoted))
	}
	e.mu.Unlock()
	e.demote(demoted)
}

func (e *Engine) demote(keys []string) {
	if e.cfg.OnDemote == nil {
		return
	}
	for _, k := range keys {
		e.cfg.OnDemote(k)
	}
}

// Explain describes why key is (or is not) cached.
func (e *Engine) Explain(key string) Decision {
	eff := e.pol.Load()
	e.mu.Lock()
	defer e.mu.Unlock()
	d := Decision{Key: key, PolicyVersion: eff.Version, PolicySource: eff.Source, SizeBytes: -1}
	r := e.table[key]
	if r == nil {
		if st, reason, blocked := e.blocked(key, nil, eff); blocked {
			d.State, d.Reason = st.String(), reason
		} else {
			d.State, d.Reason = Cold.String(), "not among the locally tracked hot-key candidates"
		}
		d.Pinned = eff.Pin.Match(key)
		return d
	}
	d.State, d.Reason = r.state.String(), r.reason
	d.ReadRate, d.WriteRate, d.FillRate, d.InvalidationRate = r.readRate, r.writeRate, r.fillRate, r.invRate
	d.HitRatio, d.SavedRate, d.SizeBytes = r.hitRatio, r.savedRate, r.size
	d.HitRatioKind = "predicted"
	if r.hitObserved {
		d.HitRatioKind = "observed"
	}
	d.Qualifying, d.Strikes, d.Pinned = r.qualifying, r.strikes, r.pinned
	if r.state == Cooldown {
		d.CooldownUntil = r.cooldownUntil.UTC().Format(time.RFC3339)
	}
	return d
}

// LastReport returns the most recent window report.
func (e *Engine) LastReport() WindowReport {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.last
}

// Counters returns cumulative counters.
func (e *Engine) Counters() Counters {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.counters
}

// TableSize returns the number of tracked records (bounded by capacity).
func (e *Engine) TableSize() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.table)
}
