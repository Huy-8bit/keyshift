package admission

import (
	"fmt"
	"math"
	"slices"
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
	if size >= 0 {
		k.size.Store(size)
	}
}

type admittedSet struct {
	m map[string]*KeyStats
}

var emptySet = &admittedSet{m: map[string]*KeyStats{}}

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

// blocked reports whether policy or limits prevent caching key at all.
func (e *Engine) blocked(key string, r *record, eff *policy.Effective) (State, string, bool) {
	if why := e.unavailable.Load(); why != nil {
		return Disabled, "local caching unavailable: " + *why, true
	}
	if !e.healthy.Load() {
		return Unhealthy, "client tracking is unhealthy; caching suspended", true
	}
	if eff.Mode == policy.ModeOff {
		return Disabled, "caching disabled by policy (mode=off)", true
	}
	if !e.inRoll.Load() {
		return Disabled, "instance outside policy rollout", true
	}
	if eff.Deny.Match(key) {
		return Bypassed, "denied by policy pattern", true
	}
	if eff.HasDenyF && e.cfg.Fingerprint != nil {
		if _, ok := eff.DenyFP[e.cfg.Fingerprint(key)]; ok {
			return Bypassed, "denied by policy fingerprint", true
		}
	}
	if r != nil && r.size > eff.Admission.MaxEntryBytes {
		return Bypassed, fmt.Sprintf("value size %d exceeds max_entry_bytes %d", r.size, eff.Admission.MaxEntryBytes), true
	}
	return 0, "", false
}

// Rotate closes the current window, re-evaluates every tracked key, and
// publishes a new admitted set. It must not be called concurrently with
// itself; it is safe to call concurrently with the request path.
func (e *Engine) Rotate(now time.Time) WindowReport {
	e.mu.Lock()
	eff := e.pol.Load()
	cur := e.admitted.Load()

	// 1. Harvest cache-path counters for admitted keys and fold their reads
	// into the detector so admitted keys are ranked with everything else.
	var cachedReads, fills, invs uint64
	for key, ks := range cur.m {
		r := e.table[key]
		if r == nil { // should not happen; keep the engine consistent anyway
			r = &record{key: key, state: Admitted, size: -1}
			e.table[key] = r
		}
		r.reads = ks.reads.Swap(0)
		r.fills = ks.fills.Swap(0)
		r.invalidations = ks.invalidations.Swap(0)
		if sz := ks.size.Load(); sz >= 0 {
			r.size = sz
		}
		cachedReads += r.reads
		fills += r.fills
		invs += r.invalidations
		r.seen = true
		e.reads.Add(key, r.reads, r.size)
	}

	// Ask for TopK candidates beyond the keys that are already admitted, so
	// the admitted set can grow to the cache budget instead of being capped
	// at TopK by its own incumbents.
	rs := e.reads.RotateK(now, e.cfg.Detector.TopK+len(cur.m))
	ws := e.writes.Rotate(now)
	secs := now.Sub(rs.Start).Seconds()
	if secs <= 0 {
		secs = 1e-3
	}
	writeCount := make(map[string]uint64, len(ws.Top))
	var totalWrites uint64
	for _, c := range ws.Top {
		writeCount[c.Key] = c.Window
	}
	totalWrites = ws.Recorded

	// 2. Merge detector candidates into the table (bounded).
	for _, c := range rs.Top {
		r := e.table[c.Key]
		if r == nil {
			if len(e.table) >= e.maxTable {
				continue
			}
			r = &record{key: c.Key, state: Cold, size: -1}
			e.table[c.Key] = r
		}
		if r.stats == nil { // not admitted: detector count is the read count
			r.reads = c.Window
			r.fills, r.invalidations = 0, 0
			// The detector's decayed count is a long-memory estimate (with
			// halving per window it converges to twice the per-window
			// count). Its lower bound (Score-Err) is used as the read rate
			// of non-admitted keys so admission is not driven by a few
			// lucky windows.
			r.scoreRate = float64(c.Score-min(c.Err, c.Score)) / float64(uint64(1)<<e.reads.Config().DecayShift) / secs
		}
		if c.SizeBytes >= 0 {
			r.size = c.SizeBytes
		}
		r.seen = true
	}

	// 3. Evaluate every record.
	stalenessFloor := 1 / e.cfg.MaxStaleness.Seconds()
	var eligible []*record
	for key, r := range e.table {
		if !r.seen {
			r.reads, r.fills, r.invalidations = 0, 0, 0
		}
		r.writes = writeCount[key]
		// Smooth read and write rates across windows (EWMA, alpha 0.5).
		// Per-window counts of moderately hot keys are noisy; deciding on
		// raw counts makes keys near a threshold flip in and out.
		rr, wr := float64(r.reads)/secs, float64(r.writes)/secs
		if r.stats == nil {
			if !r.seen {
				r.scoreRate = 0
			}
			// Conservative: the smaller of the window rate and the decayed
			// estimate must clear the thresholds.
			rr = min(rr, r.scoreRate)
		}
		if r.hasRate {
			r.readRate = rateAlpha*rr + (1-rateAlpha)*r.readRate
			r.writeRate = rateAlpha*wr + (1-rateAlpha)*r.writeRate
		} else {
			r.readRate, r.writeRate, r.hasRate = rr, wr, true
		}
		r.fillRate = float64(r.fills) / secs
		r.invRate = float64(r.invalidations) / secs
		e.evaluate(r, eff, now, stalenessFloor)
		if r.state == Admitted {
			eligible = append(eligible, r)
		}
	}

	// 4. Enforce the budget: pinned keys first, then by benefit density with
	// a stability bonus for incumbents.
	incumbent := func(r *record) bool { return r.stats != nil }
	density := func(r *record) float64 {
		d := r.savedRate / float64(max(r.size, 0)+int64(len(r.key))+EntryOverheadBytes)
		if incumbent(r) {
			d *= 1.25
		}
		return d
	}
	slices.SortFunc(eligible, func(a, b *record) int {
		if a.pinned != b.pinned {
			if a.pinned {
				return -1
			}
			return 1
		}
		da, db := density(a), density(b)
		switch {
		case da > db:
			return -1
		case da < db:
			return 1
		}
		if a.key < b.key {
			return -1
		}
		return 1
	})
	next := make(map[string]*KeyStats, len(eligible))
	var usedBytes int64
	for _, r := range eligible {
		cost := max(r.size, 0) + int64(len(r.key)) + EntryOverheadBytes
		if len(next) >= eff.Budget.MaxEntries || usedBytes+cost > eff.Budget.MaxMemoryBytes {
			r.state = Candidate
			r.reason = "qualifies but the cache budget is exhausted"
			e.counters.BudgetRejected++
			continue
		}
		usedBytes += cost
		if r.stats == nil {
			r.stats = &KeyStats{}
			r.stats.size.Store(r.size)
			e.counters.Admissions++
		}
		next[r.key] = r.stats
	}

	// 5. Detach stats from records that left the admitted set.
	var demoted []string
	for key := range cur.m {
		if _, ok := next[key]; !ok {
			demoted = append(demoted, key)
			if r := e.table[key]; r != nil {
				r.stats = nil
			}
			e.counters.Demotions++
		}
	}
	// If health flipped to unhealthy during evaluation, SetHealthy is
	// waiting on e.mu and will clear this set as soon as we release it;
	// Lookup already refuses to serve while unhealthy.
	e.admitted.Store(&admittedSet{m: next})

	// 6. Build the report and prune cold, unseen records.
	rep := WindowReport{
		Start: rs.Start, End: now,
		Reads: rs.Observed, Recorded: rs.Recorded, Dropped: rs.Dropped,
		Writes:      totalWrites,
		CachedReads: cachedReads, Fills: fills, Invalidations: invs,
		Admitted: len(next), Tracked: rs.Tracked, Coverage: rs.Coverage(),
		PolicyVersion: eff.Version, PolicySource: eff.Source,
	}
	keys := make([]KeyReport, 0, len(e.table))
	for key, r := range e.table {
		switch r.state {
		case Candidate:
			rep.Candidates++
		case Cooldown:
			rep.Cooldown++
		}
		if r.seen || r.state != Cold {
			keys = append(keys, KeyReport{
				Key: key, State: r.state, Reason: r.reason,
				Reads: r.reads, Writes: r.writes, Fills: r.fills, Invalidations: r.invalidations,
				SizeBytes: r.size, ReadRate: r.readRate, SavedRate: r.savedRate, HitRatio: r.hitRatio,
			})
		}
		if !r.seen && r.state == Cold {
			delete(e.table, key)
		}
		r.seen = false
	}
	slices.SortFunc(keys, func(a, b KeyReport) int {
		switch {
		case a.Reads > b.Reads:
			return -1
		case a.Reads < b.Reads:
			return 1
		case a.Key < b.Key:
			return -1
		default:
			return 1
		}
	})
	if len(keys) > e.cfg.MaxReportKeys {
		keys = keys[:e.cfg.MaxReportKeys]
	}
	rep.Keys = keys
	e.pruneLocked()
	e.counters.Rotations++
	e.last = rep
	e.mu.Unlock()

	e.demote(demoted)
	return rep
}

// pruneLocked bounds the table when cooldown/bypassed records accumulate.
func (e *Engine) pruneLocked() {
	if len(e.table) <= e.maxTable {
		return
	}
	var victims []*record
	for _, r := range e.table {
		if r.stats == nil {
			victims = append(victims, r)
		}
	}
	slices.SortFunc(victims, func(a, b *record) int {
		switch {
		case a.reads < b.reads:
			return -1
		case a.reads > b.reads:
			return 1
		}
		return 0
	})
	for _, r := range victims {
		if len(e.table) <= e.maxTable {
			return
		}
		delete(e.table, r.key)
	}
}

// evaluate applies the state machine to one record.
func (e *Engine) evaluate(r *record, eff *policy.Effective, now time.Time, stalenessFloor float64) {
	a := eff.Admission
	r.pinned = eff.Pin.Match(r.key)

	// Observed (admitted) vs predicted hit ratio.
	if r.stats != nil && r.reads > 0 {
		r.hitObserved = true
		r.hitRatio = 1 - math.Min(1, float64(r.fills)/float64(r.reads))
		r.savedRate = r.readRate * r.hitRatio
	} else {
		r.hitObserved = false
		predictedFill := math.Min(r.readRate, r.writeRate+stalenessFloor)
		if r.readRate > 0 {
			r.hitRatio = 1 - predictedFill/r.readRate
		} else {
			r.hitRatio = 0
		}
		r.savedRate = math.Max(0, r.readRate-predictedFill)
	}

	if st, reason, blocked := e.blocked(r.key, r, eff); blocked {
		if st == Unhealthy && r.state == Admitted {
			r.qualifying = a.MinObservationWindows // allow prompt recovery
		}
		r.state, r.reason = st, reason
		return
	}

	if r.state == Cooldown {
		if now.Before(r.cooldownUntil) {
			r.reason = fmt.Sprintf("cooling down after ineffective caching (strike %d)", r.strikes)
			return
		}
		r.state, r.qualifying = Cold, 0
	}

	if r.pinned {
		r.state, r.reason = Admitted, "pinned by policy"
		return
	}
	if eff.Mode == policy.ModeExplicit {
		r.state, r.reason, r.qualifying = Cold, "explicit mode: key does not match a pin pattern", 0
		return
	}

	if r.stats != nil { // currently admitted
		switch {
		case r.reads >= minSampleReads && r.hitRatio < a.ExitHitRatio:
			r.strikes++
			cool := time.Duration(a.CooldownSeconds) * time.Second << min(r.strikes-1, 4)
			r.cooldownUntil = now.Add(cool)
			r.state, r.qualifying, r.coldWindows = Cooldown, 0, 0
			r.reason = fmt.Sprintf("observed hit ratio %.2f below exit threshold %.2f (fills %d / reads %d, invalidations %d); cooldown %s",
				r.hitRatio, a.ExitHitRatio, r.fills, r.reads, r.invalidations, cool)
			e.counters.Cooldowns++
		case r.readRate < a.MinReadRate/2:
			r.coldWindows++
			if r.coldWindows >= a.ExitWindows {
				r.state, r.qualifying, r.coldWindows = Cold, 0, 0
				r.reason = fmt.Sprintf("read rate %.2f/s stayed below exit threshold %.2f/s for %d windows", r.readRate, a.MinReadRate/2, a.ExitWindows)
				return
			}
			r.state = Admitted
			r.reason = fmt.Sprintf("admitted; read rate low for %d/%d windows", r.coldWindows, a.ExitWindows)
		default:
			r.coldWindows = 0
			r.state = Admitted
			r.reason = fmt.Sprintf("admitted; observed hit ratio %.2f saves %.1f Redis reads/s", r.hitRatio, r.savedRate)
		}
		return
	}

	// Cold, Candidate, Unhealthy (recovering), Bypassed/Disabled (policy relaxed).
	switch {
	case r.readRate < a.MinReadRate:
		r.state, r.qualifying = Cold, 0
		r.reason = fmt.Sprintf("read rate %.2f/s below min_read_rate %.2f/s", r.readRate, a.MinReadRate)
	case r.hitRatio < a.MinHitRatio:
		r.state, r.qualifying = Cold, 0
		r.reason = fmt.Sprintf("predicted hit ratio %.2f below min_hit_ratio %.2f (write rate %.2f/s)", r.hitRatio, a.MinHitRatio, r.writeRate)
	default:
		r.qualifying++
		if r.qualifying >= a.MinObservationWindows {
			r.state = Admitted
			r.reason = fmt.Sprintf("admitted; predicted hit ratio %.2f saves %.1f Redis reads/s", r.hitRatio, r.savedRate)
		} else {
			r.state = Candidate
			r.reason = fmt.Sprintf("qualifying window %d/%d", r.qualifying, a.MinObservationWindows)
		}
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
