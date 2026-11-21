package keyshift

import (
	"runtime"
	"slices"
	"sync"
	"time"

	"github.com/Huy-8bit/keyshift/internal/admission"
	"github.com/Huy-8bit/keyshift/internal/rediscompat"
	"github.com/Huy-8bit/keyshift/internal/telemetry"
	"github.com/Huy-8bit/keyshift/internal/tracking"
)

// Capabilities reports what the Redis backend supports at runtime.
type Capabilities = rediscompat.Capabilities

// TrackingStatus reports client tracking health.
type TrackingStatus = tracking.Status

// Stats is a snapshot of cumulative client counters and current residency.
//
// The read counters are distinct quantities: Reads counts logical reads by
// the application; CachedReads were routed to the local cache path, of
// which LocalHits were served without contacting Redis; RedisReads reached
// Redis (direct reads, bypasses, and cache fills).
type Stats struct {
	Reads          uint64 `json:"reads"`
	CachedReads    uint64 `json:"cached_reads"`
	DirectReads    uint64 `json:"direct_reads"`
	Bypasses       uint64 `json:"bypasses"`
	LocalHits      uint64 `json:"local_hits"`
	LocalMisses    uint64 `json:"local_misses"`
	RedisReads     uint64 `json:"redis_reads"`
	Writes         uint64 `json:"writes"`
	Deletes        uint64 `json:"deletes"`
	Errors         uint64 `json:"errors"`
	CacheFallbacks uint64 `json:"cache_fallbacks"`

	// RedisReadsAvoided equals LocalHits: each local hit is a GET that did
	// not reach Redis.
	RedisReadsAvoided uint64 `json:"redis_reads_avoided"`

	Invalidations uint64 `json:"invalidations"`
	Evictions     uint64 `json:"evictions"`
	Entries       int64  `json:"entries"`
	MemoryBytes   int64  `json:"memory_bytes"`

	AdmittedKeys     int    `json:"admitted_keys"`
	Admissions       uint64 `json:"admissions"`
	Demotions        uint64 `json:"demotions"`
	Cooldowns        uint64 `json:"cooldowns"`
	BudgetRejections uint64 `json:"budget_rejections"`
	Suspensions      uint64 `json:"suspensions"`

	PolicyVersion  int64  `json:"policy_version"`
	PolicySource   string `json:"policy_source"`
	PolicyMode     string `json:"policy_mode"`
	PolicyRejected uint64 `json:"policy_rejected"`

	Tracking     TrackingStatus `json:"tracking"`
	Capabilities Capabilities   `json:"capabilities"`

	TelemetrySent    uint64 `json:"telemetry_sent"`
	TelemetryFailed  uint64 `json:"telemetry_failed"`
	TelemetryDropped uint64 `json:"telemetry_dropped"`

	LastWindow WindowSummary `json:"last_window"`
}

// WindowSummary summarizes the most recent observation window.
type WindowSummary struct {
	Start      time.Time `json:"start"`
	End        time.Time `json:"end"`
	Reads      uint64    `json:"reads"`
	Candidates int       `json:"candidates"`
	Cooldown   int       `json:"cooldown"`
	Tracked    int       `json:"tracked"`
	Coverage   float64   `json:"coverage"`
	Dropped    uint64    `json:"dropped"`
}

// Stats returns a snapshot of client statistics.
func (c *Client) Stats() Stats {
	cs := c.backend.CacheStats()
	ec := c.engine.Counters()
	eff := c.engine.Policy()
	s := Stats{
		Reads: c.reads.Load(), CachedReads: c.cachedReads.Load(), DirectReads: c.directReads.Load(),
		Bypasses: c.bypasses.Load(), LocalHits: cs.Hits, LocalMisses: cs.Misses,
		Writes: c.writes.Load(), Deletes: c.deletes.Load(), Errors: c.errorsTotal.Load(), CacheFallbacks: c.fallbacks.Load(),
		Invalidations: cs.Invalidations, Evictions: cs.Evictions, Entries: cs.Entries, MemoryBytes: cs.MemoryBytes,
		AdmittedKeys: c.engine.AdmittedCount(), Admissions: ec.Admissions, Demotions: ec.Demotions,
		Cooldowns: ec.Cooldowns, BudgetRejections: ec.BudgetRejected, Suspensions: ec.Suspensions,
		PolicyVersion: eff.Version, PolicySource: eff.Source, PolicyMode: string(eff.Mode), PolicyRejected: c.policyErrors.Load(),
		Tracking: c.monitor.Status(), Capabilities: c.backend.Capabilities(),
	}
	s.RedisReads = s.DirectReads + s.Bypasses + cs.Misses
	s.RedisReadsAvoided = cs.Hits
	if c.reporter != nil {
		rs := c.reporter.Stats()
		s.TelemetrySent, s.TelemetryFailed = rs.Sent, rs.Failed
		s.TelemetryDropped = rs.Dropped + rs.Rejected
	}
	rep := c.engine.LastReport()
	s.LastWindow = WindowSummary{Start: rep.Start, End: rep.End, Reads: rep.Reads, Candidates: rep.Candidates,
		Cooldown: rep.Cooldown, Tracked: rep.Tracked, Coverage: rep.Coverage, Dropped: rep.Dropped}
	return s
}

// telemetryAgg accumulates window reports between telemetry uploads. Its
// size is bounded by maxKeys*4 entries.
type telemetryAgg struct {
	mu      sync.Mutex
	maxKeys int
	start   time.Time
	end     time.Time
	totals  telemetry.Totals
	keys    map[string]*telemetry.Candidate
	det     telemetry.Detector
	// Counter snapshots taken at window rotation, so every total in a batch
	// covers exactly the same closed windows.
	curCS   rediscompat.CacheStats
	curEC   admission.Counters
	curErr  uint64
	curByp  uint64
	prevCS  rediscompat.CacheStats
	prevEC  admission.Counters
	prevErr uint64
	prevByp uint64
}

func newTelemetryAgg(maxKeys int) *telemetryAgg {
	return &telemetryAgg{maxKeys: maxKeys, keys: map[string]*telemetry.Candidate{}}
}

func (a *telemetryAgg) add(rep admission.WindowReport, c *Client) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.start.IsZero() {
		a.start = rep.Start
	}
	a.end = rep.End
	a.curCS, a.curEC = c.backend.CacheStats(), c.engine.Counters()
	a.curErr, a.curByp = c.errorsTotal.Load(), c.bypasses.Load()
	a.totals.Reads += rep.Reads
	a.totals.CachedReads += rep.CachedReads
	a.totals.Writes += rep.Writes
	a.det.Observed += rep.Reads
	a.det.Recorded += rep.Recorded
	a.det.Dropped += rep.Dropped
	a.det.Tracked = rep.Tracked
	a.det.Coverage = rep.Coverage
	for _, k := range rep.Keys {
		fp := c.hasher.Fingerprint(k.Key)
		cand := a.keys[fp]
		if cand == nil {
			if len(a.keys) >= a.maxKeys*4 {
				continue
			}
			cand = &telemetry.Candidate{FP: fp}
			if c.display.Match(k.Key) && len(k.Key) <= telemetry.MaxDisplayLen {
				cand.Display = k.Key
			}
			a.keys[fp] = cand
		}
		cand.Reads += k.Reads
		cand.Writes += k.Writes
		cand.Fills += k.Fills
		cand.Invalidations += k.Invalidations
		cand.SizeBytes = k.SizeBytes
		cand.State = k.State.String()
		cand.Reason = truncate(k.Reason, telemetry.MaxReasonLen)
		cand.SavedRate = k.SavedRate
		cand.HitRatio = min(max(k.HitRatio, 0), 1)
	}
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

// buildBatch is called by the telemetry reporter. It drains the aggregate.
func (c *Client) buildBatch() *telemetry.Batch {
	a := c.agg
	a.mu.Lock()
	if a.start.IsZero() {
		a.mu.Unlock()
		return nil
	}
	cs, ec, errs, byp := a.curCS, a.curEC, a.curErr, a.curByp
	t := a.totals
	t.LocalHits = min(cs.Hits-a.prevCS.Hits, t.Reads)
	t.Bypasses = byp - a.prevByp
	t.RedisReads = t.Reads - t.LocalHits
	t.Invalidations = cs.Invalidations - a.prevCS.Invalidations
	t.Evictions = cs.Evictions - a.prevCS.Evictions
	t.Errors = errs - a.prevErr
	t.Admissions = ec.Admissions - a.prevEC.Admissions
	t.Demotions = ec.Demotions - a.prevEC.Demotions

	cands := make([]telemetry.Candidate, 0, len(a.keys))
	for _, k := range a.keys {
		cands = append(cands, *k)
	}
	slices.SortFunc(cands, func(x, y telemetry.Candidate) int {
		switch {
		case x.Reads > y.Reads:
			return -1
		case x.Reads < y.Reads:
			return 1
		case x.FP < y.FP:
			return -1
		default:
			return 1
		}
	})
	if len(cands) > a.maxKeys {
		cands = cands[:a.maxKeys]
	}
	start, end, det := a.start, a.end, a.det
	a.start, a.totals, a.det = time.Time{}, telemetry.Totals{}, telemetry.Detector{}
	clear(a.keys)
	a.prevCS, a.prevEC, a.prevErr, a.prevByp = cs, ec, errs, byp
	a.mu.Unlock()

	eff := c.engine.Policy()
	tst := c.monitor.Status()
	caps := c.backend.Capabilities()
	live := c.backend.CacheStats()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	var dropped uint64
	if c.reporter != nil {
		rs := c.reporter.Stats()
		dropped = rs.Dropped + rs.Rejected + rs.Failed
	}
	return &telemetry.Batch{
		SchemaVersion: telemetry.SchemaVersion,
		Service:       c.cfg.ServiceName, Instance: c.instanceID, Seq: c.seq.Add(1),
		SDKVersion: Version, Driver: caps.Driver, Topology: string(caps.Topology),
		WindowStart: start, WindowEnd: end,
		FingerprintScope: string(c.hasher.Scope()), FingerprintKeyID: c.hasher.KeyID(),
		PolicyVersion: eff.Version, PolicySource: eff.Source, CacheMode: string(eff.Mode),
		Tracking: telemetry.Tracking{State: string(tst.State), Reason: truncate(tst.Reason, telemetry.MaxReasonLen), Mode: caps.TrackingMode},
		Totals:   t,
		Cache: telemetry.Cache{Entries: live.Entries, MemoryBytes: live.MemoryBytes,
			MaxEntries: int64(eff.Budget.MaxEntries), MaxMemoryBytes: eff.Budget.MaxMemoryBytes, Admitted: c.engine.AdmittedCount()},
		Detector:   det,
		Runtime:    telemetry.Runtime{HeapAllocBytes: ms.HeapAlloc, Goroutines: runtime.NumGoroutine(), GCCPUFraction: ms.GCCPUFraction, TelemetryDropped: dropped},
		Candidates: cands,
	}
}
