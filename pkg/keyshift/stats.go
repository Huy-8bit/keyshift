package keyshift

import (
	"time"

	"github.com/Huy-8bit/keyshift/internal/rediscompat"
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
	// NotifiedWrites counts keys passed to NotifyWrite (writes made outside
	// the client). They are not included in Writes.
	NotifiedWrites uint64 `json:"notified_writes"`

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
		Writes: c.writes.Load(), Deletes: c.deletes.Load(), NotifiedWrites: c.notifiedWrites.Load(), Errors: c.errorsTotal.Load(), CacheFallbacks: c.fallbacks.Load(),
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
