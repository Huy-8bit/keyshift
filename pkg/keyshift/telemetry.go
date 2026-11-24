package keyshift

import (
	"context"
	"fmt"
	"net/http"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/Huy-8bit/keyshift/internal/admission"
	"github.com/Huy-8bit/keyshift/internal/policy"
	"github.com/Huy-8bit/keyshift/internal/rediscompat"
	"github.com/Huy-8bit/keyshift/internal/telemetry"
)

// startTelemetry creates the reporter, fetches the current policy once
// (best effort: the controller is optional and must not block startup), and
// starts periodic uploads.
func (c *Client) startTelemetry() error {
	t := c.cfg.Telemetry
	hc := t.HTTPClient
	if hc == nil {
		tr := http.DefaultTransport.(*http.Transport).Clone()
		tr.TLSClientConfig = t.TLSConfig
		tr.MaxIdleConnsPerHost = 2
		hc = &http.Client{Timeout: 10 * time.Second, Transport: tr}
	}
	var err error
	c.reporter, err = telemetry.NewReporter(telemetry.ReporterConfig{
		Endpoint: strings.TrimRight(t.Endpoint, "/"), Token: t.Token, Interval: t.Interval,
		HTTPClient: hc, Build: c.buildBatch, Logger: c.cfg.Logger,
		OnPolicy:      func(env *policy.Envelope) { _ = c.applyEnvelope(env) },
		OnNoPolicy:    c.revertToLocalPolicy,
		OnContact:     func() { c.lastContact.Store(time.Now().UnixNano()) },
		PolicyVersion: func() int64 { return c.engine.Policy().Version },
		Service:       c.cfg.ServiceName, Instance: c.instanceID,
	})
	if err != nil {
		return fmt.Errorf("keyshift: %w", err)
	}
	if !t.DisableRemotePolicy {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if env, err := c.reporter.FetchPolicy(ctx); err != nil {
			c.cfg.Logger.Warn("keyshift controller unreachable at startup; using local policy", "error", err)
		} else if env != nil {
			_ = c.applyEnvelope(env)
		}
		cancel()
	}
	c.reporter.Start()
	return nil
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
