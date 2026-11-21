package keyshift

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Huy-8bit/keyshift/internal/tracking"
)

type readPath int

const (
	pathCache readPath = iota
	pathDirect
)

// collector exposes client statistics as Prometheus metrics. Counters are
// read from the client at scrape time, so the request path pays only for
// the optional latency histogram. No metric carries a Redis key label.
type collector struct {
	c        *Client
	latency  *prometheus.HistogramVec
	rotation prometheus.Histogram
	byPath   [2]prometheus.Observer
	descs    map[string]*prometheus.Desc
}

func newCollector(c *Client) *collector {
	labels := prometheus.Labels{"service": c.cfg.ServiceName}
	col := &collector{c: c, descs: map[string]*prometheus.Desc{}}
	col.latency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "keyshift", Name: "read_duration_seconds",
		Help:        "Latency of Get calls by path (cache: admitted key via the tracked client-side cache; direct: Redis).",
		ConstLabels: labels,
		Buckets:     []float64{.00001, .000025, .00005, .0001, .00025, .0005, .001, .0025, .005, .01, .025, .1, .5},
	}, []string{"path"})
	col.byPath[pathCache] = col.latency.WithLabelValues("cache")
	col.byPath[pathDirect] = col.latency.WithLabelValues("direct")
	col.rotation = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "keyshift", Name: "window_rotation_duration_seconds", ConstLabels: labels,
		Help:    "Time spent closing an observation window and re-evaluating admission.",
		Buckets: prometheus.ExponentialBuckets(0.00005, 2, 14),
	})
	d := func(name, help string, varLabels ...string) {
		col.descs[name] = prometheus.NewDesc("keyshift_"+name, help, varLabels, labels)
	}
	d("requests_total", "Logical requests by operation and path.", "op", "path")
	d("cache_hits_total", "Reads served from the local cache without contacting Redis.")
	d("cache_misses_total", "Cache-path reads that were fetched from Redis (fills).")
	d("cache_bypasses_total", "Reads that explicitly bypassed the local cache (GetFresh).")
	d("cache_fallbacks_total", "Cache-path reads retried on the direct path after a transport error.")
	d("cache_entries", "Entries resident in the local cache (-1 when unknown).")
	d("cache_memory_bytes", "Estimated memory of local cache entries (-1 when unknown). Not total process memory.")
	d("cache_evictions_total", "Entries removed by the local cache itself: capacity evictions and max_staleness expiries.")
	d("cache_admitted_keys", "Keys currently admitted for local caching.")
	d("hotkey_candidates", "Keys under observation in the last window, by admission state.", "state")
	d("admission_decisions_total", "Admission engine transitions.", "decision")
	d("invalidation_events_total", "Server invalidation events received for locally cached keys.")
	d("tracking_errors_total", "Client tracking probe failures and cache-path transport errors.")
	d("tracking_healthy", "1 when client tracking is healthy and caching may be used.")
	d("tracking_state", "Client tracking state (1 for the current state).", "state")
	d("policy_version", "Version of the active caching policy (0 for the local policy).")
	d("policy_rejected_total", "Remote policies rejected as invalid.")
	d("telemetry_dropped_total", "Telemetry batches dropped (queue full, retries exhausted, or rejected).")
	d("controller_requests_total", "Telemetry uploads to the controller by result.", "result")
	d("detector_observations_total", "Reads observed by the hot-key detector.")
	d("detector_coverage_ratio", "Fraction of recorded reads attributed to the top-K candidates in the last window.")
	d("errors_total", "Redis errors returned to the application (excluding Nil).")
	return col
}

func (col *collector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range col.descs {
		ch <- d
	}
	col.latency.Describe(ch)
	col.rotation.Describe(ch)
}

var trackingStates = []tracking.State{tracking.StateUnknown, tracking.StateHealthy, tracking.StateUnhealthy, tracking.StateUnsupported, tracking.StateDisabled}

func (col *collector) Collect(ch chan<- prometheus.Metric) {
	s := col.c.Stats()
	counter := func(name string, v uint64, lv ...string) {
		ch <- prometheus.MustNewConstMetric(col.descs[name], prometheus.CounterValue, float64(v), lv...)
	}
	gauge := func(name string, v float64, lv ...string) {
		ch <- prometheus.MustNewConstMetric(col.descs[name], prometheus.GaugeValue, v, lv...)
	}
	counter("requests_total", s.CachedReads, "get", "cache")
	counter("requests_total", s.DirectReads, "get", "direct")
	counter("requests_total", s.Bypasses, "get", "bypass")
	counter("requests_total", s.Writes, "set", "direct")
	counter("requests_total", s.Deletes, "del", "direct")
	counter("cache_hits_total", s.LocalHits)
	counter("cache_misses_total", s.LocalMisses)
	counter("cache_bypasses_total", s.Bypasses)
	counter("cache_fallbacks_total", s.CacheFallbacks)
	gauge("cache_entries", float64(s.Entries))
	gauge("cache_memory_bytes", float64(s.MemoryBytes))
	counter("cache_evictions_total", s.Evictions)
	gauge("cache_admitted_keys", float64(s.AdmittedKeys))
	gauge("hotkey_candidates", float64(s.LastWindow.Candidates), "candidate")
	gauge("hotkey_candidates", float64(s.AdmittedKeys), "admitted")
	gauge("hotkey_candidates", float64(s.LastWindow.Cooldown), "cooldown")
	counter("admission_decisions_total", s.Admissions, "admit")
	counter("admission_decisions_total", s.Demotions, "demote")
	counter("admission_decisions_total", s.Cooldowns, "cooldown")
	counter("admission_decisions_total", s.BudgetRejections, "budget_reject")
	counter("invalidation_events_total", s.Invalidations)
	counter("tracking_errors_total", s.Tracking.Errors)
	healthy := 0.0
	if s.Tracking.State == tracking.StateHealthy {
		healthy = 1
	}
	gauge("tracking_healthy", healthy)
	for _, st := range trackingStates {
		v := 0.0
		if s.Tracking.State == st {
			v = 1
		}
		gauge("tracking_state", v, string(st))
	}
	gauge("policy_version", float64(s.PolicyVersion))
	counter("policy_rejected_total", s.PolicyRejected)
	counter("telemetry_dropped_total", s.TelemetryDropped)
	counter("controller_requests_total", s.TelemetrySent, "sent")
	counter("controller_requests_total", s.TelemetryFailed, "failed")
	counter("detector_observations_total", s.Reads)
	gauge("detector_coverage_ratio", s.LastWindow.Coverage)
	counter("errors_total", s.Errors)
	col.latency.Collect(ch)
	col.rotation.Collect(ch)
}

func (c *Client) now() time.Time {
	if c.metrics == nil || c.cfg.Metrics.DisableLatencyHistograms {
		return time.Time{}
	}
	return time.Now()
}

func (c *Client) observe(start time.Time, p readPath) {
	if start.IsZero() {
		return
	}
	c.metrics.byPath[p].Observe(time.Since(start).Seconds())
}
