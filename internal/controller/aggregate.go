package controller

import (
	"slices"
	"sync"
	"time"

	"github.com/Huy-8bit/keyshift/internal/telemetry"
)

// Warning is an operational warning shown by the CLI and exported to Prometheus.
type Warning struct {
	Severity string `json:"severity"` // info, warning, critical
	Code     string `json:"code"`
	Message  string `json:"message"`
	Count    int    `json:"count,omitempty"`
}

// Rates groups per-second rates derived from telemetry windows.
type Rates struct {
	Reads         float64 `json:"reads"`
	LocalHits     float64 `json:"local_hits"`
	RedisReads    float64 `json:"redis_reads"`
	Writes        float64 `json:"writes"`
	Invalidations float64 `json:"invalidations"`
	Evictions     float64 `json:"evictions"`
	Admissions    float64 `json:"admissions"`
	Demotions     float64 `json:"demotions"`
}

func (r *Rates) add(o Rates) {
	r.Reads += o.Reads
	r.LocalHits += o.LocalHits
	r.RedisReads += o.RedisReads
	r.Writes += o.Writes
	r.Invalidations += o.Invalidations
	r.Evictions += o.Evictions
	r.Admissions += o.Admissions
	r.Demotions += o.Demotions
}

// HitRatio is the fraction of SDK reads served locally.
func (r Rates) HitRatio() float64 {
	if r.Reads == 0 {
		return 0
	}
	return min(1, r.LocalHits/r.Reads)
}

// Overview is the global summary.
type Overview struct {
	GeneratedAt      time.Time      `json:"generated_at"`
	ActiveInstances  int            `json:"active_instances"`
	StaleInstances   int            `json:"stale_instances"`
	Services         int            `json:"services"`
	Rates            Rates          `json:"rates"`
	LocalHitRatio    float64        `json:"local_hit_ratio"`
	CacheEntries     int64          `json:"cache_entries"`
	CacheMemoryBytes int64          `json:"cache_memory_bytes"`
	CacheMemoryLimit int64          `json:"cache_memory_limit_bytes"`
	AdmittedKeys     int            `json:"admitted_keys"`
	Tracking         map[string]int `json:"tracking"`
	PolicyVersions   map[string]int `json:"policy_versions"`
	TelemetryDropped uint64         `json:"telemetry_dropped"`
	GlobalKeys       int            `json:"global_keys"`
	Warnings         []Warning      `json:"warnings"`
	Controller       ControllerInfo `json:"controller"`
}

// ControllerInfo reports controller self-statistics.
type ControllerInfo struct {
	StartedAt         time.Time `json:"started_at"`
	Version           string    `json:"version"`
	IngestAccepted    uint64    `json:"ingest_accepted"`
	IngestDuplicate   uint64    `json:"ingest_duplicate"`
	IngestRejected    uint64    `json:"ingest_rejected"`
	IngestBytes       uint64    `json:"ingest_bytes"`
	AggregationMillis float64   `json:"aggregation_ms"`
	RegistryCapacity  int       `json:"registry_capacity"`
}

// ServiceView aggregates one service.
type ServiceView struct {
	Name              string   `json:"name"`
	Instances         int      `json:"instances"`
	Stale             int      `json:"stale"`
	Rates             Rates    `json:"rates"`
	HitRatio          float64  `json:"hit_ratio"`
	CacheEntries      int64    `json:"cache_entries"`
	CacheMemoryBytes  int64    `json:"cache_memory_bytes"`
	AdmittedKeys      int      `json:"admitted_keys"`
	TrackingUnhealthy int      `json:"tracking_unhealthy"`
	TrackingUnsupport int      `json:"tracking_unsupported"`
	PolicyVersions    []int64  `json:"policy_versions"`
	Drivers           []string `json:"drivers"`
}

// InstanceView describes one SDK instance.
type InstanceView struct {
	Service          string             `json:"service"`
	Instance         string             `json:"instance"`
	SDKVersion       string             `json:"sdk_version"`
	Driver           string             `json:"driver"`
	Topology         string             `json:"topology"`
	Tracking         telemetry.Tracking `json:"tracking"`
	FirstSeen        time.Time          `json:"first_seen"`
	LastSeen         time.Time          `json:"last_seen"`
	AgeSeconds       float64            `json:"age_seconds"`
	Fresh            bool               `json:"fresh"`
	PolicyVersion    int64              `json:"policy_version"`
	PolicySource     string             `json:"policy_source"`
	CacheMode        string             `json:"cache_mode"`
	Rates            Rates              `json:"rates"`
	HitRatio         float64            `json:"hit_ratio"`
	Cache            telemetry.Cache    `json:"cache"`
	Detector         telemetry.Detector `json:"detector"`
	Runtime          telemetry.Runtime  `json:"runtime"`
	FingerprintScope string             `json:"fingerprint_scope"`
	Batches          uint64             `json:"batches"`
	Duplicates       uint64             `json:"duplicates"`
}

// ServiceShare is one service's share of a key's traffic.
type ServiceShare struct {
	Service   string  `json:"service"`
	ReadRate  float64 `json:"read_rate"`
	Instances int     `json:"instances"`
}

// KeyView is the global view of one key fingerprint.
type KeyView struct {
	FP               string         `json:"fp"`
	Display          string         `json:"display,omitempty"`
	Scope            string         `json:"scope"`
	ReadRate         float64        `json:"read_rate"`
	WriteRate        float64        `json:"write_rate"`
	FillRate         float64        `json:"fill_rate"`
	InvalidationRate float64        `json:"invalidation_rate"`
	SavedRate        float64        `json:"saved_rate"`
	Instances        int            `json:"instances"`
	States           map[string]int `json:"states"`
	Services         []ServiceShare `json:"services"`
	MaxLocalRate     float64        `json:"max_local_rate"`
	MeanLocalRate    float64        `json:"mean_local_rate"`
	LocalShare       float64        `json:"local_share"`
	Classification   string         `json:"classification"`
	AvgSizeBytes     int64          `json:"avg_size_bytes"`
	CachedHitRatio   float64        `json:"cached_hit_ratio"`
	Recommendation   string         `json:"recommendation"`
	Trend            []float64      `json:"trend,omitempty"`
}

// CacheView summarizes cache efficiency.
type CacheView struct {
	Rates             Rates          `json:"rates"`
	HitRatio          float64        `json:"hit_ratio"`
	MemoryBytes       int64          `json:"memory_bytes"`
	MemoryLimitBytes  int64          `json:"memory_limit_bytes"`
	MemoryUtilization float64        `json:"memory_utilization"`
	Entries           int64          `json:"entries"`
	AdmittedKeys      int            `json:"admitted_keys"`
	ChurnPerMinute    float64        `json:"churn_per_minute"`
	StateCounts       map[string]int `json:"state_counts"`
	TopBeneficial     []KeyView      `json:"top_beneficial"`
	TopChurn          []KeyView      `json:"top_churn"`
}

// Point is a time-series sample of global rates.
type Point struct {
	At        time.Time `json:"at"`
	Reads     float64   `json:"reads"`
	LocalHits float64   `json:"local_hits"`
	Redis     float64   `json:"redis_reads"`
	Writes    float64   `json:"writes"`
	Invalids  float64   `json:"invalidations"`
	Instances int       `json:"instances"`
	MemoryMB  float64   `json:"memory_mb"`
	HitRatio  float64   `json:"hit_ratio"`
}

// View is an immutable aggregation result.
type View struct {
	Overview  Overview
	Services  []ServiceView
	Instances []InstanceView
	Keys      []KeyView
	Cache     CacheView
}

// AggregatorConfig bounds aggregation.
type AggregatorConfig struct {
	FreshFor      time.Duration // instances seen within this are active
	MaxGlobalKeys int
	HistoryPoints int
	TrendKeys     int
	TrendPoints   int
	// MinLocalRate is used for recommendations (reads/s an instance needs
	// before local caching is worthwhile; mirrors the SDK default).
	MinLocalRate float64
}

// Aggregator builds Views from registry snapshots. Results are cached and
// rebuilt at most once per tick.
type Aggregator struct {
	cfg AggregatorConfig

	mu      sync.RWMutex
	view    *View
	history []Point
	trends  map[string][]float64
	trendAt map[string]time.Time
}

// NewAggregator creates an aggregator.
func NewAggregator(cfg AggregatorConfig) *Aggregator {
	return &Aggregator{cfg: cfg, view: &View{}, trends: map[string][]float64{}, trendAt: map[string]time.Time{}}
}

// Current returns the latest view.
func (a *Aggregator) Current() *View {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.view
}

// History returns the global time series.
func (a *Aggregator) History() []Point {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return slices.Clone(a.history)
}

func batchRates(b *telemetry.Batch) Rates {
	secs := b.WindowEnd.Sub(b.WindowStart).Seconds()
	if secs < 1 {
		secs = 1
	}
	t := b.Totals
	return Rates{
		Reads: float64(t.Reads) / secs, LocalHits: float64(t.LocalHits) / secs,
		RedisReads: float64(t.RedisReads) / secs, Writes: float64(t.Writes) / secs,
		Invalidations: float64(t.Invalidations) / secs, Evictions: float64(t.Evictions) / secs,
		Admissions: float64(t.Admissions) / secs, Demotions: float64(t.Demotions) / secs,
	}
}
