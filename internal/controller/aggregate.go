package controller

import (
	"fmt"
	"maps"
	"slices"
	"sort"
	"strconv"
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

type keyAgg struct {
	view     KeyView
	sizeSum  int64
	sizeN    int64
	fills    uint64
	cachedRd uint64
	services map[string]*ServiceShare
}

// Build aggregates the registry snapshot into a new View.
func (a *Aggregator) Build(recs []instanceRecord, now time.Time, ctl ControllerInfo) *View {
	start := time.Now()
	v := &View{}
	ov := &v.Overview
	ov.GeneratedAt = now
	ov.Tracking = map[string]int{}
	ov.PolicyVersions = map[string]int{}
	services := map[string]*ServiceView{}
	keys := map[string]*keyAgg{}
	keyIDs := map[string]bool{}
	var instanceScope, unhealthy, unsupported, memHot int
	stateCounts := map[string]int{}

	slices.SortFunc(recs, func(x, y instanceRecord) int {
		if x.service != y.service {
			if x.service < y.service {
				return -1
			}
			return 1
		}
		if x.instance < y.instance {
			return -1
		}
		return 1
	})

	for i := range recs {
		rec := &recs[i]
		b := rec.latest
		if b == nil {
			continue
		}
		fresh := now.Sub(rec.lastSeen) <= a.cfg.FreshFor
		sv := services[rec.service]
		if sv == nil {
			sv = &ServiceView{Name: rec.service}
			services[rec.service] = sv
		}
		rates := batchRates(b)
		iv := InstanceView{Service: rec.service, Instance: rec.instance, SDKVersion: b.SDKVersion, Driver: b.Driver,
			Topology: b.Topology, Tracking: b.Tracking, FirstSeen: rec.firstSeen, LastSeen: rec.lastSeen,
			AgeSeconds: now.Sub(rec.lastSeen).Seconds(), Fresh: fresh, PolicyVersion: b.PolicyVersion,
			PolicySource: b.PolicySource, CacheMode: b.CacheMode, Rates: rates, HitRatio: rates.HitRatio(),
			Cache: b.Cache, Detector: b.Detector, Runtime: b.Runtime, FingerprintScope: b.FingerprintScope,
			Batches: rec.batches, Duplicates: rec.duplicates}
		v.Instances = append(v.Instances, iv)
		if !fresh {
			ov.StaleInstances++
			sv.Stale++
			continue
		}
		ov.ActiveInstances++
		sv.Instances++
		ov.Rates.add(rates)
		sv.Rates.add(rates)
		ov.Tracking[b.Tracking.State]++
		ov.PolicyVersions["v"+strconv.FormatInt(b.PolicyVersion, 10)+" ("+b.PolicySource+")"]++
		ov.TelemetryDropped += b.Runtime.TelemetryDropped
		if b.Cache.Entries > 0 {
			ov.CacheEntries += b.Cache.Entries
			sv.CacheEntries += b.Cache.Entries
		}
		if b.Cache.MemoryBytes > 0 {
			ov.CacheMemoryBytes += b.Cache.MemoryBytes
			sv.CacheMemoryBytes += b.Cache.MemoryBytes
			if b.Cache.MaxMemoryBytes > 0 && float64(b.Cache.MemoryBytes) > 0.9*float64(b.Cache.MaxMemoryBytes) {
				memHot++
			}
		}
		ov.CacheMemoryLimit += b.Cache.MaxMemoryBytes
		ov.AdmittedKeys += b.Cache.Admitted
		sv.AdmittedKeys += b.Cache.Admitted
		switch b.Tracking.State {
		case "unhealthy":
			unhealthy++
			sv.TrackingUnhealthy++
		case "unsupported":
			unsupported++
			sv.TrackingUnsupport++
		}
		if !slices.Contains(sv.PolicyVersions, b.PolicyVersion) {
			sv.PolicyVersions = append(sv.PolicyVersions, b.PolicyVersion)
		}
		if !slices.Contains(sv.Drivers, b.Driver) {
			sv.Drivers = append(sv.Drivers, b.Driver)
		}
		shared := b.FingerprintScope == "shared"
		if shared {
			keyIDs[b.FingerprintKeyID] = true
		} else {
			instanceScope++
		}
		secs := max(b.WindowEnd.Sub(b.WindowStart).Seconds(), 1)
		for _, c := range b.Candidates {
			stateCounts[c.State]++
			id := b.FingerprintKeyID + ":" + c.FP
			scope := "shared"
			if !shared {
				id = rec.instance + ":" + c.FP
				scope = "instance"
			}
			ka := keys[id]
			if ka == nil {
				ka = &keyAgg{view: KeyView{FP: c.FP, Scope: scope, States: map[string]int{}}, services: map[string]*ServiceShare{}}
				keys[id] = ka
			}
			kv := &ka.view
			if kv.Display == "" {
				kv.Display = c.Display
			}
			rr := float64(c.Reads) / secs
			kv.ReadRate += rr
			kv.WriteRate += float64(c.Writes) / secs
			kv.FillRate += float64(c.Fills) / secs
			kv.InvalidationRate += float64(c.Invalidations) / secs
			kv.SavedRate += c.SavedRate
			kv.Instances++
			kv.States[c.State]++
			kv.MaxLocalRate = max(kv.MaxLocalRate, rr)
			if c.SizeBytes >= 0 {
				ka.sizeSum += c.SizeBytes
				ka.sizeN++
			}
			if c.State == "admitted" {
				ka.cachedRd += c.Reads
				ka.fills += c.Fills
			}
			ss := ka.services[rec.service]
			if ss == nil {
				ss = &ServiceShare{Service: rec.service}
				ka.services[rec.service] = ss
			}
			ss.ReadRate += rr
			ss.Instances++
		}
	}
	ov.Services = len(services)
	ov.LocalHitRatio = ov.Rates.HitRatio()
	ctl.AggregationMillis = 0

	// Keys: finalize, rank, bound.
	kvs := make([]KeyView, 0, len(keys))
	for _, ka := range keys {
		kv := ka.view
		if ka.sizeN > 0 {
			kv.AvgSizeBytes = ka.sizeSum / ka.sizeN
		}
		if ka.cachedRd > 0 {
			kv.CachedHitRatio = 1 - min(1, float64(ka.fills)/float64(ka.cachedRd))
		}
		// Per-instance saved rates come from the SDK's last window while
		// read rates cover the whole batch; never report savings above reads.
		kv.SavedRate = min(kv.SavedRate, kv.ReadRate)
		kv.MeanLocalRate = kv.ReadRate / float64(kv.Instances)
		if kv.ReadRate > 0 {
			kv.LocalShare = kv.MaxLocalRate / kv.ReadRate
		}
		switch {
		case kv.Instances == 1:
			kv.Classification = "single-instance"
		case kv.LocalShare >= 0.5:
			kv.Classification = "local"
		default:
			kv.Classification = "global"
		}
		for _, s := range ka.services {
			kv.Services = append(kv.Services, *s)
		}
		slices.SortFunc(kv.Services, func(x, y ServiceShare) int { return cmpFloatDesc(x.ReadRate, y.ReadRate) })
		if len(kv.Services) > 10 {
			kv.Services = kv.Services[:10]
		}
		kv.Recommendation = a.recommend(&kv)
		kvs = append(kvs, kv)
	}
	slices.SortFunc(kvs, func(x, y KeyView) int {
		if c := cmpFloatDesc(x.ReadRate, y.ReadRate); c != 0 {
			return c
		}
		if x.FP < y.FP {
			return -1
		}
		return 1
	})
	if len(kvs) > a.cfg.MaxGlobalKeys {
		kvs = kvs[:a.cfg.MaxGlobalKeys]
	}
	v.Keys = kvs
	ov.GlobalKeys = len(kvs)

	for _, sv := range services {
		sv.HitRatio = sv.Rates.HitRatio()
		slices.Sort(sv.PolicyVersions)
		v.Services = append(v.Services, *sv)
	}
	sort.Slice(v.Services, func(i, j int) bool { return v.Services[i].Name < v.Services[j].Name })

	// Cache efficiency.
	cv := &v.Cache
	cv.Rates, cv.HitRatio = ov.Rates, ov.LocalHitRatio
	cv.MemoryBytes, cv.MemoryLimitBytes, cv.Entries, cv.AdmittedKeys = ov.CacheMemoryBytes, ov.CacheMemoryLimit, ov.CacheEntries, ov.AdmittedKeys
	if cv.MemoryLimitBytes > 0 {
		cv.MemoryUtilization = float64(cv.MemoryBytes) / float64(cv.MemoryLimitBytes)
	}
	cv.ChurnPerMinute = (ov.Rates.Admissions + ov.Rates.Demotions) * 60
	cv.StateCounts = stateCounts
	byBenefit := slices.Clone(kvs)
	slices.SortFunc(byBenefit, func(x, y KeyView) int { return cmpFloatDesc(x.SavedRate, y.SavedRate) })
	cv.TopBeneficial = byBenefit[:min(20, len(byBenefit))]
	var churn []KeyView
	for _, k := range kvs {
		if k.States["cooldown"] > 0 || (k.FillRate > 0 && k.InvalidationRate > 0.3*k.ReadRate) {
			churn = append(churn, k)
		}
	}
	slices.SortFunc(churn, func(x, y KeyView) int { return cmpFloatDesc(x.InvalidationRate, y.InvalidationRate) })
	cv.TopChurn = churn[:min(20, len(churn))]

	// Warnings.
	w := func(sev, code, msg string, n int) {
		ov.Warnings = append(ov.Warnings, Warning{Severity: sev, Code: code, Message: msg, Count: n})
	}
	if unhealthy > 0 {
		w("critical", "tracking_unhealthy", fmt.Sprintf("%d instance(s) report unhealthy client tracking; their local caches are suspended", unhealthy), unhealthy)
	}
	if unsupported > 0 {
		w("info", "tracking_unsupported", fmt.Sprintf("%d instance(s) cannot use local caching (unsupported driver, topology, or permissions)", unsupported), unsupported)
	}
	if ov.StaleInstances > 0 {
		w("warning", "stale_telemetry", fmt.Sprintf("%d instance(s) have not reported recently", ov.StaleInstances), ov.StaleInstances)
	}
	if instanceScope > 0 {
		w("warning", "instance_fingerprints", fmt.Sprintf("%d instance(s) use per-process fingerprint secrets; their keys cannot be aggregated across instances", instanceScope), instanceScope)
	}
	if len(keyIDs) > 1 {
		w("warning", "fingerprint_secret_mismatch", fmt.Sprintf("%d different fingerprint secrets are in use; keys are aggregated per secret", len(keyIDs)), len(keyIDs))
	}
	if ov.TelemetryDropped > 0 {
		w("warning", "telemetry_dropped", fmt.Sprintf("SDK instances report %d dropped telemetry batches", ov.TelemetryDropped), int(min(ov.TelemetryDropped, 1<<30)))
	}
	if memHot > 0 {
		w("info", "cache_memory_pressure", fmt.Sprintf("%d instance(s) use more than 90%% of their cache memory budget", memHot), memHot)
	}
	if len(churn) > 0 {
		w("info", "invalidation_churn", fmt.Sprintf("%d key(s) show invalidation churn; adaptive admission keeps them in cooldown", len(churn)), len(churn))
	}
	if cap := ctl.RegistryCapacity; cap > 0 && len(recs) > cap*9/10 {
		w("warning", "registry_capacity", fmt.Sprintf("instance registry at %d of %d", len(recs), cap), len(recs))
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	// Time series and trends.
	a.history = append(a.history, Point{At: now, Reads: ov.Rates.Reads, LocalHits: ov.Rates.LocalHits, Redis: ov.Rates.RedisReads,
		Writes: ov.Rates.Writes, Invalids: ov.Rates.Invalidations, Instances: ov.ActiveInstances,
		MemoryMB: float64(ov.CacheMemoryBytes) / (1 << 20), HitRatio: ov.LocalHitRatio})
	if over := len(a.history) - a.cfg.HistoryPoints; over > 0 {
		a.history = slices.Delete(a.history, 0, over)
	}
	for i := 0; i < len(v.Keys) && i < a.cfg.TrendKeys; i++ {
		fp := v.Keys[i].FP
		s := append(a.trends[fp], v.Keys[i].ReadRate)
		if over := len(s) - a.cfg.TrendPoints; over > 0 {
			s = slices.Delete(s, 0, over)
		}
		a.trends[fp], a.trendAt[fp] = s, now
	}
	// Bound trend memory: drop series not refreshed recently.
	if len(a.trends) > a.cfg.TrendKeys*3 {
		fps := slices.Collect(maps.Keys(a.trendAt))
		slices.SortFunc(fps, func(x, y string) int { return a.trendAt[x].Compare(a.trendAt[y]) })
		for _, fp := range fps[:len(fps)-a.cfg.TrendKeys*3] {
			delete(a.trends, fp)
			delete(a.trendAt, fp)
		}
	}
	for i := range v.Keys {
		if s, ok := a.trends[v.Keys[i].FP]; ok && a.trendAt[v.Keys[i].FP].Equal(now) {
			v.Keys[i].Trend = slices.Clone(s)
		}
	}
	ctl.AggregationMillis = float64(time.Since(start).Microseconds()) / 1000
	ov.Controller = ctl
	a.view = v
	return v
}

func (a *Aggregator) recommend(k *KeyView) string {
	writeHeavy := k.ReadRate > 0 && (k.WriteRate+k.InvalidationRate) > 0.5*k.ReadRate
	switch {
	case writeHeavy:
		return "write-heavy: local caching would mostly churn; keep uncached"
	case k.States["cooldown"] > 0:
		return "invalidation churn on some instances; adaptive admission placed it in cooldown"
	case k.States["bypassed"] > 0:
		return "excluded by policy or size limits on some instances"
	case k.States["admitted"] == k.Instances:
		return "cached on every reporting instance"
	case k.States["admitted"] > 0:
		return "cached on instances where it is locally hot"
	case k.Instances > 1 && k.MaxLocalRate < a.cfg.MinLocalRate && k.ReadRate >= a.cfg.MinLocalRate:
		return "globally hot but locally cold on each instance: per-pod caching saves little"
	case k.MaxLocalRate >= a.cfg.MinLocalRate:
		return "candidate: hot locally, not yet admitted"
	default:
		return "cold"
	}
}

func cmpFloatDesc(a, b float64) int {
	switch {
	case a > b:
		return -1
	case a < b:
		return 1
	}
	return 0
}
