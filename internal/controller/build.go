package controller

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/Huy-8bit/keyshift/internal/telemetry"
)

// Limits of the per-key and cache views.
const (
	maxServicesPerKey = 10
	maxTopKeys        = 20
	// memoryPressure is the share of an instance's cache budget above which
	// it counts as under memory pressure.
	memoryPressure = 0.9
)

// keyAgg accumulates one key fingerprint across instances.
type keyAgg struct {
	view     KeyView
	sizeSum  int64
	sizeN    int64
	fills    uint64
	cachedRd uint64
	services map[string]*ServiceShare
}

// viewBuilder accumulates one aggregation pass. It is not safe for
// concurrent use and is discarded after Build.
type viewBuilder struct {
	cfg AggregatorConfig
	now time.Time
	v   *View

	services    map[string]*ServiceView
	keys        map[string]*keyAgg
	keyIDs      map[string]bool // fingerprint secrets in use (shared scope)
	stateCounts map[string]int  // candidate states over all instances

	instanceScope int // instances with per-process fingerprint secrets
	unhealthy     int
	unsupported   int
	memoryHot     int
}

// Build aggregates the registry snapshot into a new View.
func (a *Aggregator) Build(recs []instanceRecord, now time.Time, ctl ControllerInfo) *View {
	start := time.Now()
	b := &viewBuilder{
		cfg: a.cfg, now: now,
		v: &View{Overview: Overview{
			GeneratedAt: now, Tracking: map[string]int{}, PolicyVersions: map[string]int{},
		}},
		services:    map[string]*ServiceView{},
		keys:        map[string]*keyAgg{},
		keyIDs:      map[string]bool{},
		stateCounts: map[string]int{},
	}
	slices.SortFunc(recs, func(x, y instanceRecord) int {
		return cmp.Or(strings.Compare(x.service, y.service), strings.Compare(x.instance, y.instance))
	})
	for i := range recs {
		b.addInstance(&recs[i])
	}
	v := b.v
	ov := &v.Overview
	ov.Services = len(b.services)
	ov.LocalHitRatio = ov.Rates.HitRatio()
	v.Keys = b.rankKeys(a.recommend)
	ov.GlobalKeys = len(v.Keys)
	v.Services = b.serviceViews()
	churn := b.buildCacheView()
	ov.Warnings = b.warnings(len(churn), len(recs), ctl.RegistryCapacity)

	a.mu.Lock()
	defer a.mu.Unlock()
	a.recordHistoryLocked(v, now)
	ctl.AggregationMillis = float64(time.Since(start).Microseconds()) / 1000
	ov.Controller = ctl
	a.view = v
	return v
}

// addInstance folds one instance's latest batch into the view. Stale
// instances are listed but excluded from every aggregate.
func (b *viewBuilder) addInstance(rec *instanceRecord) {
	batch := rec.latest
	if batch == nil {
		return
	}
	fresh := b.now.Sub(rec.lastSeen) <= b.cfg.FreshFor
	sv := b.service(rec.service)
	rates := batchRates(batch)
	b.v.Instances = append(b.v.Instances, InstanceView{
		Service: rec.service, Instance: rec.instance, SDKVersion: batch.SDKVersion, Driver: batch.Driver,
		Topology: batch.Topology, Tracking: batch.Tracking, FirstSeen: rec.firstSeen, LastSeen: rec.lastSeen,
		AgeSeconds: b.now.Sub(rec.lastSeen).Seconds(), Fresh: fresh, PolicyVersion: batch.PolicyVersion,
		PolicySource: batch.PolicySource, CacheMode: batch.CacheMode, Rates: rates, HitRatio: rates.HitRatio(),
		Cache: batch.Cache, Detector: batch.Detector, Runtime: batch.Runtime, FingerprintScope: batch.FingerprintScope,
		Batches: rec.batches, Duplicates: rec.duplicates,
	})
	ov := &b.v.Overview
	if !fresh {
		ov.StaleInstances++
		sv.Stale++
		return
	}
	ov.ActiveInstances++
	sv.Instances++
	ov.Rates.add(rates)
	sv.Rates.add(rates)
	ov.Tracking[batch.Tracking.State]++
	ov.PolicyVersions["v"+strconv.FormatInt(batch.PolicyVersion, 10)+" ("+batch.PolicySource+")"]++
	ov.TelemetryDropped += batch.Runtime.TelemetryDropped
	b.addCache(sv, batch.Cache)
	switch batch.Tracking.State {
	case "unhealthy":
		b.unhealthy++
		sv.TrackingUnhealthy++
	case "unsupported":
		b.unsupported++
		sv.TrackingUnsupport++
	}
	if !slices.Contains(sv.PolicyVersions, batch.PolicyVersion) {
		sv.PolicyVersions = append(sv.PolicyVersions, batch.PolicyVersion)
	}
	if !slices.Contains(sv.Drivers, batch.Driver) {
		sv.Drivers = append(sv.Drivers, batch.Driver)
	}
	b.addCandidates(rec, batch)
}

func (b *viewBuilder) service(name string) *ServiceView {
	sv := b.services[name]
	if sv == nil {
		sv = &ServiceView{Name: name}
		b.services[name] = sv
	}
	return sv
}

func (b *viewBuilder) addCache(sv *ServiceView, c telemetry.Cache) {
	ov := &b.v.Overview
	if c.Entries > 0 {
		ov.CacheEntries += c.Entries
		sv.CacheEntries += c.Entries
	}
	if c.MemoryBytes > 0 {
		ov.CacheMemoryBytes += c.MemoryBytes
		sv.CacheMemoryBytes += c.MemoryBytes
		if c.MaxMemoryBytes > 0 && float64(c.MemoryBytes) > memoryPressure*float64(c.MaxMemoryBytes) {
			b.memoryHot++
		}
	}
	ov.CacheMemoryLimit += c.MaxMemoryBytes
	ov.AdmittedKeys += c.Admitted
	sv.AdmittedKeys += c.Admitted
}

// addCandidates folds one batch's hot-key candidates into the key table.
// Fingerprints from a shared secret aggregate across instances; those from
// a per-process secret only within their instance.
func (b *viewBuilder) addCandidates(rec *instanceRecord, batch *telemetry.Batch) {
	shared := batch.FingerprintScope == "shared"
	if shared {
		b.keyIDs[batch.FingerprintKeyID] = true
	} else {
		b.instanceScope++
	}
	secs := max(batch.WindowEnd.Sub(batch.WindowStart).Seconds(), 1)
	for _, c := range batch.Candidates {
		b.stateCounts[c.State]++
		id, scope := batch.FingerprintKeyID+":"+c.FP, "shared"
		if !shared {
			id, scope = rec.instance+":"+c.FP, "instance"
		}
		ka := b.keys[id]
		if ka == nil {
			ka = &keyAgg{view: KeyView{FP: c.FP, Scope: scope, States: map[string]int{}}, services: map[string]*ServiceShare{}}
			b.keys[id] = ka
		}
		ka.add(rec.service, c, secs)
	}
}

func (ka *keyAgg) add(service string, c telemetry.Candidate, secs float64) {
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
	ss := ka.services[service]
	if ss == nil {
		ss = &ServiceShare{Service: service}
		ka.services[service] = ss
	}
	ss.ReadRate += rr
	ss.Instances++
}

// finish derives the per-key ratios, classification, and service shares.
func (ka *keyAgg) finish(recommend func(*KeyView) string) KeyView {
	kv := ka.view
	if ka.sizeN > 0 {
		kv.AvgSizeBytes = ka.sizeSum / ka.sizeN
	}
	if ka.cachedRd > 0 {
		kv.CachedHitRatio = 1 - min(1, float64(ka.fills)/float64(ka.cachedRd))
	}
	// Per-instance saved rates come from the SDK's last window while read
	// rates cover the whole batch; never report savings above reads.
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
	if len(kv.Services) > maxServicesPerKey {
		kv.Services = kv.Services[:maxServicesPerKey]
	}
	kv.Recommendation = recommend(&kv)
	return kv
}

// rankKeys finalizes every key and returns the MaxGlobalKeys hottest.
func (b *viewBuilder) rankKeys(recommend func(*KeyView) string) []KeyView {
	kvs := make([]KeyView, 0, len(b.keys))
	for _, ka := range b.keys {
		kvs = append(kvs, ka.finish(recommend))
	}
	slices.SortFunc(kvs, func(x, y KeyView) int {
		return cmp.Or(cmpFloatDesc(x.ReadRate, y.ReadRate), strings.Compare(x.FP, y.FP))
	})
	return kvs[:min(len(kvs), b.cfg.MaxGlobalKeys)]
}

// serviceViews returns the services sorted by name.
func (b *viewBuilder) serviceViews() []ServiceView {
	out := make([]ServiceView, 0, len(b.services))
	for _, sv := range b.services {
		sv.HitRatio = sv.Rates.HitRatio()
		slices.Sort(sv.PolicyVersions)
		out = append(out, *sv)
	}
	slices.SortFunc(out, func(x, y ServiceView) int { return strings.Compare(x.Name, y.Name) })
	return out
}

// buildCacheView fills View.Cache and returns the keys showing invalidation
// churn.
func (b *viewBuilder) buildCacheView() []KeyView {
	ov, kvs := &b.v.Overview, b.v.Keys
	cv := &b.v.Cache
	cv.Rates, cv.HitRatio = ov.Rates, ov.LocalHitRatio
	cv.MemoryBytes, cv.MemoryLimitBytes, cv.Entries, cv.AdmittedKeys = ov.CacheMemoryBytes, ov.CacheMemoryLimit, ov.CacheEntries, ov.AdmittedKeys
	if cv.MemoryLimitBytes > 0 {
		cv.MemoryUtilization = float64(cv.MemoryBytes) / float64(cv.MemoryLimitBytes)
	}
	cv.ChurnPerMinute = (ov.Rates.Admissions + ov.Rates.Demotions) * 60
	cv.StateCounts = b.stateCounts

	byBenefit := slices.Clone(kvs)
	slices.SortFunc(byBenefit, func(x, y KeyView) int { return cmpFloatDesc(x.SavedRate, y.SavedRate) })
	cv.TopBeneficial = byBenefit[:min(maxTopKeys, len(byBenefit))]

	var churn []KeyView
	for _, k := range kvs {
		if k.States["cooldown"] > 0 || (k.FillRate > 0 && k.InvalidationRate > 0.3*k.ReadRate) {
			churn = append(churn, k)
		}
	}
	slices.SortFunc(churn, func(x, y KeyView) int { return cmpFloatDesc(x.InvalidationRate, y.InvalidationRate) })
	cv.TopChurn = churn[:min(maxTopKeys, len(churn))]
	return churn
}

// warnings lists the operational problems visible in this view.
func (b *viewBuilder) warnings(churnKeys, registered, registryCapacity int) []Warning {
	ov := &b.v.Overview
	var out []Warning
	add := func(sev, code string, n int, format string, args ...any) {
		out = append(out, Warning{Severity: sev, Code: code, Message: fmt.Sprintf(format, args...), Count: n})
	}
	if b.unhealthy > 0 {
		add("critical", "tracking_unhealthy", b.unhealthy,
			"%d instance(s) report unhealthy client tracking; their local caches are suspended", b.unhealthy)
	}
	if b.unsupported > 0 {
		add("info", "tracking_unsupported", b.unsupported,
			"%d instance(s) cannot use local caching (unsupported driver, topology, or permissions)", b.unsupported)
	}
	if ov.StaleInstances > 0 {
		add("warning", "stale_telemetry", ov.StaleInstances, "%d instance(s) have not reported recently", ov.StaleInstances)
	}
	if b.instanceScope > 0 {
		add("warning", "instance_fingerprints", b.instanceScope,
			"%d instance(s) use per-process fingerprint secrets; their keys cannot be aggregated across instances", b.instanceScope)
	}
	if n := len(b.keyIDs); n > 1 {
		add("warning", "fingerprint_secret_mismatch", n, "%d different fingerprint secrets are in use; keys are aggregated per secret", n)
	}
	if ov.TelemetryDropped > 0 {
		add("warning", "telemetry_dropped", int(min(ov.TelemetryDropped, 1<<30)),
			"SDK instances report %d dropped telemetry batches", ov.TelemetryDropped)
	}
	if b.memoryHot > 0 {
		add("info", "cache_memory_pressure", b.memoryHot,
			"%d instance(s) use more than 90%% of their cache memory budget", b.memoryHot)
	}
	if churnKeys > 0 {
		add("info", "invalidation_churn", churnKeys,
			"%d key(s) show invalidation churn; adaptive admission keeps them in cooldown", churnKeys)
	}
	if registryCapacity > 0 && registered > registryCapacity*9/10 {
		add("warning", "registry_capacity", registered, "instance registry at %d of %d", registered, registryCapacity)
	}
	return out
}

// recordHistoryLocked appends the global time-series point and per-key
// trends, bounding both. a.mu must be held.
func (a *Aggregator) recordHistoryLocked(v *View, now time.Time) {
	ov := &v.Overview
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
}

// recommend suggests what to do about a key, for operators.
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
