package controller

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Huy-8bit/keyshift/internal/policy"
)

// fleetCollector exposes the aggregated fleet view as Prometheus metrics so
// that Prometheus stores the history and Grafana renders it. Values are read
// from the latest aggregation at scrape time.
//
// Cardinality is bounded: service labels by --max-services, hot-key labels
// by --metrics-top-keys (the key label is the SDK-approved display name or a
// truncated fingerprint, never a raw key), and per-instance series only when
// --metrics-instances is enabled.
type fleetCollector struct {
	s     *Server
	descs map[string]*prometheus.Desc
}

var rateKinds = []string{"reads", "local_hits", "redis_reads", "writes", "invalidations", "evictions", "admissions", "demotions"}

func ratesByKind(r Rates) []float64 {
	return []float64{r.Reads, r.LocalHits, r.RedisReads, r.Writes, r.Invalidations, r.Evictions, r.Admissions, r.Demotions}
}

func newFleetCollector(s *Server) *fleetCollector {
	c := &fleetCollector{s: s, descs: map[string]*prometheus.Desc{}}
	d := func(name, help string, labels ...string) {
		c.descs[name] = prometheus.NewDesc(name, help, labels, nil)
	}
	d("keyshift_fleet_instances", "SDK instances known to the controller, by freshness.", "state")
	d("keyshift_fleet_services", "Services with at least one active instance.")
	d("keyshift_fleet_rate", "Fleet-wide per-second rates from SDK telemetry, by kind (reads = logical SDK reads, local_hits = Redis reads avoided, redis_reads = reads that reached Redis).", "kind")
	d("keyshift_fleet_local_hit_ratio", "Share of SDK reads served from local caches.")
	d("keyshift_fleet_cache_entries", "Local cache entries summed over active instances.")
	d("keyshift_fleet_cache_memory_bytes", "Estimated local cache memory summed over active instances.")
	d("keyshift_fleet_cache_memory_limit_bytes", "Local cache memory budgets summed over active instances.")
	d("keyshift_fleet_admitted_keys", "Admitted keys summed over active instances.")
	d("keyshift_fleet_tracking_instances", "Active instances by client tracking state.", "state")
	d("keyshift_fleet_policy_instances", "Active instances by enforced policy version and source.", "version", "source")
	d("keyshift_fleet_global_keys", "Key fingerprints in the global ranking.")
	d("keyshift_fleet_telemetry_dropped", "Telemetry batches the SDKs report as dropped.")
	d("keyshift_fleet_warnings", "Active operational warnings (value: affected count).", "code", "severity")

	d("keyshift_fleet_service_instances", "Active instances per service.", "service")
	d("keyshift_fleet_service_rate", "Per-service per-second rates, by kind.", "service", "kind")
	d("keyshift_fleet_service_hit_ratio", "Share of a service's SDK reads served locally.", "service")
	d("keyshift_fleet_service_cache_memory_bytes", "Estimated local cache memory per service.", "service")
	d("keyshift_fleet_service_admitted_keys", "Admitted keys per service (summed over instances).", "service")
	d("keyshift_fleet_service_tracking_unhealthy", "Instances of a service with unhealthy client tracking.", "service")

	d("keyshift_fleet_hotkey_rate", "Per-second rates of the globally hottest keys, by kind (max_local = busiest single instance).", "key", "fp", "classification", "kind")
	d("keyshift_fleet_hotkey_instances", "Instances reporting a hot key, by admission state (reporting = all).", "key", "fp", "state")
	d("keyshift_fleet_hotkey_cached_hit_ratio", "Hit ratio of a hot key on the instances that cache it.", "key", "fp")

	d("keyshift_fleet_instance_rate", "Per-instance per-second rates (only with --metrics-instances).", "service", "sdk_instance", "kind")
	d("keyshift_fleet_instance_hit_ratio", "Per-instance local hit ratio (only with --metrics-instances).", "service", "sdk_instance")
	d("keyshift_fleet_instance_tracking_healthy", "1 when the instance reports healthy tracking (only with --metrics-instances).", "service", "sdk_instance")
	d("keyshift_fleet_instance_telemetry_age_seconds", "Seconds since the instance last reported (only with --metrics-instances).", "service", "sdk_instance")

	d("keyshift_controller_policy_version", "Current policy version per scope.", "scope")
	d("keyshift_controller_policy_rollout_percent", "Rollout percentage of the current policy per scope.", "scope")
	d("keyshift_controller_policy_mode", "Current policy mode per scope (1 for the active mode).", "scope", "mode")
	return c
}

func (c *fleetCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range c.descs {
		ch <- d
	}
}

func keyLabel(k KeyView) string {
	if k.Display != "" {
		return k.Display
	}
	return "fp:" + k.FP[:12]
}

// gaugeFunc emits one gauge sample.
type gaugeFunc func(name string, val float64, labels ...string)

func (c *fleetCollector) Collect(ch chan<- prometheus.Metric) {
	v := c.s.agg.Current()
	g := func(name string, val float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(c.descs[name], prometheus.GaugeValue, val, labels...)
	}
	collectOverview(g, v)
	collectServices(g, v.Services)
	collectHotKeys(g, v.Keys, c.s.cfg.MetricsTopKeys)
	if c.s.cfg.MetricsInstances {
		collectInstances(g, v.Instances)
	}
	collectPolicies(g, c.s.policies.All())
}

func collectOverview(g gaugeFunc, v *View) {
	ov := v.Overview
	g("keyshift_fleet_instances", float64(ov.ActiveInstances), "active")
	g("keyshift_fleet_instances", float64(ov.StaleInstances), "stale")
	g("keyshift_fleet_services", float64(ov.Services))
	for i, val := range ratesByKind(ov.Rates) {
		g("keyshift_fleet_rate", val, rateKinds[i])
	}
	g("keyshift_fleet_local_hit_ratio", ov.LocalHitRatio)
	g("keyshift_fleet_cache_entries", float64(ov.CacheEntries))
	g("keyshift_fleet_cache_memory_bytes", float64(ov.CacheMemoryBytes))
	g("keyshift_fleet_cache_memory_limit_bytes", float64(ov.CacheMemoryLimit))
	g("keyshift_fleet_admitted_keys", float64(ov.AdmittedKeys))
	for _, st := range []string{"healthy", "unhealthy", "unsupported", "disabled", "unknown"} {
		g("keyshift_fleet_tracking_instances", float64(ov.Tracking[st]), st)
	}
	policyCount := map[[2]string]int{}
	for _, iv := range v.Instances {
		if iv.Fresh {
			policyCount[[2]string{strconv.FormatInt(iv.PolicyVersion, 10), iv.PolicySource}]++
		}
	}
	for k, n := range policyCount {
		g("keyshift_fleet_policy_instances", float64(n), k[0], k[1])
	}
	g("keyshift_fleet_global_keys", float64(ov.GlobalKeys))
	g("keyshift_fleet_telemetry_dropped", float64(ov.TelemetryDropped))
	for _, w := range ov.Warnings {
		g("keyshift_fleet_warnings", float64(max(w.Count, 1)), w.Code, w.Severity)
	}
}

func collectServices(g gaugeFunc, services []ServiceView) {
	for _, sv := range services {
		g("keyshift_fleet_service_instances", float64(sv.Instances), sv.Name)
		for i, val := range ratesByKind(sv.Rates) {
			g("keyshift_fleet_service_rate", val, sv.Name, rateKinds[i])
		}
		g("keyshift_fleet_service_hit_ratio", sv.HitRatio, sv.Name)
		g("keyshift_fleet_service_cache_memory_bytes", float64(sv.CacheMemoryBytes), sv.Name)
		g("keyshift_fleet_service_admitted_keys", float64(sv.AdmittedKeys), sv.Name)
		g("keyshift_fleet_service_tracking_unhealthy", float64(sv.TrackingUnhealthy), sv.Name)
	}
}

// collectHotKeys exports the top hottest keys. Instance-scoped fingerprints
// can repeat across instances; only the first (hottest) is exported so that
// label sets stay unique.
func collectHotKeys(g gaugeFunc, keys []KeyView, top int) {
	seen := map[string]bool{}
	for _, k := range keys[:min(len(keys), max(top, 0))] {
		name, fp := keyLabel(k), k.FP
		if seen[fp] {
			continue
		}
		seen[fp] = true
		for _, kv := range []struct {
			kind string
			val  float64
		}{{"reads", k.ReadRate}, {"writes", k.WriteRate}, {"saved", k.SavedRate}, {"fills", k.FillRate},
			{"invalidations", k.InvalidationRate}, {"max_local", k.MaxLocalRate}} {
			g("keyshift_fleet_hotkey_rate", kv.val, name, fp, k.Classification, kv.kind)
		}
		g("keyshift_fleet_hotkey_instances", float64(k.Instances), name, fp, "reporting")
		for _, st := range []string{"admitted", "candidate", "cold", "cooldown", "bypassed"} {
			g("keyshift_fleet_hotkey_instances", float64(k.States[st]), name, fp, st)
		}
		g("keyshift_fleet_hotkey_cached_hit_ratio", k.CachedHitRatio, name, fp)
	}
}

func collectInstances(g gaugeFunc, instances []InstanceView) {
	for _, iv := range instances {
		for i, val := range ratesByKind(iv.Rates) {
			g("keyshift_fleet_instance_rate", val, iv.Service, iv.Instance, rateKinds[i])
		}
		g("keyshift_fleet_instance_hit_ratio", iv.HitRatio, iv.Service, iv.Instance)
		healthy := 0.0
		if iv.Tracking.State == "healthy" {
			healthy = 1
		}
		g("keyshift_fleet_instance_tracking_healthy", healthy, iv.Service, iv.Instance)
		g("keyshift_fleet_instance_telemetry_age_seconds", iv.AgeSeconds, iv.Service, iv.Instance)
	}
}

func collectPolicies(g gaugeFunc, envs map[string]*policy.Envelope) {
	for scope, env := range envs {
		g("keyshift_controller_policy_version", float64(env.Current.Version), scope)
		g("keyshift_controller_policy_rollout_percent", float64(env.RolloutPercent), scope)
		for _, m := range []string{"adaptive", "explicit", "off"} {
			val := 0.0
			if string(env.Current.Mode) == m {
				val = 1
			}
			g("keyshift_controller_policy_mode", val, scope, m)
		}
	}
}
