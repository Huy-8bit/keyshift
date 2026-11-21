# Observability

## SDK metrics

Register the collector with `Config.Metrics.Registerer`. Every metric has a constant `service` label. **No metric has a key label.** Counters are read at scrape time; the request path pays only for the optional latency histogram.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `keyshift_requests_total` | counter | `op` (get, set, del), `path` (cache, direct, bypass) | logical requests |
| `keyshift_cache_hits_total` | counter | | reads served locally (Redis reads avoided) |
| `keyshift_cache_misses_total` | counter | | cache-path reads fetched from Redis |
| `keyshift_cache_bypasses_total` | counter | | `GetFresh` calls |
| `keyshift_cache_fallbacks_total` | counter | | cache-path reads retried on the direct path after transport errors |
| `keyshift_cache_entries` | gauge | | resident entries (−1 if unknown) |
| `keyshift_cache_memory_bytes` | gauge | | estimated entry memory (not process memory) |
| `keyshift_cache_evictions_total` | counter | | capacity evictions and `max_staleness` expiries |
| `keyshift_cache_admitted_keys` | gauge | | keys currently admitted |
| `keyshift_hotkey_candidates` | gauge | `state` (candidate, admitted, cooldown) | keys under observation |
| `keyshift_admission_decisions_total` | counter | `decision` (admit, demote, cooldown, budget_reject) | admission transitions |
| `keyshift_invalidation_events_total` | counter | | server invalidations for cached keys |
| `keyshift_tracking_errors_total` | counter | | probe failures and cache-path transport errors |
| `keyshift_tracking_healthy` | gauge | | 1 when caching may be used |
| `keyshift_tracking_state` | gauge | `state` (unknown, healthy, unhealthy, unsupported, disabled) | one-hot state |
| `keyshift_policy_version` | gauge | | active policy version (0 = local) |
| `keyshift_policy_rejected_total` | counter | | invalid remote policies rejected |
| `keyshift_telemetry_dropped_total` | counter | | batches dropped (queue full, retries exhausted, rejected) |
| `keyshift_controller_requests_total` | counter | `result` (sent, failed) | telemetry uploads |
| `keyshift_detector_observations_total` | counter | | reads observed by the detector |
| `keyshift_detector_coverage_ratio` | gauge | | share of recorded reads in the last window's top keys |
| `keyshift_errors_total` | counter | | Redis errors returned to the application |
| `keyshift_read_duration_seconds` | histogram | `path` (cache, direct) | `Get` latency |
| `keyshift_window_rotation_duration_seconds` | histogram | | admission evaluation time |

Useful queries:

```promql
# Redis reads avoided per second, per service
sum by (service) (rate(keyshift_cache_hits_total[5m]))

# Local hit ratio of all reads
sum(rate(keyshift_cache_hits_total[5m])) / sum(rate(keyshift_requests_total{op="get"}[5m]))

# Instances with caching suspended
count(keyshift_tracking_healthy == 0) by (service)

# Admission churn
sum by (service) (rate(keyshift_admission_decisions_total{decision=~"admit|demote"}[5m]))
```

Suggested alerts: `keyshift_tracking_healthy == 0` for 5 minutes (caching suspended: Redis load rises), `rate(keyshift_policy_rejected_total[15m]) > 0`, and a rising `keyshift_telemetry_dropped_total`.

## Programmatic access

```go
s := client.Stats()        // counters, residency, tracking status, capabilities, policy
d := client.Explain(key)   // per-key decision and reason
```

`Stats.Tracking` includes the state, reason, last check, consecutive failures, and transition count. `Stats.Capabilities` reports the driver, topology, whether caching is available, the tracking mode, and why caching is unavailable when it is.

## Logs

The SDK logs through `Config.Logger` (`log/slog`) at state transitions only: startup, caching unavailable, tracking healthy or unhealthy, policy applied, rejected, or expired, and telemetry rejection. Nothing is logged per request. Keys, values, and passwords are never logged.

## Controller endpoints and metrics

| Endpoint | Auth | Purpose |
|---|---|---|
| `GET /healthz` | none | liveness |
| `GET /readyz` | none | readiness (policy store loaded) |
| `GET /metrics` | none by default (`--metrics-public=false` requires viewer) | Prometheus |

| Metric | Labels |
|---|---|
| `keyshift_controller_requests_total` | `route`, `code` |
| `keyshift_controller_ingest_total` | `result` (accepted, duplicate, invalid, too_large, rate_limited, instance_rate_limited, capacity) |
| `keyshift_controller_aggregation_duration_seconds` | |
| `keyshift_controller_instances`, `keyshift_controller_active_instances` | |
| `keyshift_controller_global_keys` | |
| `keyshift_controller_ingest_bytes_total` | |
| Go runtime and process collectors | |

## Fleet metrics (controller)

The controller exports the aggregated fleet view on `/metrics`, so Prometheus keeps the history and Grafana renders it. Values come from the latest aggregation (every `--aggregate-interval`).

| Metric | Labels | Meaning |
|---|---|---|
| `keyshift_fleet_instances` | `state` (active, stale) | SDK instances known to the controller |
| `keyshift_fleet_services` | | services with active instances |
| `keyshift_fleet_rate` | `kind` (reads, local_hits, redis_reads, writes, invalidations, evictions, admissions, demotions) | fleet-wide per-second rates |
| `keyshift_fleet_local_hit_ratio` | | share of SDK reads served locally |
| `keyshift_fleet_cache_entries`, `_cache_memory_bytes`, `_cache_memory_limit_bytes`, `_admitted_keys` | | local caches summed over active instances |
| `keyshift_fleet_tracking_instances` | `state` | active instances by tracking state |
| `keyshift_fleet_policy_instances` | `version`, `source` | active instances by enforced policy |
| `keyshift_fleet_warnings` | `code`, `severity` | active operational warnings (value: affected count) |
| `keyshift_fleet_service_*` | `service` (+ `kind` for `_rate`) | instances, rates, hit ratio, cache memory, admitted keys, unhealthy instances per service |
| `keyshift_fleet_hotkey_rate` | `key`, `fp`, `classification`, `kind` (reads, writes, saved, fills, invalidations, max_local) | the `--metrics-top-keys` hottest keys (default 20) |
| `keyshift_fleet_hotkey_instances` | `key`, `fp`, `state` (reporting, admitted, candidate, cold, cooldown, bypassed) | instances reporting each hot key, by admission state |
| `keyshift_fleet_hotkey_cached_hit_ratio` | `key`, `fp` | hit ratio of a hot key where it is cached |
| `keyshift_fleet_instance_*` | `service`, `sdk_instance` | per-instance rates, hit ratio, tracking health, telemetry age (only with `--metrics-instances`) |
| `keyshift_controller_policy_version`, `_rollout_percent`, `_mode` | `scope` (+ `mode`) | current policies |

**Cardinality**: the `key` label is the name allowed by the SDK's `DisplayKeyPatterns`, or `fp:` plus the first 12 hex digits of the fingerprint; it never contains other raw key names. Hot-key series are bounded by `--metrics-top-keys`, service series by `--max-services`, and per-instance series are opt-in because they grow with the fleet (5,000 instances × 11 series at the target scale).

## Grafana dashboards

`lab/grafana/dashboards/` contains two dashboards, provisioned automatically in the lab:

- **KeyShift Fleet** (controller metrics): active instances, hit ratio, Redis reads avoided, unhealthy tracking; global workload over time; per-service charts and table; the global hot-key table (global vs busiest-instance rate, savings, writes, on how many instances each key is cached); cache efficiency (invalidations, evictions, admission churn, memory utilization); SDK instances; policies, policy versions in use, warnings; controller ingestion, aggregation latency, and memory.
- **KeyShift SDK** (metrics scraped from services that embed the SDK, with a `service` selector): hit ratio, Redis reads avoided, admitted keys, cache memory, tracking state, policy version; requests by path; hits, misses, bypasses, fallbacks; read latency p50/p99 by path; admission decisions; invalidations and evictions; residency; candidates; detector coverage; telemetry.

To use them outside the lab, import the JSON files into Grafana and point the `prometheus` datasource UID at your Prometheus.
