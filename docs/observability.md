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

## Dashboard

The dashboard (served by the controller at `/`) shows the overview with warnings, tracking health, policy versions, and the global workload over time; the hot-key ranking with local vs global rates, service distribution, admission states, and trends; cache efficiency with savings, memory, invalidations, churn, beneficial keys, and churning keys; policies with history, staged rollout, rollback, and disable; and instances with identity, SDK status, tracking health, telemetry freshness, and cache usage. It handles loading, empty, error, unauthorized, and disconnected states.
