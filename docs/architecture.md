# Architecture

KeyShift has five parts. Only the SDK runs on the request path.

| Component | Location | Role | Required |
|---|---|---|---|
| Go SDK | `pkg/keyshift` + `internal/*` | detection, admission, cached reads, safety, metrics, telemetry | yes |
| Controller | `cmd/keyshift-controller`, `internal/controller` | telemetry ingestion, global aggregation, policies, API | no |
| CLI | `cmd/keyshift`, `internal/cli` | diagnostics, queries, policy management, benchmarks | no |
| Dashboard | `web/` (embedded into the controller) | operational UI over the controller API | no |
| Lab | `internal/bench`, `internal/simulate`, `test/`, `deploy/docker` | benchmarks, fault injection, scale simulation | no |

## Package map

```text
pkg/keyshift            public API: Client, Config, Stats, Decision, Policy
internal/detector       sharded Space-Saving heavy-hitter detector
internal/admission      per-window admission state machine and budgets
internal/policy         policy schema, validation, glob matching, merge rules
internal/rediscompat    go-redis and rueidis adapters (no protocol code)
internal/tracking       tracking health monitor (probe, fail-fast, recover)
internal/telemetry      wire schema, encode/decode with limits, reporter
internal/fingerprint    keyed HMAC key fingerprints
internal/observability  striped counters
internal/controller     registry, aggregator, policy store, HTTP API, auth
internal/hotkeys        Redis 8.6 HOTKEYS diagnostic sessions
internal/cli            keyshift command implementations
internal/bench          scenario benchmark harness
internal/simulate       large-scale logical telemetry simulation
internal/testutil       Redis test helpers, fault-injecting TCP proxy
web/                    React + TypeScript dashboard (Vite)
```

## SDK data flow

```text
Get(key)
  │
  ├─ engine.Lookup(key)            one atomic load + one map lookup in an immutable
  │                                 admitted-key snapshot; nil when tracking is unhealthy
  │
  ├─ admitted ─► backend.GetCached ─► client library's tracked cache
  │                                     hit:  served locally (no network)
  │                                     miss: Reserve → GET on a tracking connection
  │                                           → OnFill hook (per-key fill counter)
  │              KeyStats.AddRead     (atomic counter, folded into the detector later)
  │
  └─ otherwise ─► backend.GetDirect ─► application client (no tracking)
                 detector.Observe(key, size)   sharded lock, O(log capacity)

every Detection.Window:
  engine.Rotate
    1. harvest per-key counters of admitted keys (reads, fills, invalidations)
    2. rotate detectors (reads, writes); take top (TopK + admitted) candidates
    3. evaluate each record (state machine, see adaptive-admission.md)
    4. enforce budgets (pinned first, then benefit density, incumbents preferred)
    5. publish a new immutable admitted set; Forget() demoted keys
  telemetry aggregate += window report   (fingerprints computed here, not per request)
```

The request path never performs remote calls, never takes a global lock, and allocates nothing beyond what the Redis client allocates.

## Driver adapters

### go-redis (default)

go-redis v9.23 caches every cacheable command once client-side caching is enabled, and enables default-mode `CLIENT TRACKING` on every pooled connection. KeyShift therefore uses **two clients**:

- the **application client** (yours, or created from `RedisConfig`) for writes and every non-admitted read. It is never switched to tracking mode;
- a small **dedicated tracked client** created from the application client's options with RESP3 and a KeyShift-instrumented `redis.LocalCache` (`ClientSideCache`), used only for admitted keys.

Because Redis tracks only keys read on tracking-enabled connections, the server tracking table and invalidation traffic cover only admitted keys. This gives the overhead profile of `OPTIN` tracking without reimplementing the tracking protocol (verified by `TestGoRedisServerTracksOnlyCachedKeys`).

All correctness-critical behavior stays inside go-redis: reservations and in-flight invalidation races, eviction of entries owned by a connection that loses tracking, the background drainer for idle connections, and the `MaxStaleness` backstop. The KeyShift wrapper only counts and forwards.

go-redis `ClusterClient` has no client-side caching in v9.23. With the go-redis driver on Cluster, KeyShift still detects hot keys, reports telemetry, and serves every read directly from Redis (capability state `unsupported`).

### rueidis

rueidis implements `CLIENT TRACKING ON OPTIN`: it sends `CLIENT CACHING YES` before each `DoCache` command. KeyShift calls `DoCache` (with `ToStaticTTL`, so no MULTI/PTTL/EXEC wrapper; freshness comes from invalidations bounded by `max_staleness`) for admitted keys and `Do` for everything else. KeyShift installs its own bounded cache store through rueidis's documented `NewCacheStoreFn` / `SimpleCache` adapter. rueidis keeps single-flight and connection-close semantics, and KeyShift adds a global memory and entry budget across connections, per-key forgetting after local writes, and statistics. Cluster redirects (MOVED/ASK) are handled by rueidis.

## Safety layers

1. **Client library tracking**: invalidation on write and expiry, flush on connection loss.
2. **KeyShift health monitor**: probes `CLIENT TRACKINGINFO` on the tracked path. A single failure, a timeout (enforced by the monitor even if the client ignores context deadlines), or a cache-path transport error suspends caching immediately and flushes the local cache. Recovery requires consecutive successful probes and starts from an empty cache.
3. **`max_staleness`**: a hard upper bound on how long any entry is served without a refetch.
4. **Policy**: deny patterns, deny fingerprints, `explicit` mode, `off` mode, and `GetFresh` for authoritative reads.

## Controller

```text
POST /v1/telemetry ─► auth(ingest) ─► global token bucket ─► size limits ─► decode/validate
                      ─► registry.Ingest (bounded; per-instance seq dedup; per-instance min interval)
                      ◄─ response: accepted, policy envelope if the instance's selected version differs

every AggregateInterval:
  registry.Sweep (expire instances) ─► aggregator.Build (latest batch per fresh instance)
    → overview, services, instances, global key ranking (bounded), cache view, warnings,
      time series ring buffers
```

- **State is in memory** except policies, which persist to `--data-dir` with atomic file replacement. After a restart the global view rebuilds from telemetry within one report interval.
- **Aggregation uses the latest batch of each fresh instance**. Re-sending a batch cannot double count, and an instance that disappears drops out after `--fresh-for`.
- **Policies** are versioned envelopes `{current, previous, rollout_percent}`. Each instance deterministically selects `current` or `previous` by hashing its identity (`policy.RolloutBucket`), so a staged rollout is stable across restarts.

See [scaling.md](scaling.md) for sizing and [security-model.md](security-model.md) for the trust model.

## Architecture decision records

- [ADR 0001: hot-key detection algorithm](adr/0001-hotkey-detection-algorithm.md)
- [ADR 0002: tracking strategy and client adapters](adr/0002-tracking-strategy.md)
- [ADR 0003: controller state and aggregation semantics](adr/0003-controller-aggregation.md)
