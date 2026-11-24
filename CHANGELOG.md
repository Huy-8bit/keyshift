# Changelog

All notable changes to this project are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- `Client.MGet`: batch reads. Admitted keys are served from the local cache; the other keys are fetched in one pipeline (one round trip per node, cross-slot keys allowed on Redis Cluster). Every key counts toward detection.
- `Client.NotifyWrite`: declares writes made outside the client (underlying client, Lua scripts, `INCR`, `EXPIRE`, …). Local entries are dropped at once (read-your-writes) and the writes feed admission. Reported as `Stats.NotifiedWrites` and `keyshift_requests_total{op="notify_write"}`.
- Valkey support verified: the SDK and adapter integration suites pass on Valkey 8.1 and 9.0. `make test-compat` runs them against Valkey, also in CI.
- `keyshift doctor` and `keyshift inspect hotkeys` identify Valkey servers (product, version, mode).
- Cache-hit benchmarks against real Redis: `BenchmarkCacheHit` (SDK, both drivers) and `BenchmarkGoRedisCacheHit` (go-redis cache with and without the KeyShift wrapper).
- CLI tests: controller queries and the policy lifecycle against an in-process controller; `doctor` and `inspect key` against Redis.

### Changed

- Faster cache hits: adapters return strings instead of byte slices, which removes two copies per `Get` (16 KiB values: 12.7 µs → 6.5–8.3 µs per hit on the development machine).
- Latency histograms are recorded only when `Metrics.Registerer` is set; `GetFresh` and each `MGet` pipeline now contribute samples to the `direct` path.
- Transport errors on the cache path are classified by error type (`net.Error`, `io.EOF`, connection reset/refused, broken pipe, deadline exceeded, client closed or pool timeout) instead of by message text.
- Internal refactoring without behavior changes: the SDK is split by responsibility (`client.go`, `commands.go`, `policy.go`, `telemetry.go`, `connect.go`); the admission engine's window rotation and state machine are split into phases (`rotate.go`, `evaluate.go`); the controller's aggregation uses a view builder (`build.go`); the benchmark runner separates load generation, measurement, and summary; `keyshift doctor` is a set of independent checks.

### Fixed

- The tracking monitor could report a false probe failure (and delay recovery) when a check started right after the previous one completed. This made two tests flaky in CI.
- `keyshift inspect key` printed `-1ns` as the TTL of keys without expiry.

## [0.1.0] - 2026-11-21

First public release.

### Added

- Go SDK (`pkg/keyshift`): `New`, `NewFromGoRedis`, `NewFromRueidis`; `Get`, `GetBytes`, `GetFresh`, `Set`, `SetString`, `Del`, `Explain`, `Stats`, `ApplyPolicy`, `ClearPolicy`, `FlushLocal`, Prometheus collector.
- Sharded Space-Saving hot-key detector with per-window decay and sampling.
- Adaptive admission engine: predicted and observed hit ratio, write and invalidation awareness, hysteresis, EWMA and decayed-count smoothing, exponential cooldown, entry and memory budgets, pin and deny patterns, deny-by-fingerprint, explicit and off modes.
- go-redis v9.23 adapter (dedicated tracked client, admitted keys only) and rueidis v1.0.80 adapter (OPTIN, Redis Cluster, bounded KeyShift store).
- Tracking health monitor that suspends caching and flushes on probe failure or timeout.
- Controller: authenticated, rate-limited, idempotent telemetry ingestion; bounded aggregation; global and per-service rankings; versioned policies with staged rollout, rollback, history, and persistence; REST API; Prometheus fleet metrics.
- Fleet metrics for Prometheus (instances, workload, per-service breakdown, global hot keys, cache efficiency, policies, warnings) and Grafana dashboards "KeyShift Fleet" and "KeyShift SDK".
- CLI: `doctor`, `inspect` (HOTKEYS, key, fingerprint), `top`, `stats`, `instances`, `policy`, `bench`, `simulate`.
- Redis 8.6 HOTKEYS diagnostic sessions with capability, permission, and conflict handling.
- KeyShift Lab (`lab/`): Docker Compose stack with Prometheus and Grafana, an automated 13-step scenario runner with reports, fault-injecting TCP proxy, Redis restart and cluster slot-migration tests, scenario benchmark harness (closed and open loop, stale-read measurement), 5,000-instance logical controller simulation.
- Documentation, ADRs, CI (lint, race tests on Go 1.26/1.27, integration, fuzz smoke, build, govulncheck), release configuration.
