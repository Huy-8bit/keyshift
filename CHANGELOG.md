# Changelog

All notable changes to this project are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses [Semantic Versioning](https://semver.org/).

## [0.1.0] - 2026-11-21

First public release.

### Added

- Go SDK (`pkg/keyshift`): `New`, `NewFromGoRedis`, `NewFromRueidis`; `Get`, `GetBytes`, `GetFresh`, `Set`, `SetString`, `Del`, `Explain`, `Stats`, `ApplyPolicy`, `ClearPolicy`, `FlushLocal`, Prometheus collector.
- Sharded Space-Saving hot-key detector with per-window decay and sampling.
- Adaptive admission engine: predicted and observed hit ratio, write and invalidation awareness, hysteresis, EWMA and decayed-count smoothing, exponential cooldown, entry and memory budgets, pin and deny patterns, deny-by-fingerprint, explicit and off modes.
- go-redis v9.23 adapter (dedicated tracked client, admitted keys only) and rueidis v1.0.80 adapter (OPTIN, Redis Cluster, bounded KeyShift store).
- Tracking health monitor that suspends caching and flushes on probe failure or timeout.
- Controller: authenticated, rate-limited, idempotent telemetry ingestion; bounded aggregation; global and per-service rankings; versioned policies with staged rollout, rollback, history, and persistence; REST API; embedded dashboard.
- React and TypeScript dashboard: overview, hot keys, cache efficiency, policies, instances.
- CLI: `doctor`, `inspect` (HOTKEYS, key, fingerprint), `top`, `stats`, `instances`, `policy`, `bench`, `simulate`.
- Redis 8.6 HOTKEYS diagnostic sessions with capability, permission, and conflict handling.
- KeyShift Lab: Docker Compose stack, fault-injecting TCP proxy, Redis restart and cluster slot-migration tests, scenario benchmark harness (closed and open loop, stale-read measurement), 5,000-instance logical controller simulation.
- Documentation, ADRs, CI (lint, race tests on Go 1.26/1.27, integration, fuzz smoke, build, govulncheck, npm audit), release configuration.
