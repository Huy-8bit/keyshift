# ADR 0002: Tracking strategy and client adapters

- Status: accepted
- Date: 2026-11-20

## Context

The SDK needs Redis server-assisted invalidation without implementing RESP or the tracking protocol itself. The design should limit server tracking to cache-path reads, avoid BCAST fan-out, support go-redis adoption, and provide a Redis Cluster option.

Verified facts (source inspection and integration tests):

1. go-redis v9.23 caches every cacheable command once client-side caching is enabled, and enables default-mode `CLIENT TRACKING` on every pooled connection. It exposes a pluggable `redis.Cache` and `redis.NewLocalCache`. It has no cluster client-side caching and no OPTIN support.
2. rueidis v1.0.80 supports `CLIENT TRACKING ON OPTIN` (`CLIENT CACHING YES` before each `DoCache`), Cluster, and a custom store via `NewCacheStoreFn` + `NewSimpleCacheAdapter`.
3. With rueidis's multiplexed connections, a local write does not guarantee that the invalidation for an entry cached on another connection is applied before the write returns.

## Decision

- **go-redis adapter**: two clients. The application client serves writes and non-admitted reads untracked. A dedicated client with RESP3 and an instrumented `redis.LocalCache` serves only admitted keys, so Redis tracks only admitted keys. The tracked client gets its own push processor and maintenance-notification config (go-redis writes derived state back into `Options`).
- **rueidis adapter**: OPTIN; `DoCache` for admitted keys, `Do` otherwise; a KeyShift bounded store installed through `NewCacheStoreFn` for global budgets, per-key `Forget` after local writes, and statistics. Commands are pinned to avoid a buffer-reuse race observed under connection cuts.
- **No custom invalidation listener, no REDIRECT connection, no BCAST.**
- **KeyShift health monitor** on top: probe `CLIENT TRACKINGINFO` with a timeout enforced outside the client; fail fast, recover slowly, flush on every transition.

## Rejected alternatives

- *Implementing OPTIN on go-redis with a REDIRECT connection*: requires per-connection redirect re-registration on reconnect, custom ordering logic for invalidations versus replies, and iteration over pooled connections, which go-redis does not expose. High risk; duplicates go-redis's work.
- *Custom `redis.Cache` gating admission inside go-redis*: admission works, but default-mode tracking would still register every read key on the server.
- *BCAST*: prefix broadcasts to thousands of pods.

## Consequences

- Server tracking cost scales with admitted keys (verified: 1 tracked key for 200 direct + 1 cached reads; 341 vs 32,885 tracked keys in the open-loop Zipf benchmark).
- go-redis users get caching only on standalone servers; cluster users should use rueidis.
- KeyShift depends on go-redis's experimental cache API and its namespace layout; both are pinned and covered by integration tests.
- rueidis drops an overlapping fill after it is stored, leaving a brief concurrent-read window. See the [consistency contract](../consistency.md).
