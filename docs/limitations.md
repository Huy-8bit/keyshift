# Known limitations

This list is maintained honestly; it is part of the release.

## Functional scope

- **Cached operations**: string reads through `Get`, `GetBytes`, and `MGet` are cached. `MGet` serves admitted keys from the local cache one by one (a miss is one round trip each) and fetches the other keys in one pipeline. Other read commands (`HGET`, `HGETALL`, `SMEMBERS`, …) are not routed through KeyShift; use the underlying client directly for them. Pipelines, transactions, scripts, and pub/sub are never cached.
- **go-redis and Redis Cluster**: go-redis v9.23 has no client-side caching for `ClusterClient`. With the go-redis driver on Cluster, KeyShift only detects and reports. Use the rueidis driver for cluster caching.
- **Writes through other paths**: writes made with the underlying client (`client.GoRedis()`, `client.Rueidis()`), another library, or another service are invalidated by Redis asynchronously. Writes through `keyshift.Client` (`Set`, `Del`) get immediate local read-your-writes; for other commands on the same instance (`INCR`, `EXPIRE`, Lua scripts, …), call `NotifyWrite(keys...)` after the write. Writes made by other instances or services always rely on server invalidation.
- **External write rates are invisible before admission**: a key written by other services is admitted on probation, measured, and then put into cooldown. Each probation costs a few fills and invalidations. Deny such patterns to avoid it.
- **Per-key fills under go-redis**: go-redis may serve concurrent waiters for the same miss from one fill; KeyShift counts the fill once, which is correct for Redis load but makes per-call hit attribution approximate.
- **`max_staleness` ceiling**: every entry is refetched at least once per `max_staleness` interval, which bounds the hit ratio of moderately hot keys.

## Consistency

- Eventually consistent; see [consistency.md](consistency.md). No linearizability, no monotonic reads.
- Local writes and in-flight fills: with go-redis a fill that overlaps a local write is rejected precisely. With rueidis (no reservation hook) the entry is dropped again right after such a fill, which leaves a window of microseconds in which another reader can hit it. Writes made outside `keyshift.Client` rely on server invalidation only.

## Library integration

- go-redis client-side caching is an **experimental** API in go-redis v9.23. KeyShift mirrors go-redis's internal cache-key namespace layout (`<db>\0<len>:<user>\0<key>`) to map invalidations back to application keys. This is covered by integration tests for the pinned version and may need updating when go-redis changes.
- **go-redis cache-hit cost**: KeyShift wraps go-redis's `LocalCache` to observe fills and invalidations per key and to reject fills that race a local write (read-your-writes). go-redis uses its zero-copy hit path and batched invalidation, and offers its opt-in miss coalescing and refresh-on-invalidate, only for its own `*LocalCache` type, so every hit through the wrapper copies the value once more. Measured on the development machine (parallel hits, 8 cores): 64 B values 272 → 341 ns, 16 KiB values 3.5 → 6.6–7.2 µs per hit (`BenchmarkGoRedisCacheHit`; see [engineering-notes.md](engineering-notes.md#measured-trade-off-the-go-redis-cache-wrapper)). The rueidis driver does not copy on hits (about 270 ns regardless of value size, `BenchmarkCacheHit`), so prefer it for large values on hot read paths.
- rueidis v1.0.80: under `-race`, a data race was observed inside rueidis when a connection is cut while commands are in flight (a pooled command buffer is reused while the pipe's background writer still reads it). KeyShift pins every command (`Completed.Pin`, a documented API), which avoids the shared buffer at the cost of one allocation per command. The underlying issue should be reported upstream.
- When the rueidis driver wraps a caller-supplied client that was not created with `OPTIN`, Redis tracks every key that client reads. `NewFromRueidis` always enables `OPTIN`.

## Controller

- Single replica; policies persist to local disk; aggregation state is rebuilt from telemetry after a restart (about one report interval).
- Static bearer tokens with three roles. No built-in SSO, per-user audit identity, or fine-grained authorization; use an authenticating proxy.
- The global view is the latest batch per instance (by default up to 30 s old), not a precise time series per key.

## Verification gaps

- **Scale**: the 5,000-instance figures come from a **logical simulation** (virtual instances uploading synthetic telemetry over real HTTP on one machine), not from 5,000 application pods. `deploy/kubernetes/scale-test.yaml` describes a distributed telemetry load test that has not been run as part of this release.
- **Benchmarks** were run on one developer laptop (Apple M1 Pro, Docker Desktop–style VM networking via OrbStack). Absolute numbers will differ on Linux servers with real networks; relative comparisons are more meaningful than absolute throughput.
- **Not tested**: Redis Sentinel/failover clients, Redis Enterprise, Valkey, managed cloud Redis, Redis behind proxies, TLS to Redis in integration tests (the option is passed through to the client libraries), and real cluster node failover with replicas (slot migration is tested; node loss is not).
- **Windows** builds of the CLI are produced by the release configuration but not tested.
