# Consistency contract

**KeyShift does not provide strong consistency or linearizability.** Redis client tracking delivers invalidations asynchronously. A value cached by one instance can be served after another client has changed it, until the invalidation reaches that instance.

## What KeyShift guarantees

1. **Writes always go to Redis.** `Set` and `Del` are ordinary Redis commands; KeyShift never acknowledges a write locally.
2. **Read-your-writes within one instance** for writes made through the same `keyshift.Client`: the local entry is dropped after the write returns, and a cache fill that was already in flight when the write completed is not cached (go-redis: the fill is rejected; rueidis: the entry is dropped again right after the fill). This race was found by the benchmark's stale-read detector (96 stale reads out of ~1.7M in the `local-hot` scenario before the fix) and is covered by `TestGatedCacheRejectsFillOverlappingLocalWrite`.
3. **Invalidation on external change** through Redis client tracking: `SET`, `DEL`, expiry, `FLUSHALL`, and tracking-table eviction.
4. **No serving after tracking loss**: when the tracked connection closes, the client library evicts its entries; when tracking cannot be confirmed (probe failure, timeout, transport error), KeyShift suspends caching and flushes the local cache.
5. **Hard staleness bound**: no entry is served more than `max_staleness` (default 3 s) after it was fetched, even if every other mechanism fails.
6. **Redis errors are not hidden**: when Redis is unavailable, `Get` returns the Redis error. KeyShift never serves cached data *because* Redis is down. Cache hits that happen before KeyShift notices an outage are bounded by items 4 and 5.

## What it does not guarantee

- Monotonic reads across `Get` and `GetFresh`: a `GetFresh` can return a newer value than a later cached `Get`.
- Ordering between keys: invalidations for different keys may be applied in any order relative to each other.
- Bounded staleness smaller than the invalidation delivery time. Typical delivery is sub-millisecond to a few milliseconds on a healthy connection. Under silent partitions, the bound is `HealthInterval + HealthTimeout` (≈ 1.6 s by default), and ultimately `max_staleness`.

## Measured behavior

From the integration and benchmark suites (single machine, Redis 8.6):

| Scenario | Result |
|---|---|
| concurrent writer (1 write/ms) and 8 readers on a cached key | maximum version lag 1 write; converged after writes stopped |
| write-heavy benchmark, conventional CSC (all keys cached) | 4.4% (closed loop) and 6.9% (open loop) of reads stale, up to 8.7 ms old; see docs/benchmarks.md |
| write-heavy benchmark, KeyShift | write-heavy keys not admitted; 0 stale reads |
| all benchmark scenarios, KeyShift | 8 stale reads in total (`local-hot`, `startup-surge`), caused by writes from other instances, at most 0.9 ms old |
| tracked connection killed, then external write | stale for 0–1.8 ms |
| silent partition (test settings 100 ms + 100 ms) | stale for ~140–175 ms, then direct reads only |
| Redis restart without persistence | pre-restart value never served |

## Data that must not be cached

Use deny patterns (local config or controller policy), `explicit` mode, or `GetFresh` for:

- financial balances and ledgers;
- distributed lock state and fencing tokens;
- rate-limit counters and quotas;
- inventory or seat counts that require strict freshness;
- authorization, session revocation, and other security-sensitive state;
- any value read to decide a conditional write.

KeyShift only caches `GET` on string keys. Counters updated with `INCR` are commonly also *read* with `GET`, so deny them explicitly; adaptive admission will usually reject them anyway when this instance writes them, but it cannot see writes from other services until after admission.

## Controller availability is separate

The controller distributes policies and collects telemetry. If it is unavailable, the SDK keeps its last valid policy until `valid_for_seconds` expires, then reverts to its local policy. Redis access is unaffected (`TestControllerOutageDoesNotAffectRedisAccess`).
