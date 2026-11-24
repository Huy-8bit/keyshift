# Consistency contract

**KeyShift does not provide strong consistency or linearizability.** Redis client tracking delivers invalidations asynchronously. A value cached by one instance can be served after another client has changed it, until the invalidation reaches that instance.

## Write and invalidation behavior

1. **Writes always go to Redis.** `Set` and `Del` are ordinary Redis commands; KeyShift never acknowledges a write locally.
2. **Local writes invalidate local entries.** `Set` and `Del` drop the affected entries after the Redis command. For writes through another path, call `NotifyWrite(keys...)` after the write; without it, the instance relies on asynchronous server invalidation. The fill guard also handles fills that overlap a local write: go-redis rejects the fill, while rueidis drops the entry after the fill. With rueidis, another concurrent reader can briefly hit the entry between the fill and that drop. Do not rely on cached reads for strict read-your-writes under concurrent fills; use `GetFresh` when freshness is required.
3. **Invalidation on external change** through Redis client tracking: `SET`, `DEL`, expiry, `FLUSHALL`, and tracking-table eviction.
4. **Tracking loss response**: when the client library detects a tracked connection closing, it evicts its entries. When a probe fails or times out, or the cache path reports a transport error, KeyShift suspends caching and flushes the local cache.
5. **Maximum entry age**: `max_staleness` (default 3 s) limits how long an entry can be served without a refetch. It is not a guarantee of linearizability or a bound on the age of the underlying data.
6. **Redis errors are not hidden on the direct path**: a direct read returns Redis errors to the caller. A cache hit can still succeed before KeyShift detects an outage; KeyShift does not deliberately serve stale data as an outage fallback. Entry age and tracking health are separate safeguards.

## What it does not guarantee

- Monotonic reads across `Get` and `GetFresh`: a `GetFresh` can return a newer value than a later cached `Get`.
- Ordering between keys: invalidations for different keys may be applied in any order relative to each other.
- A fixed invalidation delivery time. A silent partition may remain undetected until a probe fails; probes have ±10% interval jitter and are subject to scheduling and network behavior. `max_staleness` provides the separate entry-age backstop.

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
