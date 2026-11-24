# How KeyShift works

This guide follows a key through detection, admission, caching, and invalidation in a single application instance. The optional controller provides fleet visibility; it does not sit on the Redis request path.

## 1. Every read is observed

When `client.Get(ctx, "product:123")` runs, the SDK first checks whether the key is **admitted**. If it is not, the read goes directly to Redis. The detector records the access and the returned value size in a bounded streaming summary.

Admitted-key reads are counted separately and folded into the detector at rotation. A key therefore does not appear cold simply because its reads are served locally.

## 2. Each window, candidates are scored

Every `Detection.Window` (default 10 s) the admission engine closes the window. For each hot key it estimates:

| Signal | Source |
|---|---|
| read rate | detector (decayed, conservative lower bound) or exact counters for admitted keys, EWMA-smoothed |
| write rate | writes issued by this instance (`Set`, `Del`) |
| fill rate, invalidation rate | the local cache, for admitted keys |
| value size | last observed value |

For an uncached key, the engine estimates fills from the local write rate and the `max_staleness` refresh interval. Its predicted hit ratio is `1 - min(read_rate, write_rate + 1/max_staleness) / read_rate`. External writes are not yet visible to this estimate.

## 3. Admission with hysteresis

A key becomes a **candidate** when its read rate and predicted hit ratio meet `min_read_rate` and `min_hit_ratio`. After `min_observation_windows` consecutive qualifying windows, it can be admitted if it fits the budget. The engine favors benefit per byte and gives existing admissions a stability bonus. Pins skip the observation wait, while deny rules, oversized values, `off` mode, and unhealthy tracking block admission.

The engine publishes an immutable admitted-key set. Subsequent reads of those keys use the tracked cache path.

## 4. The first cached read registers tracking

The first cached read normally misses and fetches the value over a tracking-enabled connection. For rueidis, the client sends `CLIENT CACHING YES` before `GET`. Redis records the tracked read, and later reads can be served from process memory.

## 5. Invalidation

When a tracked key changes, is deleted, or expires, Redis sends an invalidation to the relevant tracking connections. The client library removes the cached entry, and a later read fetches it again. KeyShift records invalidations for admission and telemetry.

Writes made through this KeyShift client also drop the local entry after the Redis command. For writes through another local path, call `NotifyWrite` after the write. Concurrent fills have a driver-specific caveat; see the [consistency contract](consistency.md).

## 6. Observed feedback

Once a key is cached and has reads in the window, the engine uses **observed** numbers: `hit_ratio = 1 - fills / reads`. If the ratio falls below `exit_hit_ratio` with at least 20 reads in the window, the key enters **cooldown** (30 s, doubling per strike up to 16×) and its entry is dropped. A key whose read rate falls below half of `min_read_rate` for `exit_windows` windows returns to **cold**.

This feedback matters for external writers: a key can appear attractive before admission, then prove expensive to cache once its fills and invalidations become visible.

## 7. When tracking is uncertain

The tracking health monitor periodically probes the tracked path (every second by default, with jitter). If a probe fails or times out, or a cached read encounters a transport error, KeyShift:

1. empties the admitted set, so the next read goes to Redis;
2. flushes the local cache;
3. reports `tracking_state="unhealthy"` in metrics, stats, and telemetry.

Caching resumes only after consecutive successful probes, starting with an empty cache.

## 8. Fleet view (optional)

When telemetry is enabled, the SDK sends a compact batch approximately every 30 seconds, with jitter. It includes window totals, cache residency, tracking state, and up to 20 candidates identified by **HMAC fingerprints** by default. The controller aggregates recent batches to show global and busiest-instance rates, services, and admission across instances.

The controller can push a **policy** (for example, deny `lock:*` for checkout-api or roll out explicit mode to 10% of instances). SDKs validate policies and enforce local budget and deny-list limits. Policies can still change admission thresholds and add pins. If the controller is unreachable for longer than `valid_for_seconds`, SDKs revert to their local policy.

## Worked example

| Key | Pod reads/s | Writes | Outcome |
|---|---|---|---|
| `product:123` | 2,000 | none | admitted after 2 windows; ~99.9% local hits (one refetch per 3 s `max_staleness`) |
| `stock:7` | 15 | 25/s by another service | admitted on probation (writes are invisible locally), observed hit ratio ≈ 0, cooldown with growing backoff |
| `counter:views` | 500 | 400/s by this pod | predicted hit ratio < 0.6, never admitted |
| `config:flags` | 0.5 | rare | below `min_read_rate`: not admitted unless pinned |
| `balance:42` | 900 | rare | matches a deny pattern: bypassed |
| `report:2026` | 50 | none, 2 MiB value | exceeds `max_entry_bytes`: bypassed |
