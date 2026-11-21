# How KeyShift works

This page follows one key through its lifecycle in one application instance.

## 1. Every read is observed

`client.Get(ctx, "product:123")` first checks whether the key is currently **admitted**. Most keys are not, so the read goes straight to Redis through the application's normal client. The access is recorded by the **detector**, a bounded streaming top-K structure, together with the size of the returned value.

Reads served from the local cache are counted too, through a per-key atomic counter, so a cached key does not look cold just because it stopped reaching Redis.

## 2. Each window, candidates are scored

Every `Detection.Window` (default 10 s) the admission engine closes the window. For each hot key it estimates:

| Signal | Source |
|---|---|
| read rate | detector (decayed, conservative lower bound) or exact counters for admitted keys, EWMA-smoothed |
| write rate | writes issued by this instance (`Set`, `Del`) |
| fill rate, invalidation rate | the local cache, for admitted keys |
| value size | last observed value |

For a key that is not cached yet, the engine **predicts** how often a cached copy would have to be refetched: once per local write, plus once per `max_staleness` interval. The predicted hit ratio is `1 - min(read_rate, write_rate + 1/max_staleness) / read_rate`.

## 3. Admission with hysteresis

A key becomes a **candidate** when its read rate and predicted hit ratio clear the thresholds (`min_read_rate`, `min_hit_ratio`). It is **admitted** after it qualifies for `min_observation_windows` consecutive windows, if it fits in the entry and memory budget. Keys with the highest benefit per byte win, and keys already admitted get a stability bonus. Pinned keys skip the wait. Denied keys, oversized values, `off` mode, and unhealthy tracking block admission.

The engine publishes an immutable admitted-key set. From the next request on, reads of admitted keys go through the tracked cache.

## 4. The first cached read registers tracking

The first read of an admitted key misses the local cache. The client library sends `GET` on a tracking-enabled connection (`CLIENT CACHING YES` + `GET` for rueidis), stores the reply, and Redis remembers that this connection holds the key. Later reads are served from process memory.

## 5. Invalidation

When anyone writes or deletes the key, or it expires, Redis pushes an `invalidate` message to every connection that tracks it. The client library drops the entry, and the next read refetches it. KeyShift counts the invalidation for the key.

Writes made through this KeyShift client also drop the local entry immediately after the write returns, so a subsequent read in the same instance sees its own write.

## 6. Observed feedback

Once a key is cached, the engine stops predicting and uses **observed** numbers: `hit_ratio = 1 - fills / reads`. If invalidations from other services make the hit ratio fall below `exit_hit_ratio`, the key is demoted into **cooldown** (30 s, doubling per strike up to 16×) and its entry is dropped. A key whose read rate falls below half of `min_read_rate` for `exit_windows` windows returns to **cold**.

This is the core idea: **the cost of caching a key is measured, not assumed**. A key written by another service looks attractive until it is cached; then its invalidation rate is visible and it is removed.

## 7. When tracking is uncertain

The tracking health monitor probes the tracked path every second. If a probe fails or times out, or a cached read hits a transport error, KeyShift:

1. empties the admitted set, so the next read goes to Redis;
2. flushes the local cache;
3. reports `tracking_state="unhealthy"` in metrics, stats, and telemetry.

Caching resumes only after consecutive successful probes, starting with an empty cache.

## 8. Fleet view (optional)

Every ~30 s (jittered), the SDK sends the controller a compact batch: window totals, cache residency, tracking state, and up to 20 candidate keys identified by **HMAC fingerprints**. The controller merges batches into a global ranking. It shows, for each key, the global read rate, the busiest instance's local rate, which services read it, and on how many instances it is cached.

The controller can push a **policy** (for example "deny `lock:*` for checkout-api", or "explicit mode at 10% rollout"). SDKs validate policies and can only tighten their local limits. If the controller is unreachable for longer than `valid_for_seconds`, they return to their local policy.

## Worked example

| Key | Pod reads/s | Writes | Outcome |
|---|---|---|---|
| `product:123` | 2,000 | none | admitted after 2 windows; ~99.9% local hits (one refetch per 3 s `max_staleness`) |
| `stock:7` | 15 | 25/s by another service | admitted on probation (writes are invisible locally), observed hit ratio ≈ 0, cooldown with growing backoff |
| `counter:views` | 500 | 400/s by this pod | predicted hit ratio < 0.6, never admitted |
| `config:flags` | 0.5 | rare | below `min_read_rate`: not admitted unless pinned |
| `balance:42` | 900 | rare | matches a deny pattern: bypassed |
| `report:2026` | 50 | none, 2 MiB value | exceeds `max_entry_bytes`: bypassed |
