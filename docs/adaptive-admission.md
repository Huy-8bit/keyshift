# Adaptive admission

The admission engine (`internal/admission`) decides, once per window and per instance, which keys are read through the local cache. It is deterministic and explainable: every decision carries a human-readable reason, available through `client.Explain(key)`.

## States

```text
             qualifies                 qualifies N windows + fits budget
   COLD ───────────────► CANDIDATE ──────────────────────────────────► ADMITTED
    ▲  ◄───────────────────  │                                            │ │
    │   stops qualifying                          observed hit ratio      │ │ read rate < min/2
    │                                             < exit_hit_ratio        │ │ for exit_windows
    │                                                   ▼                 │ │
    └──────────────── COOLDOWN (30 s × 2^(strikes-1), max 16×) ◄──────────┘ │
    ▲                                                                        │
    └────────────────────────────────────────────────────────────────────────┘

   BYPASSED   deny pattern, deny fingerprint, value > max_entry_bytes
   DISABLED   mode=off
   UNHEALTHY  client tracking not confirmed (caching suspended for all keys)
```

`CACHED` from the original design is folded into `ADMITTED`: residency is managed by the client library's cache and can change at any moment (invalidation, staleness refresh, connection loss). An admitted key is read through the cache path; whether a given read is a hit is reported in the hit and fill counters.

## Signals and formulas

For a key that is not cached yet (predicted):

```text
read_rate        = min(window_reads, decayed_detector_estimate) / window, EWMA-smoothed
write_rate       = local writes / window, EWMA-smoothed
predicted_fills  = min(read_rate, write_rate + 1 / max_staleness)
hit_ratio        = 1 - predicted_fills / read_rate
saved_rate       = read_rate - predicted_fills
```

For a cached key (observed):

```text
hit_ratio   = 1 - fills / reads           (this window)
saved_rate  = read_rate × hit_ratio
```

Invalidations caused by **other** clients are invisible before a key is cached. KeyShift does not guess: it admits on the predicted ratio and then measures. A key with heavy external writes is demoted within one window of observation and kept out by an exponentially growing cooldown.

## Rules

| Rule | Default |
|---|---|
| candidate when `read_rate ≥ min_read_rate` and `hit_ratio ≥ min_hit_ratio` | 2 reads/s, 0.6 |
| admitted after `min_observation_windows` consecutive qualifying windows | 2 |
| cooldown when observed `hit_ratio < exit_hit_ratio` with ≥ 20 reads in the window | 0.3 |
| cooldown length | `admission_cooldown` × 2^(strikes−1), capped at 16× |
| demoted to cold when `read_rate < min_read_rate / 2` for `exit_windows` windows | 3 |
| bypassed when value size > `max_entry_bytes` | 64 KiB |
| pinned keys are admitted without waiting, subject to deny, size, and health rules | – |
| `explicit` mode admits only pinned keys | – |

The gaps between entry and exit thresholds (rate 2 vs 1, hit ratio 0.6 vs 0.3), the observation windows, smoothing, and cooldown together provide **hysteresis**.

## Budgets

Each window, eligible keys are sorted by **pinned first**, then **benefit density** = `saved_rate / (value + key + 160 bytes)`, with a 1.25× bonus for keys that are already admitted (stability). Keys are admitted in that order until `max_entries` or `max_memory_bytes` is reached. The rest stay `CANDIDATE` with the reason "budget exhausted".

The client library's cache enforces the same limits independently as a hard bound (go-redis `LocalCache` limits or KeyShift's rueidis store), so a burst of fills between windows cannot exceed memory.

Remote policies can lower `max_entries`, `max_memory_bytes`, and `max_entry_bytes` for a service, which gives per-service budgets. Local configuration is the per-pod hard limit.

## Policy changes take effect immediately

`SetPolicy` (from the controller, `ApplyPolicy`, or staleness reversion) re-filters the admitted set at once. Newly denied keys, `mode=off`, and unhealthy tracking remove keys before the next request, and their local entries are dropped.

## Explaining a decision

```go
d := client.Explain("stock:7")
// {State:"cooldown" Reason:"observed hit ratio 0.04 below exit threshold 0.30
//   (fills 29 / reads 30, invalidations 29); cooldown 1m0s" Strikes:2 ...}
```

Fields include read, write, fill, and invalidation rates, observed or predicted hit ratio, saved rate, value size, qualifying windows, strikes, cooldown deadline, pin status, and the policy version and source.

## Tuning guidance

- **Few hits, many candidates in `cold`** with reason `read rate … below min_read_rate`: per-instance traffic is too spread out. Lowering `min_read_rate` adds memory and tracking cost for little gain; check `local_share` and `max_local_rate` in the dashboard first.
- **Many cooldowns**: the keyspace has external writers. This is the engine working as designed; deny the pattern to avoid probation fills entirely.
- **Budget rejections** (`keyshift_admission_decisions_total{decision="budget_reject"}`): raise the budget, or confirm the extra keys are worth it in the "Most beneficial keys" view.
- **`max_staleness`** caps the best hit ratio of a key at `1 - 1/(read_rate × max_staleness)`. For a key read 10×/s with 3 s staleness the ceiling is 96.7%. Raising it improves hits but lengthens the worst-case stale window if invalidations are lost.
