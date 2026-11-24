# Adaptive admission

The admission engine (`internal/admission`) evaluates keys for each application instance at the end of each detection window. Its decisions control which reads use the local cache. Use `client.Explain(key)` to inspect a key's state and the reason for it.

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

`ADMITTED` means that a key is eligible for the cache path, not that its value is currently resident. The client library manages residency, which can change after invalidation, expiry, eviction, or connection loss. Hit and fill counters describe what happened to individual reads.

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

Invalidations caused by **other** clients are invisible before a key is cached. KeyShift initially uses the predicted ratio, then measures fills after admission. If the observed ratio falls below the exit threshold and the window contains at least 20 reads, the key enters cooldown; repeated strikes extend that cooldown.

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

Different entry and exit thresholds, observation windows, smoothing, and cooldown provide **hysteresis**. Together, they reduce repeated admission and demotion for keys near a threshold.

## Budgets

Each window, the engine orders eligible keys with **pinned keys first**, then by benefit density: `saved_rate / (value + key + 160 bytes)`. Existing admissions receive a 1.25× stability bonus. Keys are selected in that order until `max_entries` or `max_memory_bytes` is reached; remaining eligible keys stay candidates with the reason "budget exhausted".

The client library's cache enforces the same limits independently as a hard bound (go-redis `LocalCache` limits or KeyShift's rueidis store), so a burst of fills between windows cannot exceed memory.

Remote policies can lower `max_entries`, `max_memory_bytes`, and `max_entry_bytes` for a service. The local configuration remains the per-instance hard limit for those settings.

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

- **Few hits, many candidates in `cold`** with reason `read rate … below min_read_rate`: per-instance traffic is too spread out. Lowering `min_read_rate` adds memory and tracking cost for little gain; check `local_share` and `max_local_rate` in the Grafana "KeyShift Fleet" dashboard first.
- **Many cooldowns**: the keyspace has external writers. This is the engine working as designed; deny the pattern to avoid probation fills entirely.
- **Budget rejections** (`keyshift_admission_decisions_total{decision="budget_reject"}`): raise the budget, or confirm the extra keys are worth it in the "Most beneficial keys" view.
- **`max_staleness`** contributes a refresh floor to the predicted hit ratio: approximately `1 - 1/(read_rate × max_staleness)` without writes. For a key read 10×/s with 3 s staleness, this estimate is 96.7%. A longer interval may improve hits but also allows entries to remain cached longer if invalidations are lost.
