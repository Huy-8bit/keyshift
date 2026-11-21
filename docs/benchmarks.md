# Benchmarks

All numbers on this page were produced by `benchmarks/run-all.sh` on the environment described below and are stored unedited in `benchmarks/results/`. They come from **one developer laptop**; reproduce them on your own hardware before drawing conclusions. Relative comparisons between modes are more meaningful than absolute throughput.

## What is compared

| Mode | Client | What is cached |
|---|---|---|
| `none` | go-redis v9.23 `Client` | nothing (Baseline A) |
| `csc` | go-redis v9.23 with its built-in client-side cache (`LocalCache`, same limits), wrapped with the same counters KeyShift uses | every `GET` (Baseline B: conventional client-side caching) |
| `keyshift` | KeyShift SDK, go-redis driver | keys admitted by adaptive admission (Baseline C) |
| `keyshift-rueidis` | KeyShift SDK, rueidis driver (OPTIN) | admitted keys (open-loop runs only) |

For Redis Cluster, go-redis has no client-side caching, so `csc` uses rueidis caching every read (`DoCache`) and `keyshift` uses the rueidis driver.

All caching modes use the same limits: 1,000 entries, 16 MiB, `max_staleness` 3 s. KeyShift uses a 1 s detection window in these runs (the production default is 10 s; a separate run measures the default).

## Workloads

Each run preloads the keys (`FLUSHALL` first), warms up, then measures. Values embed a version number. Writers serialize writes per key, and record the committed version and commit time after Redis acknowledges the write. A read counts as **stale** when it returns a version older than one that was committed *before the read started*. This measures real staleness experienced by the application, not a model of it. A miss on a nonexistent key triggers cache-aside population.

| Scenario | Keys | Distribution | Reads | Notes |
|---|---|---|---|---|
| uniform | 100k | uniform | 95% | **unfavorable**: no locality |
| zipf | 100k | Zipf s=1.1 | 95% | |
| single-hot | 100k | one key gets 50% of reads | 99% | |
| local-hot | 100k | 4 instances, each with its own 50-key hot set (80% of its reads) | 98% | locally hot, globally moderate |
| read-only-zipf | 100k | Zipf s=1.1 | 100% | immutable keys |
| write-heavy | 10k | Zipf s=1.1, writes target hot keys | 50% | **unfavorable** |
| expiring | 10k | Zipf, 2 s TTL, rewritten by cache-aside | 98% | |
| small-values / large-values | 100k / 2k | Zipf | 98% | 16 B / 32 KiB values |
| oversized-values | 500 | Zipf | 98% | **unfavorable**: 128 KiB > `max_entry_bytes` |
| hot-shift | 100k | Zipf, hot set moves every 5 s | 98% | |
| burst | 100k | Zipf, alternating 1 s full load and 1 s at 10% | 98% | |
| tracking-interruption | 100k | Zipf; all client connections killed halfway | 98% | fault |
| redis-restart | 10k | Zipf; Redis restarted halfway (no persistence) | 98% | fault |
| startup-surge | 100k | 32 instances start at once, no warmup | 98% | |
| controller-outage | 100k | Zipf; telemetry endpoint unreachable | 98% | KeyShift only differs |
| cluster-zipf | 100k | Zipf on a 3-master cluster | 98% | |

Two load models are used:

- **Closed loop** (32 workers): each worker issues the next operation as soon as the previous one completes. Cache hits turn into extra throughput, so Redis load per second can *rise*. Use these results for latency, throughput, and correctness.
- **Open loop** (fixed 20,000 ops/s offered, 64 workers): the same work is offered to every mode. Latency is measured from each operation's scheduled start (no coordinated omission). Use these results to compare **Redis load, Redis CPU, and server tracking state for the same work**.

Measured per run: throughput; p50/p99 latency; local hit ratio; Redis `GET` calls/s and all commands/s (`INFO commandstats` deltas); Redis CPU seconds (`used_cpu_user + used_cpu_sys` delta); `tracking_total_keys`, `tracking_total_items`, and `tracking_clients`; invalidations received; stale reads and maximum staleness; local cache memory; allocated bytes per operation; client process CPU per operation (`getrusage`); GC cycles and pause time; peak heap.

## Environment

| | |
|---|---|
| date | 2026-11-20 |
| machine | Apple M1 Pro laptop, 8 CPUs, darwin/arm64 |
| Redis | 8.6.8 in Docker (OrbStack VM), client and server on the same machine |
| Go / go-redis / rueidis | go1.26.11 / 9.23.0 / v1.0.80 |
| SDK settings | window 1 s (default run: 10 s), max_staleness 3 s, 1,000 entries / 16 MiB, drain interval 1 ms |

Client and Redis share one machine, so a "network round trip" here is a loopback hop through a VM (≈ 0.2–0.3 ms), much cheaper than a real network. Local caching saves more on real networks.

**Re-measured rows:** the `keyshift-rueidis` open-loop rows and the `keyshift` cluster row were re-measured after the rueidis driver switched to `ToStaticTTL` (marked in the notes); all other rows come from the full suite run on the same day.

**Run-to-run variance:** closed-loop throughput on this laptop varied by up to ±15% between repeated runs of the same configuration (KeyShift zipf: 84k, 103k, 105k, 109k ops/s across four runs during development). Hit ratios, tracking counts, and stale-read counts were stable to within a few percent.

## Summary

Read with the caveats above:

1. **Redis load for the same work (open loop, 20k ops/s).** Both caching modes cut Redis `GET`s by 50–86% on workloads with locality. Conventional CSC achieves a slightly higher hit ratio on Zipf workloads (69.6% vs 65.7%) because it caches every key it reads, including the tail; KeyShift caches only keys above `min_read_rate`. Redis CPU was similar or lower with KeyShift (zipf 1.01 s vs 1.06 s; single-hot 1.20 vs 1.42; local-hot 0.81 vs 0.86; large values 0.89 vs 1.02 per 10 s).
2. **Server tracking state.** Conventional CSC made Redis track every key the instance read: up to 91,191 tracked keys for one instance (32,885 on Zipf; fewer on the small-keyspace large-value and write-heavy runs). KeyShift tracked 0–547: on Zipf, about 100× fewer. At 5,000 instances this is the difference between exceeding `tracking-table-max-keys` and staying far below it ([scaling.md](scaling.md)).
3. **Invalidation traffic.** Conventional CSC received 5,739 invalidations in the open-loop Zipf run vs 92 for KeyShift (go-redis) and 39 (rueidis). In the closed-loop Zipf run the numbers were 104,943 vs 342.
4. **Stale reads.** On the write-heavy workload conventional CSC served **6.9%** stale reads in the open-loop run (4.4% closed loop; up to 8.7 ms old). KeyShift refused to cache those keys and served none. Across all scenarios KeyShift served stale reads only in `local-hot` (6 of ~1.6M) and `startup-surge` (2), all caused by writes from other instances and at most 0.9 ms old.
5. **Unfavorable workloads cost nothing.** On uniform and oversized-value workloads KeyShift admitted nothing, so its Redis load, allocations, and client CPU matched the uncached baseline. Conventional CSC tracked 91–95k keys and allocated 1.6–4.5× more per operation for a 0–1% hit ratio on the uniform workload.
6. **Where conventional CSC is better.** It adapts instantly: with a shifting hot set (`hot-shift`) it reached 71% hits vs 50% for KeyShift, and during a cold start (`startup-surge`) 66% vs 46%, because KeyShift needs `min_observation_windows` windows of evidence before admitting a key. With the production default 10 s window, the Zipf hit ratio after warmup was 59.8% (vs 65.7% with a 1 s window).
7. **Faults.** With connections killed mid-run and with Redis restarted mid-run, KeyShift served **no stale reads**; conventional CSC served 29 stale reads in the restart run. With the controller unreachable, KeyShift's results matched the normal run.
8. **Overhead.** On uncached reads KeyShift adds about 15–30 allocated bytes and 1–2 µs of client CPU per operation compared with plain go-redis (closed-loop uniform: 399 vs 384 B/op, 41.0 vs 39.5 µs CPU per op; the CPU figure includes the benchmark harness). The detector and admission state take about 0.8 MiB per instance at default limits (see [scaling.md](scaling.md)).

The **cluster** comparison is not an equal-budget comparison: the conventional baseline there is rueidis caching every read, bounded only by 16 MiB per connection rather than 1,000 entries, so it holds far more keys (97% hits vs 67%).

## Closed-loop results

Every scenario, 32 workers.

#### uniform (unfavorable for caching)

Uniform random reads over 100k keys (no locality).

| mode | ops/s | p50 µs | p99 µs | hit ratio | Redis GET/s | Redis CPU s | tracking keys | invalidations | stale reads | max stale ms | cache MiB | alloc B/op | client CPU µs/op | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| none | 40167 | 639 | 1376 | 0.0% | 38183 | 2.61 | 0 | 0 | 0 (0.000%) | 0.0 | - | 384 | 39.45 | 0 |
| csc | 35947 | 672 | 1475 | 0.7% | 33948 | 3.00 | 94833 | 57863 | 0 (0.000%) | 0.0 | 0.29 | 1637 | 49.10 | 0 |
| keyshift | 40488 | 639 | 1409 | 0.0% | 38488 | 2.60 | 0 | 0 | 0 (0.000%) | 0.0 | 0.00 | 399 | 40.98 | 0 |

#### zipf

Zipfian (s=1.1) reads over 100k keys, 5% writes.

| mode | ops/s | p50 µs | p99 µs | hit ratio | Redis GET/s | Redis CPU s | tracking keys | invalidations | stale reads | max stale ms | cache MiB | alloc B/op | client CPU µs/op | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| none | 39391 | 639 | 1409 | 0.0% | 37448 | 2.56 | 0 | 0 | 0 (0.000%) | 0.0 | - | 384 | 39.91 | 0 |
| csc | 110283 | 1 | 1245 | 71.5% | 29839 | 3.04 | 61582 | 104943 | 1 (0.000%) | 0.0 | 0.29 | 846 | 17.25 | 0 |
| keyshift | 104540 | 2 | 1245 | 66.9% | 32926 | 2.52 | 381 | 342 | 0 (0.000%) | 0.0 | 0.11 | 510 | 17.87 | 0 |

#### single-hot

One key receives 50% of reads; the rest are uniform.

| mode | ops/s | p50 µs | p99 µs | hit ratio | Redis GET/s | Redis CPU s | tracking keys | invalidations | stale reads | max stale ms | cache MiB | alloc B/op | client CPU µs/op | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| none | 38696 | 672 | 1442 | 0.0% | 38319 | 2.47 | 0 | 0 | 0 (0.000%) | 0.0 | - | 378 | 43.55 | 0 |
| csc | 68988 | 238 | 1409 | 50.3% | 33937 | 2.96 | 97692 | 23896 | 0 (0.000%) | 0.0 | 0.29 | 1090 | 27.24 | 0 |
| keyshift | 80266 | 250 | 1245 | 50.0% | 39780 | 2.56 | 1 | 0 | 0 (0.000%) | 0.0 | 0.00 | 452 | 21.44 | 0 |

#### local-hot

Each of 4 instances has its own hot set (80% of its reads); globally moderate.

| mode | ops/s | p50 µs | p99 µs | hit ratio | Redis GET/s | Redis CPU s | tracking keys | invalidations | stale reads | max stale ms | cache MiB | alloc B/op | client CPU µs/op | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| none | 41693 | 623 | 1311 | 0.0% | 40874 | 2.55 | 0 | 0 | 0 (0.000%) | 0.0 | - | 378 | 39.01 | 0 |
| csc | 156342 | 1 | 1245 | 80.1% | 30591 | 2.95 | 90332 | 86554 | 1 (0.000%) | 0.1 | 1.16 | 743 | 12.04 | 0 |
| keyshift | 156980 | 1 | 1311 | 79.9% | 30917 | 2.43 | 200 | 129 | 6 (0.000%) | 0.9 | 0.06 | 524 | 11.02 | 0 |

#### read-only-zipf

Zipfian read-only traffic on immutable keys.

| mode | ops/s | p50 µs | p99 µs | hit ratio | Redis GET/s | Redis CPU s | tracking keys | invalidations | stale reads | max stale ms | cache MiB | alloc B/op | client CPU µs/op | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| none | 37905 | 672 | 1475 | 0.0% | 37912 | 2.42 | 0 | 0 | 0 (0.000%) | 0.0 | - | 381 | 44.02 | 0 |
| csc | 118599 | 1 | 1311 | 71.4% | 33921 | 2.83 | 81595 | 0 | 0 (0.000%) | 0.0 | 0.29 | 851 | 15.89 | 0 |
| keyshift | 116488 | 2 | 1180 | 66.9% | 38636 | 2.45 | 378 | 0 | 0 (0.000%) | 0.0 | 0.11 | 506 | 13.80 | 0 |

#### write-heavy (unfavorable for caching)

Zipfian with 50% writes aimed at the hottest keys.

| mode | ops/s | p50 µs | p99 µs | hit ratio | Redis GET/s | Redis CPU s | tracking keys | invalidations | stale reads | max stale ms | cache MiB | alloc B/op | client CPU µs/op | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| none | 29264 | 434 | 9961 | 0.0% | 14642 | 2.21 | 0 | 0 | 0 (0.000%) | 0.0 | - | 407 | 47.43 | 0 |
| csc | 32166 | 360 | 10486 | 46.1% | 8683 | 2.38 | 4963 | 84262 | 7043 (4.370%) | 7.1 | 0.28 | 861 | 44.18 | 0 |
| keyshift | 29475 | 434 | 9961 | 0.0% | 14753 | 2.25 | 0 | 0 | 0 (0.000%) | 0.0 | 0.00 | 427 | 47.50 | 0 |

#### expiring

Zipfian reads of keys with a 2s TTL that are rewritten as they expire.

| mode | ops/s | p50 µs | p99 µs | hit ratio | Redis GET/s | Redis CPU s | tracking keys | invalidations | stale reads | max stale ms | cache MiB | alloc B/op | client CPU µs/op | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| none | 37103 | 672 | 1802 | 0.0% | 36374 | 2.50 | 0 | 0 | 0 (0.000%) | 0.0 | - | 412 | 45.09 | 0 |
| csc | 109015 | 1 | 1868 | 80.9% | 20433 | 2.75 | 6436 | 191637 | 79 (0.007%) | 3.0 | 0.28 | 766 | 14.85 | 0 |
| keyshift | 130025 | 1 | 1606 | 77.4% | 28776 | 2.45 | 534 | 3274 | 0 (0.000%) | 0.0 | 0.15 | 519 | 12.70 | 0 |

#### small-values

Zipfian reads, 16-byte values.

| mode | ops/s | p50 µs | p99 µs | hit ratio | Redis GET/s | Redis CPU s | tracking keys | invalidations | stale reads | max stale ms | cache MiB | alloc B/op | client CPU µs/op | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| none | 35536 | 705 | 1769 | 0.0% | 34841 | 2.49 | 0 | 0 | 0 (0.000%) | 0.0 | - | 263 | 40.06 | 0 |
| csc | 100638 | 1 | 1573 | 71.4% | 28210 | 2.87 | 70469 | 44736 | 0 (0.000%) | 0.0 | 0.18 | 551 | 16.40 | 0 |
| keyshift | 111150 | 2 | 1245 | 66.8% | 36232 | 2.41 | 384 | 143 | 0 (0.000%) | 0.0 | 0.07 | 304 | 14.97 | 0 |

#### large-values

Zipfian reads, 32 KiB values.

| mode | ops/s | p50 µs | p99 µs | hit ratio | Redis GET/s | Redis CPU s | tracking keys | invalidations | stale reads | max stale ms | cache MiB | alloc B/op | client CPU µs/op | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| none | 30871 | 836 | 1868 | 0.0% | 30260 | 3.85 | 0 | 0 | 0 (0.000%) | 0.0 | - | 41031 | 57.53 | 0 |
| csc | 96671 | 10 | 3473 | 85.0% | 14217 | 3.75 | 1773 | 113357 | 286 (0.030%) | 3.8 | 15.58 | 86567 | 31.27 | 0 |
| keyshift | 124444 | 14 | 2621 | 85.8% | 17304 | 4.04 | 494 | 11975 | 0 (0.000%) | 0.0 | 14.29 | 70189 | 24.72 | 0 |

#### oversized-values (unfavorable for caching)

Zipfian reads, 128 KiB values (above max_entry_bytes).

| mode | ops/s | p50 µs | p99 µs | hit ratio | Redis GET/s | Redis CPU s | tracking keys | invalidations | stale reads | max stale ms | cache MiB | alloc B/op | client CPU µs/op | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| none | 14820 | 1343 | 3867 | 0.0% | 14525 | 4.95 | 0 | 0 | 0 (0.000%) | 0.0 | - | 139343 | 116.03 | 0 |
| csc | 33863 | 34 | 4981 | 79.1% | 6936 | 4.06 | 457 | 50974 | 114 (0.034%) | 4.1 | 14.02 | 326796 | 72.44 | 0 |
| keyshift | 15344 | 1311 | 3342 | 0.0% | 15032 | 4.94 | 0 | 0 | 0 (0.000%) | 0.0 | 0.00 | 139358 | 121.35 | 0 |

#### hot-shift

Zipfian whose hot set moves every 5s.

| mode | ops/s | p50 µs | p99 µs | hit ratio | Redis GET/s | Redis CPU s | tracking keys | invalidations | stale reads | max stale ms | cache MiB | alloc B/op | client CPU µs/op | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| none | 40140 | 655 | 1376 | 0.0% | 39349 | 2.52 | 0 | 0 | 0 (0.000%) | 0.0 | - | 382 | 40.41 | 0 |
| csc | 110795 | 1 | 1278 | 71.2% | 31293 | 2.93 | 80412 | 58095 | 0 (0.000%) | 0.0 | 0.29 | 855 | 16.65 | 0 |
| keyshift | 76119 | 279 | 1311 | 50.3% | 37113 | 2.52 | 698 | 105 | 0 (0.000%) | 0.0 | 0.15 | 493 | 22.27 | 0 |

#### burst

Zipfian with alternating 1s bursts and 1s lulls.

| mode | ops/s | p50 µs | p99 µs | hit ratio | Redis GET/s | Redis CPU s | tracking keys | invalidations | stale reads | max stale ms | cache MiB | alloc B/op | client CPU µs/op | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| none | 28736 | 557 | 1311 | 0.0% | 28157 | 2.12 | 0 | 0 | 0 (0.000%) | 0.0 | - | 383 | 40.70 | 0 |
| csc | 83064 | 1 | 1245 | 71.6% | 23120 | 2.49 | 65673 | 29918 | 7 (0.001%) | 4.4 | 0.29 | 851 | 16.21 | 0 |
| keyshift | 79868 | 2 | 1180 | 66.7% | 26090 | 2.18 | 368 | 165 | 0 (0.000%) | 0.0 | 0.11 | 526 | 16.22 | 0 |

#### tracking-interruption

Zipfian; client connections are killed halfway.

| mode | ops/s | p50 µs | p99 µs | hit ratio | Redis GET/s | Redis CPU s | tracking keys | invalidations | stale reads | max stale ms | cache MiB | alloc B/op | client CPU µs/op | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| none | 39205 | 655 | 1409 | 0.0% | 38436 | 2.43 | 0 | 0 | 0 (0.000%) | 0.0 | - | 389 | 40.59 | 0 |
| csc | 113960 | 1 | 1278 | 71.4% | 31934 | 2.85 | 71990 | 29991 | 0 (0.000%) | 0.0 | 0.29 | 849 | 16.15 | 0 |
| keyshift | 109018 | 2 | 1245 | 67.0% | 35330 | 2.49 | 382 | 91 | 0 (0.000%) | 0.0 | 0.11 | 502 | 15.78 | 0 |

Notes: none: killed 32 client connections at mid-run; csc: killed 32 client connections at mid-run; keyshift: killed 46 client connections at mid-run.

#### startup-surge

32 instances start simultaneously with no warmup.

| mode | ops/s | p50 µs | p99 µs | hit ratio | Redis GET/s | Redis CPU s | tracking keys | invalidations | stale reads | max stale ms | cache MiB | alloc B/op | client CPU µs/op | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| none | 39108 | 672 | 1442 | 0.0% | 38319 | 2.46 | 0 | 0 | 0 (0.000%) | 0.0 | - | 387 | 40.18 | 0 |
| csc | 98212 | 2 | 1311 | 65.7% | 33003 | 2.83 | 59382 | 24251 | 9 (0.001%) | 0.5 | 9.27 | 929 | 18.82 | 0 |
| keyshift | 68898 | 393 | 1343 | 46.3% | 36284 | 2.53 | 203 | 354 | 2 (0.000%) | 0.5 | 0.93 | 717 | 26.40 | 0 |

Notes: none: slowest instance startup 1.2 ms; csc: slowest instance startup 2.5 ms; keyshift: slowest instance startup 20.8 ms.

#### controller-outage

Zipfian; KeyShift telemetry endpoint is unreachable.

| mode | ops/s | p50 µs | p99 µs | hit ratio | Redis GET/s | Redis CPU s | tracking keys | invalidations | stale reads | max stale ms | cache MiB | alloc B/op | client CPU µs/op | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| none | 40189 | 672 | 1409 | 0.0% | 39396 | 2.53 | 0 | 0 | 0 (0.000%) | 0.0 | - | 382 | 39.90 | 0 |
| csc | 110853 | 1 | 1311 | 71.5% | 30988 | 2.86 | 71926 | 51770 | 0 (0.000%) | 0.0 | 0.29 | 851 | 16.00 | 0 |
| keyshift | 109510 | 2 | 1245 | 66.9% | 35525 | 2.51 | 385 | 142 | 0 (0.000%) | 0.0 | 0.11 | 506 | 15.75 | 0 |

Notes: none: KeyShift telemetry endpoint unreachable for the whole run; csc: KeyShift telemetry endpoint unreachable for the whole run; keyshift: KeyShift telemetry endpoint unreachable for the whole run.

#### cluster-zipf

Zipfian on Redis Cluster (requires --cluster).

| mode | ops/s | p50 µs | p99 µs | hit ratio | Redis GET/s | Redis CPU s | tracking keys | invalidations | stale reads | max stale ms | cache MiB | alloc B/op | client CPU µs/op | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| none | 52508 | 483 | 1147 | 0.0% | 51491 | 5.36 | 0 | 0 | 0 (0.000%) | 0.0 | - | 383 | 34.96 | 0 |
| csc | 1206666 | 1 | 541 | 97.2% | 33808 | 6.61 | 82546 | 0 | 16 (0.000%) | 0.1 | - | 49 | 2.77 | 0 |
| keyshift | 301703 | 1 | 524 | 67.0% | 97741 | 6.00 | 378 | 198 | 0 (0.000%) | 0.0 | 0.09 | 142 | 7.58 | 0 |

Notes: keyshift: re-measured after the rueidis driver switched to ToStaticTTL (same machine, same day).

## Open-loop results (fixed 20,000 ops/s offered)

64 workers. In several scenarios the uncached `none` baseline shows p99 latencies of 100–300 ms: at this offered load its go-redis pool saturates intermittently on this machine, and the latency includes queueing from the scheduled start time. In `write-heavy`, the go-redis-based modes that send most operations to Redis (`none`, `keyshift`) saturate the same way, while rueidis's pipelining does not. This reflects the client library under this load, not admission overhead.

#### zipf

Zipfian (s=1.1) reads over 100k keys, 5% writes.

| mode | ops/s | p50 µs | p99 µs | hit ratio | Redis GET/s | Redis CPU s | tracking keys | invalidations | stale reads | max stale ms | cache MiB | alloc B/op | client CPU µs/op | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| none | 20000 | 311 | 318767 | 0.0% | 19006 | 2.04 | 0 | 0 | 0 (0.000%) | 0.0 | - | 384 | 48.22 | 0 |
| csc | 20000 | 36 | 393 | 69.6% | 5784 | 1.06 | 32885 | 5739 | 1 (0.001%) | 0.2 | 0.29 | 956 | 29.03 | 0 |
| keyshift | 20000 | 40 | 377 | 65.7% | 6529 | 1.01 | 341 | 92 | 0 (0.000%) | 0.0 | 0.10 | 628 | 29.97 | 0 |
| keyshift-rueidis | 20000 | 42 | 352 | 65.9% | 6494 | 1.18 | 350 | 39 | 0 (0.000%) | 0.0 | 0.09 | 161 | 27.72 | 0 |

Notes: keyshift-rueidis: re-measured after the rueidis driver switched to ToStaticTTL (same machine, same day).

#### single-hot

One key receives 50% of reads; the rest are uniform.

| mode | ops/s | p50 µs | p99 µs | hit ratio | Redis GET/s | Redis CPU s | tracking keys | invalidations | stale reads | max stale ms | cache MiB | alloc B/op | client CPU µs/op | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| none | 20009 | 279 | 230687 | 0.0% | 19815 | 1.91 | 0 | 0 | 0 (0.000%) | 0.0 | - | 378 | 49.40 | 0 |
| csc | 20000 | 117 | 442 | 50.4% | 9832 | 1.42 | 78300 | 2089 | 0 (0.000%) | 0.0 | 0.29 | 1181 | 35.99 | 0 |
| keyshift | 20000 | 111 | 385 | 50.0% | 9898 | 1.20 | 1 | 0 | 0 (0.000%) | 0.0 | 0.00 | 472 | 34.64 | 0 |
| keyshift-rueidis | 20000 | 117 | 352 | 50.0% | 9898 | 1.26 | 1 | 0 | 0 (0.000%) | 0.0 | 0.00 | 159 | 31.20 | 0 |

Notes: keyshift-rueidis: re-measured after the rueidis driver switched to ToStaticTTL (same machine, same day).

#### local-hot

Each of 4 instances has its own hot set (80% of its reads); globally moderate.

| mode | ops/s | p50 µs | p99 µs | hit ratio | Redis GET/s | Redis CPU s | tracking keys | invalidations | stale reads | max stale ms | cache MiB | alloc B/op | client CPU µs/op | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| none | 20001 | 262 | 1049 | 0.0% | 19610 | 1.87 | 0 | 0 | 0 (0.000%) | 0.0 | - | 378 | 53.75 | 0 |
| csc | 20000 | 25 | 360 | 79.7% | 3993 | 0.86 | 45565 | 1697 | 0 (0.000%) | 0.0 | 1.16 | 852 | 24.13 | 0 |
| keyshift | 20000 | 26 | 360 | 79.6% | 4004 | 0.81 | 200 | 19 | 0 (0.000%) | 0.0 | 0.06 | 872 | 25.00 | 0 |
| keyshift-rueidis | 20000 | 29 | 319 | 79.6% | 4004 | 0.81 | 200 | 6 | 0 (0.000%) | 0.0 | 0.05 | 142 | 19.24 | 0 |

Notes: keyshift-rueidis: re-measured after the rueidis driver switched to ToStaticTTL (same machine, same day).

#### read-only-zipf

Zipfian read-only traffic on immutable keys.

| mode | ops/s | p50 µs | p99 µs | hit ratio | Redis GET/s | Redis CPU s | tracking keys | invalidations | stale reads | max stale ms | cache MiB | alloc B/op | client CPU µs/op | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| none | 20013 | 258 | 142606 | 0.0% | 20016 | 1.91 | 0 | 0 | 0 (0.000%) | 0.0 | - | 381 | 50.23 | 0 |
| csc | 20000 | 33 | 401 | 69.6% | 6081 | 1.13 | 36089 | 0 | 0 (0.000%) | 0.0 | 0.29 | 957 | 27.96 | 0 |
| keyshift | 20001 | 38 | 360 | 65.9% | 6832 | 1.14 | 359 | 0 | 0 (0.000%) | 0.0 | 0.10 | 626 | 27.85 | 0 |
| keyshift-rueidis | 20000 | 36 | 344 | 65.9% | 6835 | 1.06 | 357 | 0 | 0 (0.000%) | 0.0 | 0.09 | 150 | 25.84 | 0 |

Notes: keyshift-rueidis: re-measured after the rueidis driver switched to ToStaticTTL (same machine, same day).

#### write-heavy (unfavorable for caching)

Zipfian with 50% writes aimed at the hottest keys.

| mode | ops/s | p50 µs | p99 µs | hit ratio | Redis GET/s | Redis CPU s | tracking keys | invalidations | stale reads | max stale ms | cache MiB | alloc B/op | client CPU µs/op | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| none | 20001 | 279 | 142606 | 0.0% | 10002 | 1.85 | 0 | 0 | 0 (0.000%) | 0.0 | - | 406 | 52.48 | 0 |
| csc | 19999 | 229 | 770 | 46.8% | 5320 | 1.98 | 4866 | 50608 | 6946 (6.946%) | 8.7 | 0.28 | 922 | 48.19 | 0 |
| keyshift | 20001 | 279 | 138412 | 0.0% | 10002 | 1.95 | 0 | 0 | 0 (0.000%) | 0.0 | 0.00 | 436 | 54.86 | 0 |
| keyshift-rueidis | 20000 | 238 | 590 | 0.0% | 10002 | 1.80 | 0 | 0 | 0 (0.000%) | 0.0 | 0.00 | 248 | 53.55 | 0 |

Notes: keyshift-rueidis: re-measured after the rueidis driver switched to ToStaticTTL (same machine, same day).

#### large-values

Zipfian reads, 32 KiB values.

| mode | ops/s | p50 µs | p99 µs | hit ratio | Redis GET/s | Redis CPU s | tracking keys | invalidations | stale reads | max stale ms | cache MiB | alloc B/op | client CPU µs/op | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| none | 20002 | 360 | 167772 | 0.0% | 19611 | 2.99 | 0 | 0 | 0 (0.000%) | 0.0 | - | 41032 | 64.75 | 0 |
| csc | 20000 | 30 | 483 | 84.3% | 3080 | 1.02 | 1813 | 23155 | 91 (0.046%) | 4.3 | 15.58 | 87273 | 33.65 | 0 |
| keyshift | 20000 | 32 | 483 | 85.4% | 2871 | 0.89 | 547 | 1734 | 0 (0.000%) | 0.0 | 14.29 | 70493 | 31.85 | 0 |
| keyshift-rueidis | 20000 | 27 | 401 | 85.9% | 2769 | 0.91 | 537 | 874 | 0 (0.000%) | 0.0 | 15.03 | 5299 | 18.77 | 0 |

Notes: keyshift-rueidis: re-measured after the rueidis driver switched to ToStaticTTL (same machine, same day).

#### uniform (unfavorable for caching)

Uniform random reads over 100k keys (no locality).

| mode | ops/s | p50 µs | p99 µs | hit ratio | Redis GET/s | Redis CPU s | tracking keys | invalidations | stale reads | max stale ms | cache MiB | alloc B/op | client CPU µs/op | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| none | 20001 | 270 | 1311 | 0.0% | 19005 | 1.98 | 0 | 0 | 0 (0.000%) | 0.0 | - | 384 | 50.31 | 0 |
| csc | 20005 | 319 | 222298 | 0.7% | 18882 | 2.38 | 91191 | 19155 | 0 (0.000%) | 0.0 | 0.29 | 1733 | 59.19 | 0 |
| keyshift | 20000 | 262 | 786 | 0.0% | 19007 | 1.99 | 0 | 0 | 0 (0.000%) | 0.0 | 0.00 | 414 | 52.74 | 0 |
| keyshift-rueidis | 20000 | 229 | 483 | 0.0% | 19006 | 1.84 | 0 | 0 | 0 (0.000%) | 0.0 | 0.00 | 225 | 47.46 | 0 |

Notes: keyshift-rueidis: re-measured after the rueidis driver switched to ToStaticTTL (same machine, same day).

## Redis restart

#### redis-restart

Zipfian; Redis is restarted halfway (requires --restart-container).

| mode | ops/s | p50 µs | p99 µs | hit ratio | Redis GET/s | Redis CPU s | tracking keys | invalidations | stale reads | max stale ms | cache MiB | alloc B/op | client CPU µs/op | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| none | 36058 | 672 | 1671 | 0.0% | 16722 | 1.16 | 0 | 0 | 0 (0.000%) | 0.0 | - | 403 | 39.38 | 0 |
| csc | 125462 | 1 | 1343 | 80.9% | 12056 | 1.30 | 8584 | 125190 | 29 (0.002%) | 4.1 | 0.29 | 754 | 12.43 | 0 |
| keyshift | 149518 | 1 | 1343 | 77.6% | 15986 | 1.36 | 543 | 2178 | 0 (0.000%) | 0.0 | 0.16 | 509 | 10.96 | 0 |

Notes: none: restarted Redis (no persistence) at mid-run; keys repopulated by cache-aside; none: Redis counters reset by the restart: Redis GET/s and CPU cover only the post-restart part (lower bound); csc: restarted Redis (no persistence) at mid-run; keys repopulated by cache-aside; csc: Redis counters reset by the restart: Redis GET/s and CPU cover only the post-restart part (lower bound); keyshift: restarted Redis (no persistence) at mid-run; keys repopulated by cache-aside; keyshift: Redis counters reset by the restart: Redis GET/s and CPU cover only the post-restart part (lower bound).

## Production default window (10 s)

`keyshift` only, Zipf, open loop at 20,000 ops/s, 30 s warmup, 20 s measured.

#### zipf

Zipfian (s=1.1) reads over 100k keys, 5% writes.

| mode | ops/s | p50 µs | p99 µs | hit ratio | Redis GET/s | Redis CPU s | tracking keys | invalidations | stale reads | max stale ms | cache MiB | alloc B/op | client CPU µs/op | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| keyshift | 20000 | 55 | 393 | 59.8% | 7638 | 2.03 | 176 | 89 | 0 (0.000%) | 0.0 | 0.05 | 572 | 31.54 | 0 |

## Large-scale logical simulation

`keyshift simulate` created **5,000 virtual SDK instances** (100 services × 50) in one process. They uploaded synthetic telemetry batches (Zipf-distributed keys per service, half shared across services) to an in-process controller over real loopback HTTP with default limits. **This is a logical simulation of the control plane, not 5,000 application pods.**

| run | round | batches | records | seconds | batches/s | records/s | p50 ms | p99 ms | max ms | errors | rejected | avg batch bytes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 20 candidates, max rate | 1 | 5000 | 100000 | 0.6 | 8391 | 167819 | 2.57 | 14.32 | 23.1 | 0 | 0 | 1329 |
| 20 candidates, max rate | 2 | 5000 | 100000 | 0.6 | 7976 | 159524 | 2.69 | 10.49 | 18.0 | 0 | 0 | 1332 |
| 20 candidates, max rate | 3 | 5000 | 100000 | 0.6 | 7863 | 157252 | 2.77 | 11.72 | 20.4 | 0 | 0 | 1331 |
| 50 candidates, max rate | 1 | 5000 | 250000 | 0.7 | 7274 | 363698 | 2.88 | 25.02 | 54.6 | 0 | 0 | 2365 |
| 50 candidates, max rate | 2 | 5000 | 250000 | 0.6 | 8073 | 403671 | 2.15 | 13.94 | 22.4 | 0 | 0 | 2368 |
| 50 candidates, max rate | 3 | 5000 | 250000 | 0.6 | 7801 | 390043 | 2.26 | 14.65 | 26.6 | 0 | 0 | 2369 |
| 20 candidates, paced over 30 s | 1 | 5000 | 100000 | 30.0 | 167 | 3334 | 0.56 | 4.34 | 12.3 | 0 | 0 | 1328 |
| 20 candidates, paced over 30 s | 2 | 5000 | 100000 | 30.0 | 167 | 3334 | 0.56 | 4.89 | 11.2 | 0 | 0 | 1332 |

| run | aggregation of 5,000 instances | global keys kept | top key seen on instances | process heap in use (controller + simulator) |
|---|---:|---:|---:|---:|
| 20 candidates | 88 ms | 5000 | 3745 | 73 MiB |
| 50 candidates | 214 ms | 5000 | 4876 | 146 MiB |
| 20 candidates, paced | 104 ms | 5000 | 3745 | 69 MiB |

At the target pacing (5,000 instances every 30 s ≈ 167 uploads/s, 3,333 records/s) every upload succeeded with a p99 of under 5 ms. At maximum rate the single controller accepted about 8,000 uploads/s with 20 candidates (about 48× the target rate) and about 7,000–8,000 uploads/s with 50 candidates.

During development the simulator first reported 100% rejected uploads. The cause was a controller defect: a token configured for both ingest and viewer kept only its highest role. Earlier numbers from that run were discarded.

## Reproducing

```bash
make test-infra-up build
./benchmarks/run-all.sh
```

Raw JSON and the generated Markdown are in `benchmarks/results/`. The scenario harness is `internal/bench`; the simulation is `internal/simulate`.
