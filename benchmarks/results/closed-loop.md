Environment: darwin/arm64, 8 CPUs, go1.26.11, Redis 8.6.8 at localhost:16379, go-redis 9.23.0. Duration 10s after 6s warmup, concurrency 32, closed-loop (each worker issues the next operation as soon as the previous completes), cache limits 1000 entries / 16 MiB, max_staleness 3s, KeyShift window 1s.

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

