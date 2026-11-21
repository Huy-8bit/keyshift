Environment: darwin/arm64, 8 CPUs, go1.26.11, Redis 8.6.8 at localhost:16379, go-redis 9.23.0. Duration 10s after 6s warmup, concurrency 64, open-loop at 20000 ops/s target (latency measured from scheduled start), cache limits 1000 entries / 16 MiB, max_staleness 3s, KeyShift window 1s.

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

