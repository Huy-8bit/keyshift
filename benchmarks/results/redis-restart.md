Environment: darwin/arm64, 8 CPUs, go1.26.11, Redis 8.6.8 at localhost:16380, go-redis 9.23.0. Duration 10s after 6s warmup, concurrency 32, closed-loop (each worker issues the next operation as soon as the previous completes), cache limits 1000 entries / 16 MiB, max_staleness 3s, KeyShift window 1s.

#### redis-restart

Zipfian; Redis is restarted halfway (requires --restart-container).

| mode | ops/s | p50 µs | p99 µs | hit ratio | Redis GET/s | Redis CPU s | tracking keys | invalidations | stale reads | max stale ms | cache MiB | alloc B/op | client CPU µs/op | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| none | 36058 | 672 | 1671 | 0.0% | 16722 | 1.16 | 0 | 0 | 0 (0.000%) | 0.0 | - | 403 | 39.38 | 0 |
| csc | 125462 | 1 | 1343 | 80.9% | 12056 | 1.30 | 8584 | 125190 | 29 (0.002%) | 4.1 | 0.29 | 754 | 12.43 | 0 |
| keyshift | 149518 | 1 | 1343 | 77.6% | 15986 | 1.36 | 543 | 2178 | 0 (0.000%) | 0.0 | 0.16 | 509 | 10.96 | 0 |

Notes: none: restarted Redis (no persistence) at mid-run; keys repopulated by cache-aside; none: Redis counters reset by the restart: Redis GET/s and CPU cover only the post-restart part (lower bound); csc: restarted Redis (no persistence) at mid-run; keys repopulated by cache-aside; csc: Redis counters reset by the restart: Redis GET/s and CPU cover only the post-restart part (lower bound); keyshift: restarted Redis (no persistence) at mid-run; keys repopulated by cache-aside; keyshift: Redis counters reset by the restart: Redis GET/s and CPU cover only the post-restart part (lower bound).

