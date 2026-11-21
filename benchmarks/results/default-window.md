Environment: darwin/arm64, 8 CPUs, go1.26.11, Redis 8.6.8 at localhost:16379, go-redis 9.23.0. Duration 20s after 30s warmup, concurrency 64, open-loop at 20000 ops/s target (latency measured from scheduled start), cache limits 1000 entries / 16 MiB, max_staleness 3s, KeyShift window 10s.

#### zipf

Zipfian (s=1.1) reads over 100k keys, 5% writes.

| mode | ops/s | p50 µs | p99 µs | hit ratio | Redis GET/s | Redis CPU s | tracking keys | invalidations | stale reads | max stale ms | cache MiB | alloc B/op | client CPU µs/op | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| keyshift | 20000 | 55 | 393 | 59.8% | 7638 | 2.03 | 176 | 89 | 0 (0.000%) | 0.0 | 0.05 | 572 | 31.54 | 0 |

