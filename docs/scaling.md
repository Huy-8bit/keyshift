# Scaling

The target scenario: **100 services × 50 pods = 5,000 SDK instances** sharing Redis Standalone or Cluster, with rolling deployments, autoscaling, restarts, and failovers.

## What scales with what

| Cost | Scales with | Bound |
|---|---|---|
| SDK memory | `MaxTrackingCandidates` + cache budget | detector + admission state ≈ 0.8 MiB at the default 1,000 candidates with 40-byte keys, all admitted (measured by `TestMemoryFootprint`), plus cached entries up to `MaxMemoryBytes` (16 MiB) |
| SDK CPU on the request path | reads | one map lookup on cached reads; ~150 ns sharded-lock detector update on uncached reads (≈ 32 ns at 10% sampling) |
| Redis tracking table | **admitted keys × instances that admitted them** | not every key read, as with default-mode tracking |
| Invalidation messages | writes to admitted keys × instances caching them | write-heavy keys are rejected or put in cooldown |
| Redis connections | +1 small tracked pool per instance (go-redis driver) | `Tracking.TrackedPoolSize` |
| Controller ingest | instances / report interval | 5,000 / 30 s ≈ 167 uploads/s, ≈ 3,333 candidate records/s at 20 candidates |
| Controller memory | instances × candidates retained | `--max-instances`, `--max-candidates`, `--max-global-keys` |

### Why per-instance admission matters at this scale

With conventional client-side caching, each instance tracks every key it reads. In the Zipf benchmark (100k keys, one instance), conventional caching made Redis track **tens of thousands** of keys for that one instance, while KeyShift tracked **a few hundred** at a similar reduction in Redis GETs (see [benchmarks.md](benchmarks.md)). Multiplied by 5,000 instances, that is the difference between a tracking table of hundreds of millions of key-client pairs (far beyond the default `tracking-table-max-keys` of 1,000,000, which triggers evictions and invalidation storms) and one of a few million.

The same logic applies to invalidations: a write to a key tracked by N instances produces N push messages. KeyShift keeps N small for write-heavy keys by not caching them.

## Controller capacity

Logical simulation results ([benchmarks.md](benchmarks.md#large-scale-logical-simulation), one laptop, controller and simulator in one process):

| Measure | 20 candidates/batch | 50 candidates/batch |
|---|---:|---:|
| uploads at the target pace (167/s over 30 s), p99 latency | 4.3–4.9 ms, 0 errors | – |
| maximum ingest rate | ≈ 8,000 uploads/s (≈ 160k records/s) | ≈ 7,000–8,000 uploads/s (≈ 390k records/s) |
| aggregation of 5,000 instances | 88–104 ms | 214 ms |
| average upload size (gzip) | 1.3 KiB | 2.4 KiB |
| process heap (controller + simulator) | 69–73 MiB | 146 MiB |

At 167 uploads/s × 1.3 KiB the controller receives about 220 KiB/s of telemetry. The default limits (`--max-instances 20000`, `--max-candidates 50`, `--max-global-keys 5000`, `--ingest-rate 2000/s`) are sized for the target with headroom. The aggregation step is O(instances × candidates) every `--aggregate-interval`.

Operational recommendations at 5,000 instances:

- Keep `Telemetry.Interval` at 30 s or more. The reporter adds ±20% jitter and spreads the first upload over a whole interval, so fleet-wide restarts do not synchronize.
- Keep `Telemetry.MaxCandidates` at 20 unless you need deeper rankings. Batches are gzip-compressed above 1 KiB.
- Run one controller replica with a persistent volume for policies. If it restarts, the global view rebuilds within one interval; SDKs are unaffected.
- Use one `FingerprintSecret` per administrative scope so keys aggregate across pods.

## Methodology and honesty

- **Lightweight CI mode** (`.github/workflows/ci.yml`): integration and fault tests against real Redis, a short benchmark smoke run, and a 500-instance simulation.
- **Large-scale mode**: `keyshift simulate` creates 5,000 *virtual* SDK instances in one process that upload synthetic telemetry batches (Zipf-distributed keys per service, a share of globally shared hot keys) to a real controller over HTTP. It measures ingestion throughput, latency, aggregation time, and memory. **It is a logical simulation of the control plane, not a deployment of 5,000 pods**, and it does not model Redis load from 5,000 instances.
- **Distributed verification** (not run for this release): `deploy/kubernetes/scale-test.yaml` runs parallel simulator Jobs against an in-cluster controller from separate network endpoints. A full verification would also run real application pods with the SDK against a production-sized Redis or Redis Cluster and observe `tracking_total_items`, `instantaneous_ops_per_sec`, and CPU on the Redis side.

## Redis-side planning

- Tracking table size ≈ Σ over instances of admitted keys; bounded by `MaxEntries` per instance (default 1,000) → at most 5,000,000 key-client pairs at 5,000 instances. Typical admitted counts are much lower (100–300 per instance in the benchmarks). Set `tracking-table-max-keys` accordingly.
- Each go-redis instance opens an additional tracked connection pool, so the connection count per Redis node grows by `instances × TrackedPoolSize`. Set `TrackedPoolSize` small (for example 4–8) at very high instance counts; the rueidis driver multiplexes over a few connections per node.
- On Redis Cluster, use the rueidis driver; tracking and invalidation are per node.
