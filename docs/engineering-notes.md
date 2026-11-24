# Engineering notes

Background for contributors: facts about the Redis client libraries that shaped KeyShift's design, and defects found while verifying the first release. Each defect is covered by a regression test.

## Verified facts that shape the design

| Fact (verified in source and with integration tests) | Consequence |
|---|---|
| go-redis v9.23.0 has native client-side caching (experimental API): RESP3, `CLIENT TRACKING ON` (default mode) on every pooled connection, pluggable `redis.Cache` interface, `MaxStaleness` backstop, eviction of entries owned by a connection that loses tracking. | KeyShift does not implement tracking. It wraps go-redis `LocalCache` to observe fills and invalidations. |
| go-redis CSC caches every cacheable command once enabled and supports standalone clients only (not `ClusterClient`). | KeyShift routes only admitted keys to a dedicated CSC-enabled client; all other reads and writes use the application's untracked client. Redis therefore tracks only admitted keys (OPTIN-equivalent). Cluster caching uses rueidis. |
| rueidis v1.0.80 supports `CLIENT TRACKING ON OPTIN`, per-call `DoCache`, Redis Cluster, and a `SimpleCache` adapter. | rueidis adapter with a KeyShift-owned bounded store (global budget, per-key forget, statistics). |
| rueidis does not apply invalidations for a local write before the write returns (multiplexed connections). | Adapter drops local entries after local writes (`Forget`). |
| After a tracked connection is closed by the server, both clients stop serving the entry within ~1–2 ms (measured). A silent partition is not detected by the clients. | KeyShift health probe (`CLIENT TRACKINGINFO` with timeout) suspends caching and flushes on failure; `max_staleness` is the final backstop. |
| Redis 8.6 adds `HOTKEYS START/GET/STOP/RESET`; go-redis exposes it on standalone clients. | Optional, privileged CLI diagnostics only. |

## Defects found and fixed before 0.1.0

| Found by | Defect | Fix |
|---|---|---|
| rueidis read-your-writes test | rueidis does not apply the invalidation for a local write before the write returns (multiplexed connections) | KeyShift-owned rueidis store with per-key `Forget` |
| silent-partition fault test | the health probe hung because go-redis ignores context deadlines for socket I/O by default; stale values were served for the full partition | monitor enforces its own timeout and allows at most one in-flight probe |
| `doctor` on Cluster | copying a live go-redis client's `Options` shared its push processor with the tracked client | tracked client gets its own processor and maintenance config |
| race detector (connection-cut test) | rueidis builder buffer reused while its background writer still read it | pin all rueidis commands |
| Zipf benchmark | the admitted set was capped at TopK because incumbents filled the candidate list | evaluate TopK candidates beyond admitted keys |
| UI review (before the UI was replaced by Grafana) | telemetry hits measured at upload time while reads covered closed windows | snapshot counters at window rotation |
| lab demo | 645 admissions/demotions per minute from noisy short windows | EWMA and decayed-count smoothing (210/min) |
| benchmark stale-read detector (`local-hot`) | a fill in flight during a local write cached the pre-write value | fill guard rejects or re-drops overlapping fills |
| code review | `New` leaked its Redis client on invalid config; Redis errors on the cache path triggered a second request; `Explain` blamed tracking health when caching was unsupported | validate first; fall back only on transport errors; explicit unavailable reason |
| benchmark stale-read detector (`local-hot`, writes from other instances) | admitted keys mostly hit, so their tracked connections stay idle and go-redis applies invalidations only on its 5 ms drainer tick | KeyShift sets the drainer to 1 ms by default (`Tracking.DrainInterval`): 4–8× fewer stale reads for ~6% client CPU |
| unit tests without `-race` | detector concurrency test rotated before observers were scheduled | test waits for observations |
| minimum-ACL integration test (rueidis) | rueidis `DoCache` wraps reads in MULTI/PTTL/GET/EXEC; without those ACL permissions every cached read failed while tracking looked healthy | `ToStaticTTL` (freshness from invalidations + `max_staleness`, like go-redis; 3 fewer commands per miss); aborted cached fetches fall back to a direct read |
| simulation output review | a token configured for both ingest and viewer kept only its highest role, so every upload was rejected (403) | tokens hold the set of all configured roles |
| Redis-restart benchmark | server counters reset by the restart made the Redis GET/s delta underflow | reset detected; post-restart part reported as a lower bound |
| Docker lab policy change | named volume `/data` was root-owned while the distroless image runs as non-root, so policies could not be saved; the error was reported as HTTP 400 | image pre-creates `/data` owned by uid 65532; persistence failures return 500 and roll back |
| final integration run | telemetry reporter test stopped the reporter before it processed the last response | test waits on the client-side contact count |
| restarted test cluster container | stale `nodes.conf` made the cluster start script exit | test cluster always starts from clean node state |
| lab policy scenario | deleting a policy on the controller left SDKs on the deleted policy until `valid_for_seconds` expired | telemetry responses carry `no_policy`; SDKs revert to their local policy at once (verified in the lab: 22 s) |

## Defects found and fixed after 0.1.0

| Found by | Defect | Fix |
|---|---|---|
| GitHub Actions (3 failed runs) | the tracking monitor cleared its in-flight flag after delivering the probe result, so a check issued right after the previous one could see that probe as still pending and record a false failure (and delay recovery) | clear the flag before delivering the result; `TestBackToBackChecksDoNotReportPending` fails on the old code |
| cache-hit profiling | latency histograms were recorded on every read even when no Prometheus registerer was configured | the collector exists only when `Metrics.Registerer` is set |
| cache-hit profiling | `Get` copied every value twice on top of the client library (string → `[]byte` in the adapter, `[]byte` → string in `Get`): 16 KiB hits took 12.7 µs | adapters return strings; `Get` copies nothing extra (6.5–8.3 µs), `GetBytes` copies once |
| cache-hit profiling | every hit re-stored the value size in a shared atomic, invalidating the cache line on all cores | store only when the size changes |
| Valkey compatibility run | `doctor` printed Valkey's compatibility `redis_version` and an empty mode ("Redis 7.2.4, mode ") | `rediscompat.ParseServer` recognizes Valkey (`server_name`, `valkey_version`, `server_mode`) |
| CLI integration test | `inspect key` printed `-1ns` as the TTL of keys without expiry | render "no expiry" / "key does not exist" |

## Measured trade-off: the go-redis cache wrapper

KeyShift's go-redis adapter wraps `redis.LocalCache` (`gatedCache`) to count fills and invalidations per key and to reject fills that overlap a local write. go-redis checks for its own `*LocalCache` type before using its zero-copy hit path (`getShared`), batched invalidation, and its opt-in miss coalescing and refresh-on-invalidate; a wrapper therefore takes the generic path, which copies each hit. `go-redis` offers no hooks that would make the wrapper unnecessary (`CacheConfig` has none).

Parallel cache hits, Apple M1 Pro, Redis 8.6 in Docker (`BenchmarkGoRedisCacheHit`, `BenchmarkCacheHit`):

| value size | go-redis `LocalCache` | go-redis + KeyShift wrapper | `keyshift.Get` (go-redis) | `keyshift.Get` (rueidis) |
|---|---|---|---|---|
| 64 B | 272 ns | 341 ns | 375 ns | 282 ns |
| 1 KiB | 527 ns | 874 ns | 925 ns | 270 ns |
| 16 KiB | 3.5 µs | 6.6–7.2 µs | 6.5–8.3 µs | 271 ns |

For small values the SDK's own work on top of the client library (admitted-set lookup, striped counters) costs about 35–50 ns per hit. At 16 KiB the time is dominated by allocating the copied value: a CPU profile shows about a third of the time in the Go heap's span-allocation lock, and repeated runs of the same benchmark vary by about 25%, so the difference between the last two go-redis columns at that size is within run-to-run noise. The correctness benefit of the wrapper (0 stale reads after local writes, versus 96 per 1.7 M reads in the `local-hot` benchmark without the fill guard) outweighs the extra copy; for large values, the rueidis driver avoids it entirely.
