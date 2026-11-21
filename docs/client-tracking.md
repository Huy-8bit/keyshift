# Redis client tracking in KeyShift

KeyShift uses Redis **server-assisted client-side caching** (`CLIENT TRACKING`, Redis 6.0+, RESP3). This is a Redis feature implemented by the client libraries; KeyShift neither invented it nor reimplements it.

## Modes Redis offers

| Mode | Redis tracks | Cost |
|---|---|---|
| default | every key read by the connection | tracking table grows with everything the client reads; invalidation for every one of those keys |
| `OPTIN` | only keys read right after `CLIENT CACHING YES` | proportional to what is actually cached |
| `BCAST` | prefixes, not keys | every write under a prefix is broadcast to every subscribed client: expensive with thousands of pods |

KeyShift aims for `OPTIN`-like cost and never uses `BCAST`.

## How each driver achieves it

**rueidis**: native `CLIENT TRACKING ON OPTIN`. Admitted keys use `DoCache` (rueidis sends `CLIENT CACHING YES` first); all other reads use `Do`, which Redis does not track.

**go-redis v9.23**: the built-in client-side cache enables default-mode tracking on every connection of a client and caches every cacheable read. KeyShift creates a **separate, small client** with the cache enabled and sends only admitted keys through it. Non-admitted reads use the untracked application client. Redis therefore tracks exactly the keys that are cached, which `TestGoRedisServerTracksOnlyCachedKeys` verifies against a real server: 200 direct reads plus 1 cached read add exactly 1 key to `tracking_total_keys`.

## Invalidation sources

| Event | Behavior |
|---|---|
| another client `SET`/`DEL`/modifies the key | Redis pushes `invalidate`; the entry is dropped |
| key expires (active or lazy expiry) | Redis pushes `invalidate` |
| `FLUSHALL` / `FLUSHDB` | Redis pushes a flush-all invalidation |
| write through this KeyShift client | the entry is dropped right after the write returns (`Forget`); the server invalidation also arrives |
| tracked connection closes | the client library evicts entries fetched over that connection |
| tracking-table pressure (`tracking-table-max-keys`) | Redis evicts tracked keys and sends invalidations |
| entry older than `max_staleness` | refetched on next access |

## Measured windows

Measured on Redis 8.6 with go-redis v9.23 and rueidis v1.0.80 on a single machine (see `pkg/keyshift/integration_test.go` and `internal/rediscompat/*_integration_test.go`):

| Situation | Stale reads observed after the triggering event |
|---|---|
| external write, connection healthy | converges within a few ms; maximum version lag of 1–4 writes at ~1 write/ms in concurrency tests. With the go-redis driver, invalidations on *idle* tracked connections are applied by a drainer every `Tracking.DrainInterval` (KeyShift default 1 ms) |
| server closes the tracked connection, then a write happens | 0 to ~1.8 ms (until the client notices the closed socket) |
| silent network partition (no FIN/RST) | ~140–175 ms with the default-style probe settings used in the test (100 ms interval, 100 ms timeout); in production bounded by `HealthInterval + HealthTimeout` (default ≈ 1.6 s) |
| Redis restart without persistence | none observed: old values were never served after the restart |

The partition case shows why KeyShift has its own health monitor. go-redis does not apply context deadlines to socket I/O unless `ContextTimeoutEnabled` is set, so without the monitor's own timeout a stale entry would be served until `max_staleness` (this was found and fixed during development; see `TestProbeIgnoringContextStillTimesOut`).

## Server settings worth knowing

- `tracking-table-max-keys` (default 1,000,000 in Redis 8.x): when exceeded, Redis evicts tracked keys and sends invalidations. KeyShift keeps the table small by tracking only admitted keys; `keyshift doctor` prints the setting.
- `CLIENT TRACKING` requires the `@connection` ACL category commands `CLIENT TRACKING`, `CLIENT TRACKINGINFO`, and for go-redis `CLIENT SETNAME`/`CLIENT SETINFO` on connect. Without them KeyShift disables caching and reports why.
- RESP3 (`HELLO 3`) is required. Proxies that do not support RESP3 push messages cannot be used for the cached path.
