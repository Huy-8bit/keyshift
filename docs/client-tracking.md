# Redis client tracking in KeyShift

KeyShift relies on Redis **server-assisted client-side caching** (`CLIENT TRACKING`, Redis 6.0+, RESP3). The Redis client libraries handle the tracking protocol and invalidation messages; KeyShift decides which keys use the cached path.

## Modes Redis offers

| Mode | Redis tracks | Cost |
|---|---|---|
| default | every key read by the connection | tracking table grows with everything the client reads; invalidation for every one of those keys |
| `OPTIN` | only keys read right after `CLIENT CACHING YES` | proportional to what is actually cached |
| `BCAST` | prefixes, not keys | every write under a prefix is broadcast to every subscribed client: expensive with thousands of pods |

KeyShift uses `OPTIN` directly with rueidis and a separate tracked client with go-redis to keep direct reads off the tracked path. It does not use `BCAST`.

## How each driver achieves it

**rueidis** uses native `CLIENT TRACKING ON OPTIN`. Admitted keys use `DoCache`, which sends `CLIENT CACHING YES`; other reads use `Do`.

**go-redis v9.23** enables default-mode tracking on the pooled connections of a cache-enabled client. KeyShift creates a separate cache-enabled client and sends only admitted reads through it; other reads use the application client. `TestGoRedisServerTracksOnlyCachedKeys` verifies this routing against Redis: 200 direct reads and one cached read add one key to `tracking_total_keys` in that test.

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
| silent network partition (no FIN/RST) | ~140–175 ms with the test's 100 ms interval and 100 ms timeout; actual detection depends on probe timing, ±10% jitter, and scheduling |
| Redis restart without persistence | none observed: old values were never served after the restart |

These measurements describe the tested environment, not a latency guarantee. The partition case also shows why KeyShift enforces its own probe timeout: go-redis does not apply context deadlines to socket I/O unless `ContextTimeoutEnabled` is set. See `TestProbeIgnoringContextStillTimesOut` and the [consistency contract](consistency.md).

## Server settings worth knowing

- `tracking-table-max-keys` (default 1,000,000 in Redis 8.x): when exceeded, Redis evicts tracked keys and sends invalidations. KeyShift keeps the table small by tracking only admitted keys; `keyshift doctor` prints the setting.
- `CLIENT TRACKING` requires the `@connection` ACL category commands `CLIENT TRACKING`, `CLIENT TRACKINGINFO`, and for go-redis `CLIENT SETNAME`/`CLIENT SETINFO` on connect. Without them KeyShift disables caching and reports why.
- RESP3 (`HELLO 3`) is required. Proxies that do not support RESP3 push messages cannot be used for the cached path.
