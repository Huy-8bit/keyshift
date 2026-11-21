# Compatibility

## Toolchain and dependencies

| Component | Version | Notes |
|---|---|---|
| Go | 1.26+ (module `go 1.26.0`, toolchain pinned to 1.26.11) | required by go-redis v9.23 and rueidis v1.0.80; CI matrix: 1.26.x, 1.27.x |
| github.com/redis/go-redis/v9 | v9.23.0 | client-side caching API is marked *experimental* by go-redis |
| github.com/redis/rueidis | v1.0.80 | |
| github.com/prometheus/client_golang | v1.25.0 | |
| Dashboard | React 19.3, TypeScript 5.9, Vite 6.4 | built into the controller binary |

KeyShift depends on documented extension points: `redis.Options.ClientSideCache` with `redis.NewLocalCache`, `rueidis.ClientOption.NewCacheStoreFn` with `rueidis.NewSimpleCacheAdapter`, and `Completed.Pin`. The go-redis cache namespace layout used to map invalidated keys back to application keys is covered by integration tests for the pinned version (`TestGoRedisMissThenHitAndKeyNamespace`). **Upgrade go-redis only together with a passing `make test-integration`.**

## Tested Redis servers

`make test-integration` and the compatibility matrix run the adapter, SDK, fault, and end-to-end suites with the race detector against:

| Server | Result |
|---|---|
| Redis 6.2 (standalone) | ✅ all suites pass |
| Redis 7.4 (standalone) | ✅ all suites pass; HOTKEYS correctly reported unsupported |
| Redis 8.4 (standalone) | ✅ all suites pass |
| Redis 8.6.8 (standalone) | ✅ all suites pass, including HOTKEYS |
| Redis 8.6.8 Cluster (3 masters) | ✅ rueidis caching, invalidation across nodes, slot migration (MOVED), go-redis detection-only mode |

Not tested: Redis Sentinel (go-redis `FailoverClient` should behave like a standalone client but is unverified), Redis Enterprise, Valkey, cloud-managed Redis variants, Redis behind proxies such as Envoy or twemproxy, and replicas as read endpoints. Contributions with test results are welcome.

## Capability matrix

| Situation | Caching | Detection, metrics, telemetry | Reported state |
|---|---|---|---|
| standalone, RESP3, `CLIENT` allowed | ✅ | ✅ | `healthy` |
| go-redis `ClusterClient` | ❌ (no go-redis cluster CSC) | ✅ | `unsupported` |
| rueidis Cluster | ✅ | ✅ | `healthy` |
| go-redis with `DB != 0` | ❌ (go-redis restriction) | ✅ | `unsupported` |
| go-redis with a credentials provider | ❌ (go-redis disables CSC) | ✅ | `unsupported` |
| server without RESP3 (< 6.0) | ❌ | ✅ | `unsupported` |
| ACL denies `CLIENT TRACKING` / `CLIENT SETNAME` | ❌ | ✅ | `unsupported` |
| `Cache.Mode = Off` | ❌ | ✅ | `disabled` |

In every unsupported case reads go directly to Redis with normal semantics, and `Stats().Capabilities.Reason` explains why.

## Minimum ACL

Application user (go-redis driver): `+get +set +del +ping +info +hello +client|tracking +client|trackinginfo +client|setname +client|setinfo` on the relevant keys. The rueidis driver additionally sends `CLIENT CACHING` and, for cluster, `CLUSTER SLOTS`/`CLUSTER SHARDS`.

HOTKEYS diagnostics need `+hotkeys` on a separate operator user. The SDK never requires administrator commands.

## Versioning policy

KeyShift follows semantic versioning. Before 1.0, minor releases may change public APIs; changes are listed in [CHANGELOG.md](../CHANGELOG.md). The telemetry schema is versioned (`schema_version: 1`), and the controller rejects unknown versions with a clear error. Policy documents reject unknown fields so typos are not silently ignored.
