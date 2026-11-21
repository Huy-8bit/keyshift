# Release readiness: KeyShift 0.1.0

Date: 2026-11-21. Verified on one developer machine (Apple M1 Pro, macOS, Docker via OrbStack, Go 1.26.11).

**Verdict: ready for an initial open-source release labelled pre-1.0 / experimental.** All release acceptance criteria that can be verified on a single machine are met. It is **not** claimed to be production-proven: there is no real multi-pod deployment, no real 5,000-pod test, and go-redis's client-side caching API is itself experimental. See "Not yet verified" below.

## Status legend

- **Implemented**: code exists and is wired into the product.
- **Tested**: covered by automated tests that pass (`make test-race`, `make test-integration`).
- **Benchmarked**: measured by `benchmarks/run-all.sh`, with results in docs/benchmarks.md.
- **Not yet verified**: implemented, or planned, but not validated in this environment.

## Acceptance criteria

### Core functionality

| Criterion | Status | Evidence |
|---|---|---|
| SDK importable by an external application | ✅ tested | `pkg/keyshift`; examples and GoDoc examples compile in a separate package |
| SDK works without the controller | ✅ tested | all SDK integration tests run without a controller; `TestControllerOutageDoesNotAffectRedisAccess` |
| Local hot-key detection with bounded resources | ✅ tested, benchmarked | detector tests (bounds, recall, fuzz); ~0.8 MiB state at defaults (`TestMemoryFootprint`) |
| Adaptive admission from real observations | ✅ tested | admission tests; `TestSDKEndToEnd`; lab demo |
| Local cache used for admitted reads | ✅ tested, benchmarked | hit counters in integration tests; benchmark hit ratios |
| Client tracking invalidates entries | ✅ tested | SET, DEL, expiry, FLUSHALL, cluster, slot migration |
| Explicit cache bypass | ✅ tested | `GetFresh`; deny patterns; explicit/off modes |
| Admission and eviction observable | ✅ tested | `Explain`, `Stats`, Prometheus metrics, telemetry, dashboard |
| Cache resource limits enforced | ✅ tested | budget tests (entries, memory, max entry size); rueidis store budget test |
| Controller ingests telemetry from many instances | ✅ tested, benchmarked | controller tests; end-to-end test; 5,000-instance simulation |
| Global and service-level aggregation | ✅ tested | `TestAggregationGlobalVersusLocal`; end-to-end test |
| Policy distribution | ✅ tested | controller, SDK, and end-to-end tests; verified live in the Docker lab |
| CLI executes real operations | ✅ verified manually | doctor, inspect, top, stats, instances, policy lifecycle, bench, simulate against real Redis and the lab |
| Dashboard displays real controller data | ✅ verified manually | screenshots of every view against live lab telemetry |

### Reliability

| Criterion | Status | Evidence |
|---|---|---|
| Redis restart recovery | ✅ tested | `TestRedisRestart` (docker restart) for both drivers; redis-restart benchmark (0 stale reads) |
| Tracking connection failures | ✅ tested | connection cut and silent partition (fault proxy) for both drivers |
| Concurrent read/write correctness | ✅ tested | concurrency integration tests under `-race`; stale-read measurement in every benchmark |
| Policy update concurrency | ✅ tested | engine and SDK concurrency tests under `-race` |
| Controller outage does not break Redis access | ✅ tested, benchmarked | integration test; controller-outage benchmark |
| Unsupported features degrade gracefully | ✅ tested | go-redis on Cluster, DB≠0, ACL without CLIENT, Redis 7.4 without HOTKEYS |
| No goroutine leaks on shutdown | ✅ tested | `TestCloseStopsGoroutinesAndRejectsCalls`; `TestCloseWhileOperationsActive` |

### Quality

| Criterion | Status | Evidence |
|---|---|---|
| Builds | ✅ | `make build` (dashboard + 3 binaries + examples); Docker images build |
| Unit tests | ✅ | `make test` |
| Race tests | ✅ | `make test-race` |
| Redis integration tests | ✅ | `make test-integration`; matrix on Redis 6.2, 7.4, 8.4, 8.6 and 8.6 Cluster |
| Lint and static checks | ✅ | gofmt, go vet, golangci-lint (errcheck, govet, staticcheck, gosec, …), tsc; govulncheck: no vulnerabilities |
| Fuzzing | ✅ | 5 fuzz targets, 20 s each, no failures |
| Examples compile and run | ✅ | basic, standalone (in the lab), redis-cluster, adaptive-caching |
| No placeholder production endpoints | ✅ | every API route is implemented and tested |
| Docker-based development | ✅ | `make test-infra-up`, `make docker-up` verified |
| CI configured | ⚠️ configured, not executed | `.github/workflows/ci.yml`, `release.yml`; the repository has no remote, so GitHub Actions has not run |
| Documentation matches behavior | ✅ reviewed | docs updated whenever verification changed behavior; numbers come from `benchmarks/results/` |

### Performance

| Criterion | Status | Evidence |
|---|---|---|
| vs uncached Redis | ✅ benchmarked | 17 scenarios, closed and open loop |
| vs conventional client-side caching | ✅ benchmarked | same suite, `csc` mode |
| Read-heavy and write-heavy workloads | ✅ benchmarked | read-only-zipf, zipf, local-hot, write-heavy |
| CPU and memory overhead | ✅ benchmarked | client CPU/op, alloc B/op, Redis CPU, cache memory, engine footprint |
| Tracking overhead | ✅ benchmarked | `tracking_total_keys`, invalidations per run |
| Large-scale logical simulation | ✅ benchmarked | 5,000 virtual instances, three configurations |
| Reproducible and honestly reported | ✅ | `benchmarks/run-all.sh`; raw JSON committed; unfavorable results and the cases where conventional CSC wins are reported |

## Not yet verified

- Real deployments with many application pods; distributed scale testing (`deploy/kubernetes/scale-test.yaml` was written but not run).
- Kubernetes manifests (validated by review only; no cluster was available).
- GitHub Actions workflows and the GoReleaser configuration (no remote repository).
- Redis Sentinel, Redis Enterprise, Valkey, managed cloud Redis, TLS to Redis, proxies, and real cluster node failover with replicas.
- Long-running soak tests (hours or days) for memory stability.
- Performance on Linux servers with real networks.

## Release blockers

None for a pre-1.0 release. Before 1.0: run CI on GitHub, publish images, run a distributed scale test, add a soak test, and track the go-redis client-side caching API out of experimental status.
