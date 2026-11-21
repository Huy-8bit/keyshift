# KeyShift Lab

A self-contained, reproducible environment to **see KeyShift work end to end** on your machine: hot-key detection, adaptive caching, invalidation, policies, fault tolerance, and observability. It runs entirely in Docker.

```text
                     ┌──────────────┐   HTTP    ┌───────────────────┐
                     │   traffic    │ ────────► │  product-service  │──┐  /metrics
                     └──────────────┘           │ (KeyShift + go-   │  │
                                                │  redis, :8090)    │  │
┌──────────────────────────────────────┐        └─────────┬─────────┘  │
│ demo: 6 simulated pods               │                  │            │
│   catalog-api ×3, checkout-api ×3    │──────┐           │            │
│   read-heavy catalog:*, write-heavy  │      │           ▼            ▼
│   stock:*, pod-local session keys    │      │     ┌──────────┐  ┌────────────┐
└──────────────────────────────────────┘      ├───► │ Redis 8.6│  │ Prometheus │
                 │ telemetry every 10–30 s    │     │  (:6379) │  │  (:9090)   │
                 ▼                            │     └──────────┘  └────────────┘
       ┌──────────────────────────────┐       │                        ▲
       │ keyshift-controller (:8080)  │◄──────┘ telemetry              │
       │ API, policies, fleet metrics │────────────────────────────────┘ /metrics
       └──────────────────────────────┘
```

| Service | Purpose | Port |
|---|---|---|
| `redis` | Redis 8.6 shared by every service | 6379 |
| `controller` | KeyShift controller: telemetry, policies, API, fleet metrics | 8080 |
| `demo` | six simulated pods in two services with different key patterns | – |
| `product-service` | a real HTTP service that wraps its own go-redis client with KeyShift | 8090 |
| `traffic` | HTTP load for `product-service` (80% of requests on 5 hot products) | – |
| `prometheus` | scrapes the controller (fleet metrics) and the SDK metrics of `product-service` | 9090 |
| `grafana` | dashboards **KeyShift Fleet** and **KeyShift SDK**, provisioned automatically | 3000 |
| `cli`, `redis-bench` | on-demand tools (`tools` profile); benchmarks use their own Redis | – |

## Requirements

- Docker with Compose v2 (Docker Desktop, OrbStack, or Docker Engine)
- `curl` and `jq` on the host (used by `run.sh`)
- About 2 GB of free memory; free ports 3000, 6379, 8080, 8090, 9090

## Run everything

From the repository root:

```bash
./lab/run.sh            # or: make lab
```

The script builds the images, starts the lab, runs every scenario below with assertions, and writes a report with all evidence to `lab/results/<timestamp>/REPORT.md`. The first run takes about 10 minutes, mostly image builds.

Options: `--no-build` reuses images; `--down` stops the lab and removes its volumes at the end.

## Scenarios

| # | Scenario | What it shows | Pass criteria |
|---|---|---|---|
| 1 | Build and start | the lab runs from scratch | every service healthy |
| 2 | Compatibility check | `keyshift doctor` against Redis and the controller | no failed checks |
| 3 | Steady state | 7 SDK instances detect hot keys and cache them | all instances healthy, local hit ratio > 30% |
| 4 | SDK metrics and `Explain` | Prometheus metrics of a real service; why `product:1` is cached | hits > 0, `product:1` admitted, **no key names in metric labels** |
| 5 | Per-pod adaptive decisions | hot `catalog:0` is cached everywhere; write-heavy `stock:0` is kept out or put in cooldown | `catalog:0` admitted on all 6 pods |
| 6 | Policies | staged rollout (50% → 100%) of "never cache `stock:*`" to checkout-api; `explicit` mode for catalog-api; deletion reverts pods to their local policy | all 3 checkout pods bypass `stock:*`; catalog pods cache ≤ 3 pinned keys; pods revert after deletion |
| 7 | Controller outage | the controller is stopped; applications keep serving | 500/500 requests succeed, 0 read errors |
| 8 | Redis restart | Redis restarts (no persistence); caching is suspended, flushed, and resumes | service recovers, tracking healthy again |
| 9 | Killed connections | every client connection is killed | all instances recover |
| 10 | Redis HOTKEYS | Redis 8.6 server-side top keys by CPU and network | session completes, hot keys reported |
| 11 | Benchmark smoke | none vs conventional caching vs KeyShift on an isolated Redis | completes |
| 12 | Prometheus and Grafana | both dashboards provisioned; fleet metrics in Prometheus | datasource healthy, ≥ 7 active instances and hot keys in Prometheus |
| 13 | Final snapshot | stats, ranking, instances from the CLI | snapshot saved |

## Explore by hand

Start the lab without running scenarios:

```bash
docker compose -f lab/docker-compose.yml up -d --build --wait     # or: make docker-up
```

**Grafana**: open <http://localhost:3000> (anonymous read-only access; sign in as `admin` / `keyshift-lab` to edit). The home dashboard is **KeyShift Fleet**: instances, workload, per-service breakdown, global hot keys, cache efficiency, instances, policies, warnings, and controller health. **KeyShift SDK** shows the metrics a service exports directly (hit/miss, latency, admission decisions, tracking state).

**Controller API**: <http://localhost:8080/api/v1/overview> with a token from [`lab.env`](lab.env) (`lab-viewer-token-change-me` to read, `lab-admin-token-change-me` to manage policies).

**CLI** (runs inside the lab network, nothing to install):

```bash
alias ks='docker compose -f lab/docker-compose.yml run --rm -T cli'
ks stats     --controller http://controller:8080 --token lab-viewer-token-change-me
ks top       --controller http://controller:8080 --token lab-viewer-token-change-me --watch 5s
ks instances --controller http://controller:8080 --token lab-viewer-token-change-me
ks doctor    --redis redis:6379 --skip-controller
ks inspect hotkeys --redis redis:6379 --duration 10s
```

**SDK view from inside a service**:

```bash
curl -s localhost:8090/debug/keyshift/stats | jq '{reads, local_hits, admitted_keys, tracking: .tracking.state}'
curl -s "localhost:8090/debug/keyshift/explain?key=product:1" | jq
curl -s localhost:8090/metrics | grep '^keyshift_cache'
```

**Per-pod decisions** (printed by the demo every window):

```bash
docker compose -f lab/docker-compose.yml logs -f demo
```

**Prometheus** at <http://localhost:9090>, for example:

```promql
sum(rate(keyshift_cache_hits_total[1m]))                 # Redis reads avoided per second
keyshift_tracking_healthy                                # 1 = caching allowed
sum by (decision) (rate(keyshift_admission_decisions_total[5m]))
keyshift_controller_active_instances
```

### Try a policy yourself

```bash
docker compose -f lab/docker-compose.yml run --rm -T -v "$PWD/lab/policies:/policies:ro" cli \
  policy set --controller http://controller:8080 --token lab-admin-token-change-me \
  --service checkout-api -f /policies/checkout-deny-stock.json --rollout 50
```

Then watch the `STOCK:0` column change to `bypassed` in `docker compose -f lab/docker-compose.yml logs -f demo`, raise the rollout with `policy rollout --percent 100`, and remove it with `policy delete`. The policy table and the "Policy versions in use" panel of the Grafana **KeyShift Fleet** dashboard show the rollout as it happens.

### Break things

```bash
docker compose -f lab/docker-compose.yml stop controller      # applications keep working
docker compose -f lab/docker-compose.yml start controller
docker compose -f lab/docker-compose.yml restart redis        # caching suspends and recovers
docker compose -f lab/docker-compose.yml exec redis redis-cli CLIENT KILL TYPE normal SKIPME yes
```

## Clean up

```bash
docker compose -f lab/docker-compose.yml --profile tools down -v     # or: make docker-down
```

## Files

| File | Purpose |
|---|---|
| `docker-compose.yml` | the lab stack |
| `run.sh` | automated scenarios and report |
| `lab.env` | lab-only tokens and fingerprint secret (never reuse them) |
| `policies/` | example policies used by scenario 6 |
| `prometheus/prometheus.yml` | scrape configuration |
| `grafana/` | datasource and dashboard provisioning, dashboard JSON |
| `results/` | reports from `run.sh` (git-ignored) |

The lab runs on a single machine, so it demonstrates behavior and safety properties rather than production performance. For performance methodology see [docs/benchmarks.md](../docs/benchmarks.md).
