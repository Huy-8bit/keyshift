<h1 align="center">KeyShift</h1>

<p align="center">
  <strong>Detect hot keys. Cache intelligently. Scale efficiently.</strong><br>
  Adaptive, selective client-side caching for Redis in Go.
</p>

<p align="center">
  <a href="https://github.com/Huy-8bit/keyshift/actions/workflows/ci.yml"><img src="https://github.com/Huy-8bit/keyshift/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://pkg.go.dev/github.com/Huy-8bit/keyshift/pkg/keyshift"><img src="https://pkg.go.dev/badge/github.com/Huy-8bit/keyshift/pkg/keyshift.svg" alt="Go Reference"></a>
  <a href="https://goreportcard.com/report/github.com/Huy-8bit/keyshift"><img src="https://goreportcard.com/badge/github.com/Huy-8bit/keyshift" alt="Go Report Card"></a>
  <a href="go.mod"><img src="https://img.shields.io/badge/go-1.26%2B-00ADD8?logo=go" alt="Go version"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" alt="License"></a>
  <img src="https://img.shields.io/badge/status-pre--1.0-orange" alt="Status: pre-1.0">
</p>

KeyShift is a Go SDK and optional control plane that **automatically finds the Redis keys worth caching in each application instance** and serves them from local memory. Invalidation comes from Redis server-assisted client-side caching (`CLIENT TRACKING`). Everything else goes straight to Redis.

> **Status: 0.1.0, pre-1.0.** The SDK, controller, CLI, Grafana dashboards, and lab are implemented and tested against real Redis (6.2 – 8.6, standalone and Cluster). APIs may change before 1.0. Please read [Limitations](docs/limitations.md) and the [Consistency contract](docs/consistency.md) before production use.

## Table of contents

- [Why KeyShift](#why-keyshift)
- [Features](#features)
- [How it compares](#how-it-compares)
- [Installation](#installation)
- [Quick start](#quick-start)
- [Usage](#usage)
- [Try the lab with Docker](#try-the-lab-with-docker)
- [Configuration](#configuration)
- [Architecture](#architecture)
- [Benchmarks](#benchmarks)
- [Compatibility](#compatibility)
- [Documentation](#documentation)
- [Roadmap](#roadmap)
- [Contributing](#contributing)
- [Security](#security)
- [License](#license)
- [Acknowledgments](#acknowledgments)

## Why KeyShift

Redis client-side caching can remove most reads of hot keys from Redis, but turning it on for everything has costs that grow with the size of a fleet:

- **Write-heavy keys churn.** Every write invalidates every cached copy, and readers can see stale values while invalidations are in flight.
- **Redis tracks every key you read.** With default tracking, the server's tracking table and invalidation traffic grow with each instance's whole read set.
- **Globally hot is not locally hot.** A key read 2,000 times per second across 50 pods may be read only 40 times per second by each pod.

KeyShift makes the decision per instance, from measured workload: it caches a key only when the local read rate and the observed hit ratio justify it. Then it keeps measuring, and backs off when invalidations show that caching does not pay.

## Features

- **Drop-in Go SDK**: `Get`, `GetBytes`, `GetFresh`, `Set`, `Del`. It can wrap your existing go-redis client and works without any extra infrastructure.
- **Hot-key detection**: a bounded, lock-sharded Space-Saving top-K detector with window decay, zero allocations per read, and fixed memory.
- **Adaptive admission**: read rate, predicted and observed hit ratio, write and invalidation rates, value size, budgets, hysteresis, and exponential cooldown. `Explain(key)` tells you why a key is or is not cached.
- **Built on mature clients**: go-redis v9 (standalone) and rueidis (standalone and Redis Cluster, `OPTIN` tracking). No custom RESP or tracking protocol code.
- **Safety first**: caching is suspended and the local cache flushed when tracking cannot be confirmed. There is a hard `max_staleness` bound, deny patterns, and an authoritative `GetFresh`.
- **Observability**: Prometheus metrics with no key labels, structured logs, and per-key decisions.
- **Optional controller**: fleet-wide hot-key ranking (local vs global), versioned policies with staged rollout and rollback, a REST API, and Prometheus fleet metrics.
- **Prometheus and Grafana**: the controller exports the whole fleet view (workload, per-service breakdown, global hot keys, cache efficiency, policies, warnings) as Prometheus metrics; ready-made Grafana dashboards are included.
- **CLI**: `keyshift doctor | inspect | top | stats | instances | policy | bench | simulate`.
- **Redis 8.6 `HOTKEYS` support**: opt-in, time-bounded server-side diagnostics.
- **Lab**: a Docker Compose stack, fault-injection tests, a reproducible benchmark suite, and a 5,000-instance control-plane simulation.

## How it compares

| | go-redis client-side cache | rueidis `DoCache` | KeyShift |
| --- | --- | --- | --- |
| Invalidation protocol | ✅ built in | ✅ built in | reuses the client library's |
| What gets cached | every cacheable read | whatever the caller chooses | keys admitted by measured local benefit |
| Keys tracked by Redis | every key read | cached keys (`OPTIN`) | cached keys only, on both drivers |
| Write-heavy keys | cached anyway | cached if requested | rejected or put in cooldown |
| Hot-key detection | – | – | ✅ |
| Silent network partition | bounded by `MaxStaleness` | bounded by client TTL | health probe suspends caching (~0.2 s in tests) |
| Fleet view and policies | – | – | ✅ optional controller |

KeyShift does not replace these libraries. It decides *what* they should cache.

## Installation

```bash
go get github.com/Huy-8bit/keyshift@latest
```

Requirements:

- Go **1.26** or newer
- Redis **6.0+** with RESP3 for local caching (tested on 6.2, 7.4, 8.4, 8.6)

Optional tools:

```bash
go install github.com/Huy-8bit/keyshift/cmd/keyshift@latest             # CLI
go install github.com/Huy-8bit/keyshift/cmd/keyshift-controller@latest  # optional controller
```

## Quick start

```go
package main

import (
    "context"
    "errors"
    "fmt"
    "log"

    "github.com/Huy-8bit/keyshift/pkg/keyshift"
)

func main() {
    client, err := keyshift.New(keyshift.Config{
        ServiceName: "product-service",
        Redis:       keyshift.RedisConfig{Addr: "localhost:6379"},
        Cache: keyshift.CacheConfig{
            DenyPatterns: []string{"balance:*", "lock:*"}, // never cache these
        },
    })
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    ctx := context.Background()
    v, err := client.Get(ctx, "product:123")
    switch {
    case errors.Is(err, keyshift.Nil):
        fmt.Println("not found")
    case err != nil:
        log.Fatal(err)
    default:
        fmt.Println(v)
    }

    // Why is this key (not) cached?
    fmt.Println(client.Explain("product:123").Reason)
}
```

Check that your Redis supports everything KeyShift needs:

```bash
keyshift doctor --redis localhost:6379
```

## Usage

### Keep your existing go-redis client

```go
rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", PoolSize: 50})
ks, err := keyshift.NewFromGoRedis(rdb, keyshift.Config{ServiceName: "product-service"})
// KeyShift never reconfigures or closes rdb.
```

### Redis Cluster

```go
ks, err := keyshift.NewFromRueidis(
    rueidis.ClientOption{InitAddress: []string{"10.0.0.1:6379", "10.0.0.2:6379"}},
    keyshift.Config{ServiceName: "catalog"},
)
```

### Authoritative reads

```go
balance, err := client.GetFresh(ctx, "balance:42") // always reads Redis
```

### Prometheus metrics

```go
reg := prometheus.NewRegistry()
client, _ := keyshift.New(keyshift.Config{
    ServiceName: "svc",
    Redis:       keyshift.RedisConfig{Addr: "localhost:6379"},
    Metrics:     keyshift.MetricsConfig{Registerer: reg},
})
```

### Optional controller

```go
Telemetry: keyshift.TelemetryConfig{
    Endpoint:          "https://keyshift-controller:8080",
    Token:             os.Getenv("KEYSHIFT_INGEST_TOKEN"),
    FingerprintSecret: []byte(os.Getenv("KEYSHIFT_FINGERPRINT_SECRET")), // keys are sent as HMAC fingerprints
},
```

More examples are in [`examples/`](examples/): a minimal program, an HTTP service with dependency injection, Redis Cluster, and a multi-instance adaptive demo.

## Try the lab with Docker

```bash
git clone https://github.com/Huy-8bit/keyshift.git && cd keyshift
make docker-up       # Redis 8.6 + controller + SDK services + Prometheus + Grafana
make lab             # or: start everything and run all lab scenarios with a report
```

Open Grafana at <http://localhost:3000> (dashboards **KeyShift Fleet** and **KeyShift SDK**) and Prometheus at <http://localhost:9090>, or use the CLI with a token from [`lab/lab.env`](lab/lab.env):

```bash
export KEYSHIFT_CONTROLLER=http://localhost:8080 KEYSHIFT_TOKEN=lab-viewer-token-change-me
keyshift stats
keyshift top --watch 5s
make docker-down
```

The [lab guide](lab/README.md) describes every scenario, how to explore by hand, and how to break things on purpose.

## Configuration

Every setting has a default. Remote policies can tighten local limits, never loosen them.

| Setting | Default | Description |
| --- | --- | --- |
| `Detection.Window` | `10s` | observation window |
| `Detection.TopK` | `100` | new candidates evaluated per window |
| `Detection.MaxTrackingCandidates` | `1000` | detector and admission memory bound |
| `Cache.Mode` | `adaptive` | `adaptive`, `explicit` (pins only), `off` |
| `Cache.MaxMemoryBytes` / `MaxEntries` | `16 MiB` / `1000` | local cache budget |
| `Cache.MaxEntryBytes` | `64 KiB` | largest cached value |
| `Cache.MaxStaleness` | `3s` | hard staleness bound per entry |
| `Cache.MinReadRate` / `MinHitRatio` / `ExitHitRatio` | `2/s`, `0.6`, `0.3` | admission thresholds with hysteresis |
| `Tracking.HealthInterval` / `HealthTimeout` | `1s` / `500ms` | tracking health probe |
| `Telemetry.Endpoint` | empty | controller URL (telemetry is off by default) |

See the [configuration reference](docs/configuration.md) for every option, the policy format, and controller flags.

## Architecture

```text
  Get(key) ─► keyshift.Client
               ├─ admitted?  ── yes ─► tracked client-side cache ─(miss)─► Redis (tracked)
               │             └─ no ──► your Redis client ────────────────► Redis (untracked)
               ├─ hot-key detector (bounded top-K)
               ├─ admission engine (per-window scoring, budgets, cooldown)
               ├─ tracking health monitor (suspend + flush on failure)
               └─ telemetry (optional, batched, fingerprints only) ──► keyshift-controller
                                                                      ├─ global ranking
                                                                      ├─ versioned policies
                                                                      ├─ API ◄─ CLI
                                                                      └─ /metrics ─► Prometheus ─► Grafana
```

The controller is never on the Redis request path. If it is down, applications keep working with their last valid or local policy. Read more in [Architecture](docs/architecture.md) and [How it works](docs/how-it-works.md).

## Benchmarks

Measured on one laptop (Apple M1 Pro, Redis 8.6 in Docker), open loop at 20,000 ops/s, Zipf workload over 100k keys:

| Mode | Redis GET/s | Redis CPU (10 s) | Keys tracked by Redis | Invalidations | Hit ratio |
| --- | ---: | ---: | ---: | ---: | ---: |
| No local cache | 19,006 | 2.04 s | 0 | 0 | 0% |
| Conventional client-side cache | 5,784 | 1.06 s | 32,885 | 5,739 | 69.6% |
| **KeyShift** | 6,529 | 1.01 s | **341** | **92** | 65.7% |

On a write-heavy workload, conventional caching served 6.9% stale reads; KeyShift did not cache those keys and served none. Conventional caching adapts faster to shifting hot sets and cold starts.

Full results, methodology, unfavorable workloads, and the 5,000-instance simulation are in [docs/benchmarks.md](docs/benchmarks.md). Reproduce them with `./benchmarks/run-all.sh`.

## Compatibility

| Server | go-redis driver | rueidis driver | HOTKEYS |
| --- | --- | --- | --- |
| Redis 6.2, 7.4, 8.4, 8.6 (standalone) | ✅ caching (DB 0) | ✅ caching | 8.6+ |
| Redis Cluster 8.6 | detection only | ✅ caching | per node |
| No RESP3 or no `CLIENT` permission | direct reads | direct reads | – |

Details, minimum ACL, and the versioning policy are in [docs/compatibility.md](docs/compatibility.md).

> ⚠️ **Consistency:** client-side caching is eventually consistent. Do not cache balances, locks, rate limits, inventory, or authorization data; use deny patterns or `GetFresh`. See [docs/consistency.md](docs/consistency.md).

## Documentation

| Topic | |
| --- | --- |
| Getting started | [Quickstart](docs/quickstart.md) · [Configuration](docs/configuration.md) · [Examples](examples/) |
| Concepts | [How it works](docs/how-it-works.md) · [Architecture](docs/architecture.md) · [Client tracking](docs/client-tracking.md) · [Hot-key detection](docs/hotkey-detection.md) · [Adaptive admission](docs/adaptive-admission.md) |
| Operations | [Consistency](docs/consistency.md) · [Scaling](docs/scaling.md) · [Observability](docs/observability.md) · [Security model](docs/security-model.md) · [Troubleshooting](docs/troubleshooting.md) |
| Project | [Benchmarks](docs/benchmarks.md) · [Compatibility](docs/compatibility.md) · [Limitations](docs/limitations.md) · [Release readiness](docs/release-readiness.md) · [Engineering notes](docs/engineering-notes.md) · [Design decisions](docs/adr/) · [Changelog](CHANGELOG.md) |

## Roadmap

- Cache more read commands (`HGET`, `MGET`, …) with verified invalidation semantics
- Distributed scale tests on Kubernetes and long-running soak tests
- Highly available controller and SSO for the controller API
- Redis Sentinel, Valkey, and managed-Redis verification
- A stable 1.0 API

Ideas and feedback are welcome in [issues](https://github.com/Huy-8bit/keyshift/issues).

## Contributing

Contributions are welcome! Please read [CONTRIBUTING.md](CONTRIBUTING.md) for the development setup and ground rules, and follow the [Code of Conduct](CODE_OF_CONDUCT.md).

```bash
make lint test-race        # quick checks
make test-integration      # real Redis via Docker
```

## Security

Please do **not** report security vulnerabilities in public issues. See [SECURITY.md](SECURITY.md) for private reporting.

## License

KeyShift is licensed under the [Apache License 2.0](LICENSE). Third-party notices are listed in [NOTICE](NOTICE).

## Acknowledgments

KeyShift builds on the excellent [go-redis](https://github.com/redis/go-redis) and [rueidis](https://github.com/redis/rueidis) clients, and on Redis's server-assisted [client-side caching](https://redis.io/docs/latest/develop/reference/client-side-caching/).
