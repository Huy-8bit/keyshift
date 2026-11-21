# Examples

| Example | What it shows | Run |
|---|---|---|
| [basic](basic) | smallest program: admission within seconds, `Explain`, deny patterns, `GetFresh` | `go run ./examples/basic -redis localhost:6379` |
| [standalone](standalone) | HTTP service that keeps its own go-redis client (`NewFromGoRedis`), dependency injection, Prometheus `/metrics`, debug endpoints | `go run ./examples/standalone -redis localhost:6379` |
| [redis-cluster](redis-cluster) | Redis Cluster caching with the rueidis driver | `go run ./examples/redis-cluster -addrs 127.0.0.1:7001,...` |
| [adaptive-caching](adaptive-caching) | six simulated pods with read-heavy, write-heavy, and pod-local keys reporting to a controller; the lab's load generator | `go run ./examples/adaptive-caching -redis localhost:6379 -controller http://localhost:8080 -token <ingest>` |

All examples are compiled by `make build`. `make docker-up` runs `adaptive-caching` and `standalone` against the lab controller.
