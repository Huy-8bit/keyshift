# Examples

These programs demonstrate SDK integration patterns. Start a compatible Redis server before running them; the adaptive-caching example also needs a controller and an ingest token.

| Example | What it shows | Run |
|---|---|---|
| [basic](basic) | smallest program: admission within seconds, `Explain`, deny patterns, `GetFresh` | `go run ./examples/basic -redis localhost:6379` |
| [standalone](standalone) | HTTP service that keeps its own go-redis client (`NewFromGoRedis`), dependency injection, Prometheus `/metrics`, debug endpoints | `go run ./examples/standalone -redis localhost:6379` |
| [redis-cluster](redis-cluster) | Redis Cluster caching with the rueidis driver | `go run ./examples/redis-cluster -addrs 127.0.0.1:7001,...` |
| [adaptive-caching](adaptive-caching) | six simulated pods with read-heavy, write-heavy, and pod-local keys reporting to a controller; the lab's load generator | `go run ./examples/adaptive-caching -redis localhost:6379 -controller http://localhost:8080 -token <ingest>` |

`make build` compiles all examples. To run `adaptive-caching` and `standalone` with Redis, the controller, Prometheus, and Grafana, see the [lab guide](../lab/README.md).
