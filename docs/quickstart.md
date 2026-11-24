# Quickstart

## 1. Run Redis

```bash
docker run -d --name redis -p 6379:6379 redis:8.6
```

## 2. Check compatibility

```bash
go install github.com/Huy-8bit/keyshift/cmd/keyshift@latest
keyshift doctor --redis localhost:6379 --skip-controller
```

`doctor` verifies connectivity, RESP3, `CLIENT TRACKING`, that the KeyShift driver can enable local caching, the tracking table limit, ACL user, and HOTKEYS availability.

## 3. Use the SDK

```go
package main

import (
	"context"
	"errors"
	"log"

	"github.com/Huy-8bit/keyshift/pkg/keyshift"
)

func main() {
	client, err := keyshift.New(keyshift.Config{
		ServiceName: "product-service",
		Redis:       keyshift.RedisConfig{Addr: "localhost:6379"},
		Cache:       keyshift.CacheConfig{DenyPatterns: []string{"balance:*", "lock:*"}},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	_ = client.SetString(ctx, "product:1", "coffee", 0)
	v, err := client.Get(ctx, "product:1")
	switch {
	case errors.Is(err, keyshift.Nil):
		// key does not exist
	case err != nil:
		log.Fatal(err) // ordinary Redis error semantics
	}
	log.Println(v, client.Explain("product:1").Reason)
}
```

Run the bundled example to watch admission happen within a few seconds:

```bash
go run ./examples/basic -redis localhost:6379
```

## 4. Adopt in an existing service

Keep your go-redis client and wiring:

```go
rdb := redis.NewClient(&redis.Options{Addr: addr, PoolSize: 50}) // existing
ks, err := keyshift.NewFromGoRedis(rdb, keyshift.Config{ServiceName: "product-service"})
// Inject ks where the repository reads hot data; keep rdb for everything else.
```

`NewFromGoRedis` never reconfigures `rdb`. It creates a small dedicated client for cached reads from `rdb`'s options. `ks.Close()` does not close `rdb`. See `examples/standalone` for an HTTP service with Prometheus metrics and dependency injection.

Reads that fetch several keys can use `ks.MGet(ctx, keys...)`: admitted keys come from the local cache and the rest from one pipeline. Commands KeyShift does not wrap (`INCR`, `HSET`, Lua scripts, …) keep running on `rdb`; after a write that changes a string key this service also reads through KeyShift, call `ks.NotifyWrite(key)` so this instance never serves the old local value.

For **Redis Cluster** caching use the rueidis driver:

```go
ks, err := keyshift.NewFromRueidis(rueidis.ClientOption{InitAddress: seeds}, cfg)
```

## 5. Expose metrics

```go
reg := prometheus.NewRegistry()
ks, _ := keyshift.New(keyshift.Config{ServiceName: "svc", Metrics: keyshift.MetricsConfig{Registerer: reg}, /* ... */})
http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
```

## 6. Optional: controller, Prometheus, and Grafana

```bash
export KEYSHIFT_INGEST_TOKENS=$(openssl rand -hex 32)
export KEYSHIFT_VIEWER_TOKENS=$(openssl rand -hex 32)
export KEYSHIFT_ADMIN_TOKENS=$(openssl rand -hex 32)
make build
bin/keyshift-controller --addr :8080 --data-dir ./data
```

Point SDKs at it:

```go
Telemetry: keyshift.TelemetryConfig{
    Endpoint:          "http://keyshift-controller:8080",
    Token:             os.Getenv("KEYSHIFT_INGEST_TOKEN"),
    FingerprintSecret: []byte(os.Getenv("KEYSHIFT_FINGERPRINT_SECRET")), // same for all pods
},
```

Scrape the controller's `/metrics` with Prometheus and import the dashboards from `lab/grafana/dashboards/` into Grafana, or use the CLI:

```bash
export KEYSHIFT_CONTROLLER=http://localhost:8080 KEYSHIFT_TOKEN=$KEYSHIFT_VIEWER_TOKENS
keyshift stats
keyshift top --watch 5s
```

## 7. Everything at once: the lab

```bash
make docker-up    # Redis, controller, simulated pods, product-service, Prometheus, Grafana
make docker-down
```
