# Quickstart

This guide starts a local Redis server and introduces the SDK. Cached reads are eventually consistent; read the [consistency contract](consistency.md) before choosing which data to cache.

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
	if err := client.SetString(ctx, "product:1", "coffee", 0); err != nil {
		log.Fatal(err)
	}
	v, err := client.Get(ctx, "product:1")
	switch {
	case errors.Is(err, keyshift.Nil):
		log.Println("key does not exist")
		return
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

Reads that fetch several keys can use `ks.MGet(ctx, keys...)`: admitted keys use the cache path and the rest use a pipeline. Commands KeyShift does not wrap (`INCR`, `HSET`, Lua scripts, …) can still run on `rdb`. After a successful write affecting a key read through KeyShift, call `ks.NotifyWrite(key)` to drop its local entry. For concurrent-fill limitations, see [consistency](consistency.md).

For **Redis Cluster** caching use the rueidis driver:

```go
ks, err := keyshift.NewFromRueidis(rueidis.ClientOption{InitAddress: seeds}, cfg)
```

## 5. Expose metrics

```go
reg := prometheus.NewRegistry()
ks, err := keyshift.New(keyshift.Config{
    ServiceName: "svc",
    Redis: keyshift.RedisConfig{Addr: "localhost:6379"},
    Metrics: keyshift.MetricsConfig{Registerer: reg},
})
if err != nil {
    log.Fatal(err)
}
defer ks.Close()
http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
```

## 6. Optional: controller, Prometheus, and Grafana

```bash
export KEYSHIFT_INGEST_TOKENS=$(openssl rand -hex 32)
export KEYSHIFT_VIEWER_TOKENS=$(openssl rand -hex 32)
export KEYSHIFT_ADMIN_TOKENS=$(openssl rand -hex 32)
export KEYSHIFT_INGEST_TOKEN=$KEYSHIFT_INGEST_TOKENS
export KEYSHIFT_FINGERPRINT_SECRET=$(openssl rand -hex 32)
make build
bin/keyshift-controller --addr :8080 --data-dir ./data
```

The controller runs in the foreground. In another terminal, pass the same ingest token and fingerprint secret to each SDK instance; keep the secrets in your deployment's secret store. Point SDKs at the controller:

```go
// Include this in keyshift.Config in each application instance.
Telemetry: keyshift.TelemetryConfig{
    Endpoint:          "http://keyshift-controller:8080",
    Token:             os.Getenv("KEYSHIFT_INGEST_TOKEN"),
    FingerprintSecret: []byte(os.Getenv("KEYSHIFT_FINGERPRINT_SECRET")), // same for all pods
},
```

Scrape the controller's `/metrics` with Prometheus and import the dashboards from `lab/grafana/dashboards/` into Grafana, or use the CLI. Set `KEYSHIFT_VIEWER_TOKENS` to the controller's viewer token in the CLI terminal before running:

```bash
export KEYSHIFT_CONTROLLER=http://localhost:8080 KEYSHIFT_TOKEN=$KEYSHIFT_VIEWER_TOKENS
keyshift stats
keyshift top --watch 5s
```

## 7. Everything at once: the lab

```bash
make docker-up    # Redis, controller, simulated pods, product-service, Prometheus, Grafana
```

`make docker-down` stops the lab and removes its volumes.
