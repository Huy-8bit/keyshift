package keyshift_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/redis/rueidis"

	"github.com/Huy-8bit/keyshift/pkg/keyshift"
)

// The examples are compiled by `go test` but not executed, because they need
// a Redis server.

func Example() {
	client, err := keyshift.New(keyshift.Config{
		ServiceName: "product-service",
		Redis:       keyshift.RedisConfig{Addr: "localhost:6379"},
		Cache: keyshift.CacheConfig{
			Mode:           keyshift.Adaptive,
			MaxMemoryBytes: 16 << 20,
			MaxEntries:     1000,
			DenyPatterns:   []string{"balance:*", "lock:*"},
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
	fmt.Println(client.Explain("product:123").Reason)
}

func ExampleNewFromGoRedis() {
	// The application's existing client stays in charge of its connection
	// settings; KeyShift never reconfigures it and does not close it.
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", PoolSize: 50})
	defer rdb.Close()

	reg := prometheus.NewRegistry()
	ks, err := keyshift.NewFromGoRedis(rdb, keyshift.Config{
		ServiceName: "product-service",
		Metrics:     keyshift.MetricsConfig{Registerer: reg},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer ks.Close()
	_, _ = ks.Get(context.Background(), "product:1")
}

func ExampleNewFromRueidis() {
	// Redis Cluster caching uses the rueidis driver with OPTIN tracking.
	ks, err := keyshift.NewFromRueidis(rueidis.ClientOption{
		InitAddress: []string{"127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"},
	}, keyshift.Config{ServiceName: "catalog"})
	if err != nil {
		log.Fatal(err)
	}
	defer ks.Close()
}

func ExampleClient_GetFresh() {
	var client *keyshift.Client // from keyshift.New
	ctx := context.Background()
	// Authoritative read before a conditional write: never served locally.
	balance, err := client.GetFresh(ctx, "balance:42")
	if err == nil {
		fmt.Println(balance)
	}
}

func ExampleClient_ApplyPolicy() {
	var client *keyshift.Client // from keyshift.New
	err := client.ApplyPolicy(&keyshift.Policy{
		Mode:         keyshift.Adaptive,
		DenyPatterns: []string{"inventory:*"},
		Admission:    keyshift.PolicyAdmission{MinReadRate: 10, CooldownSeconds: 120},
		Budget:       keyshift.PolicyBudget{MaxEntries: 200},
	})
	if err != nil {
		log.Fatal(err)
	}
}

func ExampleTelemetryConfig() {
	_ = keyshift.Config{
		ServiceName: "checkout",
		Redis:       keyshift.RedisConfig{Addr: "localhost:6379"},
		Telemetry: keyshift.TelemetryConfig{
			Endpoint:           "https://keyshift-controller:8080",
			Token:              "ingest-token-from-secret",
			Interval:           30 * time.Second,
			FingerprintSecret:  []byte("shared-secret-from-a-secret-store"),
			DisplayKeyPatterns: []string{"product:*"},
		},
	}
}
