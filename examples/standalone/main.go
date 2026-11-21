// Command standalone is a small HTTP "product service" that adopts KeyShift
// without changing its architecture: it keeps its existing go-redis client
// (constructed and owned by the application, as with dependency injection)
// and wraps it with keyshift.NewFromGoRedis.
//
//	go run ./examples/standalone -redis localhost:6379 -listen :8090
//	curl localhost:8090/products/1
//	curl localhost:8090/debug/keyshift/explain?key=product:1
//	curl localhost:8090/metrics
//
// Environment variables (all optional):
//
//	KEYSHIFT_CONTROLLER, KEYSHIFT_INGEST_TOKEN, KEYSHIFT_FINGERPRINT_SECRET
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"github.com/Huy-8bit/keyshift/pkg/keyshift"
)

// ProductStore is the application's repository. It depends on a small
// interface, so KeyShift is injected like any other dependency.
type ProductStore struct {
	cache interface {
		Get(ctx context.Context, key string) (string, error)
		SetString(ctx context.Context, key, value string, ttl time.Duration) error
	}
}

func (s *ProductStore) Get(ctx context.Context, id int) (string, error) {
	key := "product:" + strconv.Itoa(id)
	v, err := s.cache.Get(ctx, key)
	if errors.Is(err, keyshift.Nil) {
		// Cache-aside: load from the system of record and populate Redis.
		v = fmt.Sprintf(`{"id":%d,"name":"product %d","price":%d}`, id, id, 100+id)
		if err := s.cache.SetString(ctx, key, v, 10*time.Minute); err != nil {
			return "", err
		}
		return v, nil
	}
	return v, err
}

func main() {
	addr := flag.String("redis", "localhost:6379", "Redis address")
	listen := flag.String("listen", ":8090", "HTTP listen address")
	service := flag.String("service", "product-service", "service name")
	flag.Parse()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// The application's existing Redis client.
	rdb := redis.NewClient(&redis.Options{Addr: *addr, PoolSize: 50})
	defer rdb.Close()

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	cfg := keyshift.Config{
		ServiceName: *service,
		Logger:      log,
		Metrics:     keyshift.MetricsConfig{Registerer: reg},
		Cache:       keyshift.CacheConfig{DenyPatterns: []string{"cart:*", "inventory:*"}},
	}
	if ep := os.Getenv("KEYSHIFT_CONTROLLER"); ep != "" {
		cfg.Telemetry = keyshift.TelemetryConfig{
			Endpoint:          ep,
			Token:             os.Getenv("KEYSHIFT_INGEST_TOKEN"),
			FingerprintSecret: []byte(os.Getenv("KEYSHIFT_FINGERPRINT_SECRET")),
			// Product keys are not sensitive: allow their names in the dashboard.
			DisplayKeyPatterns: []string{"product:*"},
		}
	}
	ks, err := keyshift.NewFromGoRedis(rdb, cfg)
	if err != nil {
		log.Error("keyshift init failed", "error", err)
		os.Exit(1)
	}
	defer ks.Close()
	store := &ProductStore{cache: ks}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /products/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.Atoi(r.PathValue("id"))
		if err != nil {
			http.Error(w, "bad id", http.StatusBadRequest)
			return
		}
		v, err := store.Get(r.Context(), id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintln(w, v)
	})
	mux.HandleFunc("GET /debug/keyshift/explain", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(ks.Explain(r.URL.Query().Get("key")))
	})
	mux.HandleFunc("GET /debug/keyshift/stats", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(ks.Stats())
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := ks.Ping(r.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintln(w, "ok")
	})

	srv := &http.Server{Addr: *listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		log.Info("product service listening", "addr", *listen, "instance", ks.InstanceID())
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("listen failed", "error", err)
			stop()
		}
	}()
	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(shutdown)
}
