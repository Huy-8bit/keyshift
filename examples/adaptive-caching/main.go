// Command adaptive-caching runs several simulated service instances in one
// process against a shared Redis and an optional KeyShift controller. It
// demonstrates per-instance adaptive admission:
//
//   - catalog keys are read-mostly and become cached where they are hot;
//   - stock keys are written constantly and are kept out of the cache;
//   - each "pod" has its own locally hot keys that other pods never read.
//
// It also serves as the KeyShift Lab load generator in docker-compose.
//
//	go run ./examples/adaptive-caching -redis localhost:6379 \
//	    -controller http://localhost:8080 -token <ingest-token> -duration 2m
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/Huy-8bit/keyshift/pkg/keyshift"
)

type pod struct {
	service string
	id      int
	client  *keyshift.Client
}

func main() {
	addr := flag.String("redis", envOr("REDIS_ADDR", "localhost:6379"), "Redis address")
	controller := flag.String("controller", os.Getenv("KEYSHIFT_CONTROLLER"), "controller URL (optional)")
	token := flag.String("token", os.Getenv("KEYSHIFT_INGEST_TOKEN"), "controller ingest token")
	pods := flag.Int("pods", 3, "simulated pods per service")
	rate := flag.Int("rate", 2000, "reads per second per pod")
	duration := flag.Duration("duration", 0, "run time (0 = until interrupted)")
	window := flag.Duration("window", 5*time.Second, "detection window")
	report := flag.Duration("report", 10*time.Second, "telemetry interval")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	secret := []byte(envOr("KEYSHIFT_FINGERPRINT_SECRET", "lab-fingerprint-secret-change-me"))
	var all []*pod
	for _, svc := range []string{"catalog-api", "checkout-api"} {
		for i := range *pods {
			cfg := keyshift.Config{
				ServiceName: svc,
				InstanceID:  fmt.Sprintf("%s-%d", svc, i),
				Redis:       keyshift.RedisConfig{Addr: *addr},
				Detection:   keyshift.DetectionConfig{Window: *window},
				Logger:      log,
			}
			if *controller != "" {
				cfg.Telemetry = keyshift.TelemetryConfig{Endpoint: *controller, Token: *token, Interval: *report,
					FingerprintSecret: secret, DisplayKeyPatterns: []string{"catalog:*", "stock:*", "pod:*"}}
			}
			c, err := keyshift.New(cfg)
			if err != nil {
				fmt.Fprintln(os.Stderr, "init:", err)
				os.Exit(1)
			}
			defer c.Close()
			all = append(all, &pod{service: svc, id: i, client: c})
		}
	}
	seed := all[0].client
	for i := range 500 {
		seed.SetString(ctx, "catalog:"+strconv.Itoa(i), fmt.Sprintf(`{"sku":%d}`, i), 0)
	}
	for i := range 20 {
		seed.SetString(ctx, "stock:"+strconv.Itoa(i), "100", 0)
	}

	var wg sync.WaitGroup
	for _, p := range all {
		wg.Go(func() { p.run(ctx, *rate) })
	}
	// A writer that updates stock levels continuously (write-heavy keys).
	wg.Go(func() {
		t := time.NewTicker(2 * time.Millisecond)
		defer t.Stop()
		r := rand.New(rand.NewPCG(1, 2))
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				seed.SetString(ctx, "stock:"+strconv.Itoa(r.IntN(20)), strconv.Itoa(r.IntN(1000)), 0)
			}
		}
	})
	tick := time.NewTicker(*window)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			summary(all)
			return
		case <-tick.C:
			summary(all)
		}
	}
}

func (p *pod) run(ctx context.Context, rate int) {
	r := rand.New(rand.NewPCG(uint64(p.id), uint64(len(p.service))))
	z := rand.NewZipf(r, 1.2, 1, 499)
	interval := time.Second / time.Duration(max(rate/100, 1))
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		for range 100 {
			var key string
			switch x := r.IntN(100); {
			case x < 70:
				key = "catalog:" + strconv.FormatUint(z.Uint64(), 10)
			case x < 85:
				key = "stock:" + strconv.Itoa(r.IntN(20))
			default:
				// Locally hot keys owned by this pod only.
				key = fmt.Sprintf("pod:%s-%d:session:%d", p.service, p.id, r.IntN(5))
			}
			v, err := p.client.Get(ctx, key)
			if err == keyshift.Nil {
				p.client.SetString(ctx, key, "x", time.Hour)
			} else if err != nil && ctx.Err() == nil {
				fmt.Fprintln(os.Stderr, p.service, p.id, "get:", err)
			}
			_ = v
		}
	}
}

func summary(all []*pod) {
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "\n%s\nINSTANCE\tREADS\tLOCAL HITS\tHIT RATIO\tADMITTED\tSTOCK:0\tCATALOG:0\tTRACKING\tPOLICY\n", time.Now().Format(time.TimeOnly))
	for _, p := range all {
		s := p.client.Stats()
		ratio := 0.0
		if s.Reads > 0 {
			ratio = float64(s.LocalHits) / float64(s.Reads)
		}
		fmt.Fprintf(tw, "%s\t%d\t%d\t%.1f%%\t%d\t%s\t%s\t%s\tv%d/%s\n", p.client.InstanceID(), s.Reads, s.LocalHits, 100*ratio,
			s.AdmittedKeys, p.client.Explain("stock:0").State, p.client.Explain("catalog:0").State, s.Tracking.State, s.PolicyVersion, s.PolicySource)
	}
	tw.Flush()
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
