// Command keyshift-controller runs the optional KeyShift control plane.
//
// Configuration is read from flags, with environment variable fallbacks so
// secrets can come from Kubernetes Secrets or files:
//
//	KEYSHIFT_INGEST_TOKENS / --ingest-tokens-file   SDK upload tokens
//	KEYSHIFT_VIEWER_TOKENS / --viewer-tokens-file   read-only API tokens
//	KEYSHIFT_ADMIN_TOKENS  / --admin-tokens-file    policy admin tokens
//
// Token lists are comma or newline separated.
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Huy-8bit/keyshift/internal/controller"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(healthcheck(os.Args[2:]))
	}
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "keyshift-controller:", err)
		os.Exit(1)
	}
}

// healthcheck probes /healthz for container health checks (distroless
// images have no shell or curl).
func healthcheck(args []string) int {
	port := "8080"
	if len(args) > 0 {
		port = args[0]
	}
	if _, err := strconv.ParseUint(port, 10, 16); err != nil {
		fmt.Fprintln(os.Stderr, "usage: keyshift-controller healthcheck [PORT]")
		return 2
	}
	// Only the local controller is probed (container health checks).
	c := &http.Client{Timeout: 3 * time.Second}
	resp, err := c.Get("http://127.0.0.1:" + port + "/healthz") //nolint:gosec // host is fixed to loopback; port validated above
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintln(os.Stderr, resp.Status)
		return 1
	}
	return 0
}

func env(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func tokens(envName, file string) ([]string, error) {
	raw := os.Getenv(envName)
	if file != "" {
		b, err := os.ReadFile(file)
		if err != nil {
			return nil, err
		}
		raw += "\n" + string(b)
	}
	var out []string
	for _, t := range strings.FieldsFunc(raw, func(r rune) bool { return r == ',' || r == '\n' || r == '\r' }) {
		if t = strings.TrimSpace(t); t != "" {
			if len(t) < 16 {
				return nil, fmt.Errorf("%s: tokens must be at least 16 characters", envName)
			}
			out = append(out, t)
		}
	}
	return out, nil
}

func run(args []string) error {
	def := controller.DefaultConfig()
	fs := flag.NewFlagSet("keyshift-controller", flag.ContinueOnError)
	addr := fs.String("addr", env("KEYSHIFT_ADDR", ":8080"), "listen address")
	dataDir := fs.String("data-dir", env("KEYSHIFT_DATA_DIR", ""), "directory for persisted policies (empty: in-memory)")
	tlsCert := fs.String("tls-cert", env("KEYSHIFT_TLS_CERT", ""), "TLS certificate file")
	tlsKey := fs.String("tls-key", env("KEYSHIFT_TLS_KEY", ""), "TLS private key file")
	ingestFile := fs.String("ingest-tokens-file", "", "file with ingest tokens")
	viewerFile := fs.String("viewer-tokens-file", "", "file with viewer tokens")
	adminFile := fs.String("admin-tokens-file", "", "file with admin tokens")
	insecure := fs.Bool("insecure-no-auth", false, "disable authentication (only allowed on loopback addresses, for local development)")
	logLevel := fs.String("log-level", env("KEYSHIFT_LOG_LEVEL", "info"), "debug, info, warn, error")
	logJSON := fs.Bool("log-json", env("KEYSHIFT_LOG_FORMAT", "json") == "json", "JSON logs")
	fs.IntVar(&def.MaxInstances, "max-instances", def.MaxInstances, "maximum registered SDK instances")
	fs.IntVar(&def.MaxServices, "max-services", def.MaxServices, "maximum distinct services")
	fs.IntVar(&def.MaxCandidatesPerBatch, "max-candidates", def.MaxCandidatesPerBatch, "candidates retained per instance")
	fs.IntVar(&def.MaxGlobalKeys, "max-global-keys", def.MaxGlobalKeys, "fingerprints retained in the global ranking")
	fs.DurationVar(&def.InstanceTTL, "instance-ttl", def.InstanceTTL, "forget instances not seen for this long")
	fs.DurationVar(&def.FreshFor, "fresh-for", def.FreshFor, "instances seen within this window count as active")
	fs.DurationVar(&def.AggregateInterval, "aggregate-interval", def.AggregateInterval, "global view refresh interval")
	fs.Float64Var(&def.IngestRatePerSecond, "ingest-rate", def.IngestRatePerSecond, "global telemetry uploads per second")
	fs.IntVar(&def.IngestBurst, "ingest-burst", def.IngestBurst, "global telemetry burst")
	fs.DurationVar(&def.PerInstanceMinInterval, "instance-min-interval", def.PerInstanceMinInterval, "minimum interval between uploads per instance")
	fs.BoolVar(&def.MetricsPublic, "metrics-public", def.MetricsPublic, "serve /metrics without authentication")
	fs.IntVar(&def.MetricsTopKeys, "metrics-top-keys", def.MetricsTopKeys, "hot keys exported as Prometheus series")
	fs.BoolVar(&def.MetricsInstances, "metrics-instances", def.MetricsInstances, "export per-instance Prometheus series")
	if err := fs.Parse(args); err != nil {
		return err
	}

	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(*logLevel)); err != nil {
		return err
	}
	opts := &slog.HandlerOptions{Level: lvl}
	var h slog.Handler = slog.NewTextHandler(os.Stderr, opts)
	if *logJSON {
		h = slog.NewJSONHandler(os.Stderr, opts)
	}
	log := slog.New(h)

	ingest, err := tokens("KEYSHIFT_INGEST_TOKENS", *ingestFile)
	if err != nil {
		return err
	}
	viewer, err := tokens("KEYSHIFT_VIEWER_TOKENS", *viewerFile)
	if err != nil {
		return err
	}
	admin, err := tokens("KEYSHIFT_ADMIN_TOKENS", *adminFile)
	if err != nil {
		return err
	}
	auth := controller.NewAuth(ingest, viewer, admin)
	if *insecure {
		host, _, _ := net.SplitHostPort(*addr)
		ip := net.ParseIP(host)
		if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
			return errors.New("--insecure-no-auth requires a loopback listen address such as 127.0.0.1:8080")
		}
		auth.Disabled = true
		log.Warn("authentication disabled (--insecure-no-auth); do not use outside local development")
	} else if auth.Empty() {
		return errors.New("no tokens configured: set KEYSHIFT_INGEST_TOKENS, KEYSHIFT_VIEWER_TOKENS and KEYSHIFT_ADMIN_TOKENS (or use --insecure-no-auth on loopback)")
	}

	def.Auth = auth
	def.DataDir = *dataDir
	def.Logger = log
	srv, err := controller.New(def)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go srv.Run(ctx)

	hs := &http.Server{
		Addr:              *addr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    32 << 10,
		TLSConfig:         &tls.Config{MinVersion: tls.VersionTLS12},
	}
	errc := make(chan error, 1)
	go func() {
		log.Info("keyshift controller listening", "addr", *addr, "tls", *tlsCert != "", "data_dir", *dataDir, "version", controller.Version)
		if *tlsCert != "" {
			errc <- hs.ListenAndServeTLS(*tlsCert, *tlsKey)
		} else {
			errc <- hs.ListenAndServe()
		}
	}()
	select {
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
	}
	log.Info("shutting down")
	shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return hs.Shutdown(shutCtx)
}
