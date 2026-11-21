// Package keyshift is the KeyShift Go SDK: adaptive, selective client-side
// caching for Redis.
//
// A Client observes every read, detects locally hot keys with a bounded
// streaming detector, and admits only keys whose measured benefit outweighs
// their cost into a local cache backed by Redis server-assisted client-side
// caching (client tracking). Everything else goes straight to Redis.
//
// The local cache is eventually consistent: an entry can be served for a
// short time after another client modifies the key, until the invalidation
// arrives. Use GetFresh, deny patterns, or Mode Off for data that requires
// authoritative reads. See docs/consistency.md.
package keyshift

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/rueidis"

	"github.com/Huy-8bit/keyshift/internal/admission"
	"github.com/Huy-8bit/keyshift/internal/detector"
	"github.com/Huy-8bit/keyshift/internal/fingerprint"
	"github.com/Huy-8bit/keyshift/internal/observability"
	"github.com/Huy-8bit/keyshift/internal/policy"
	"github.com/Huy-8bit/keyshift/internal/rediscompat"
	"github.com/Huy-8bit/keyshift/internal/telemetry"
	"github.com/Huy-8bit/keyshift/internal/tracking"
)

// Version is the SDK version reported in telemetry.
const Version = "0.1.0"

var (
	// Nil is returned by Get when the key does not exist.
	Nil = rediscompat.ErrNil
	// ErrClosed is returned after Close.
	ErrClosed = errors.New("keyshift: client is closed")
)

// Policy is a caching policy. See ApplyPolicy and docs/configuration.md.
type Policy = policy.Policy

// PolicyAdmission and PolicyBudget are Policy sub-structures.
type (
	PolicyAdmission = policy.Admission
	PolicyBudget    = policy.Budget
)

// Decision explains why a key is or is not cached.
type Decision = admission.Decision

// Client is a KeyShift client. It is safe for concurrent use.
type Client struct {
	cfg        Config
	instanceID string
	local      *policy.Policy

	backend rediscompat.Backend
	goredis *rediscompat.GoRedis
	rueidis *rediscompat.Rueidis

	engine   *admission.Engine
	monitor  *tracking.Monitor
	hasher   *fingerprint.Hasher
	display  *policy.Matcher
	reporter *telemetry.Reporter
	metrics  *collector

	policyMu     sync.Mutex
	manualPolicy bool
	lastContact  atomic.Int64
	policyErrors atomic.Uint64

	reads, cachedReads, directReads observability.Counter
	bypasses, writes, deletes       observability.Counter
	errorsTotal, fallbacks          observability.Counter

	agg *telemetryAgg
	seq atomic.Uint64

	closed    atomic.Bool
	stop      chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
	closeErr  error
}

// New creates a Client and the underlying Redis client described by
// cfg.Redis. Close releases both.
func New(cfg Config) (*Client, error) {
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("keyshift: invalid config: %w", err)
	}
	r := cfg.Redis
	addrs := r.Addrs
	if len(addrs) == 0 && r.Addr != "" {
		addrs = []string{r.Addr}
	}
	if len(addrs) == 0 {
		return nil, errors.New("keyshift: Redis.Addr or Redis.Addrs is required")
	}
	switch r.Driver {
	case DriverGoRedis:
		var uc redis.UniversalClient
		if r.Cluster {
			uc = redis.NewClusterClient(&redis.ClusterOptions{Addrs: addrs, Username: r.Username, Password: r.Password,
				TLSConfig: r.TLSConfig, PoolSize: r.PoolSize, DialTimeout: r.DialTimeout, ReadTimeout: r.ReadTimeout, WriteTimeout: r.WriteTimeout})
		} else {
			uc = redis.NewClient(&redis.Options{Addr: addrs[0], Username: r.Username, Password: r.Password, DB: r.DB,
				TLSConfig: r.TLSConfig, PoolSize: r.PoolSize, DialTimeout: r.DialTimeout, ReadTimeout: r.ReadTimeout, WriteTimeout: r.WriteTimeout})
		}
		return newClient(cfg, func(h rediscompat.Hooks, l rediscompat.CacheLimits) (rediscompat.Backend, error) {
			return newGoRedisBackend(cfg, uc, true, h, l)
		})
	case DriverRueidis:
		opt := rueidis.ClientOption{InitAddress: addrs, Username: r.Username, Password: r.Password,
			SelectDB: r.DB, TLSConfig: r.TLSConfig, ForceSingleClient: !r.Cluster}
		if r.DialTimeout > 0 {
			opt.Dialer.Timeout = r.DialTimeout
		}
		if r.WriteTimeout > 0 {
			opt.ConnWriteTimeout = r.WriteTimeout
		}
		return NewFromRueidis(opt, cfg)
	default:
		return nil, fmt.Errorf("keyshift: unknown driver %q", r.Driver)
	}
}

// NewFromGoRedis wraps an existing go-redis client. KeyShift uses rc for
// writes and uncached reads and never changes its configuration; for cached
// reads it creates a small dedicated tracking-enabled client from rc's
// options (standalone clients only). Close does not close rc.
func NewFromGoRedis(rc redis.UniversalClient, cfg Config) (*Client, error) {
	if rc == nil {
		return nil, errors.New("keyshift: nil go-redis client")
	}
	cfg.applyDefaults()
	cfg.Redis.Driver = DriverGoRedis
	return newClient(cfg, func(h rediscompat.Hooks, l rediscompat.CacheLimits) (rediscompat.Backend, error) {
		return newGoRedisBackend(cfg, rc, false, h, l)
	})
}

// NewFromRueidis creates a rueidis client from opt with OPTIN client
// tracking and a KeyShift-managed bounded cache store. Close closes it.
func NewFromRueidis(opt rueidis.ClientOption, cfg Config) (*Client, error) {
	cfg.applyDefaults()
	cfg.Redis.Driver = DriverRueidis
	cluster := !opt.ForceSingleClient && (cfg.Redis.Cluster || len(opt.InitAddress) > 1)
	return newClient(cfg, func(h rediscompat.Hooks, l rediscompat.CacheLimits) (rediscompat.Backend, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		o := opt
		return rediscompat.NewRueidis(ctx, rediscompat.RueidisOptions{ClientOption: &o, Cluster: cluster, Limits: l, Hooks: h,
			CachingUnavailable: cachingDisabledReason(cfg)})
	})
}

func cachingDisabledReason(cfg Config) string {
	if cfg.Cache.Mode == Off || cfg.Detection.Disabled {
		return "caching disabled by configuration"
	}
	return ""
}

func newGoRedisBackend(cfg Config, rc redis.UniversalClient, owns bool, h rediscompat.Hooks, l rediscompat.CacheLimits) (rediscompat.Backend, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return rediscompat.NewGoRedis(ctx, rediscompat.GoRedisOptions{Direct: rc, OwnsDirect: owns, Limits: l,
		TrackedPoolSize: cfg.Tracking.TrackedPoolSize, DrainInterval: cfg.Tracking.DrainInterval,
		DisableCaching: cachingDisabledReason(cfg) != "", Hooks: h})
}

func defaultInstanceID() string {
	base := os.Getenv("POD_NAME")
	if base == "" {
		base, _ = os.Hostname()
	}
	base = strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.' {
			return r
		}
		return '-'
	}, base)
	if base == "" {
		base = "instance"
	}
	if len(base) > 200 {
		base = base[:200]
	}
	var b [3]byte
	_, _ = rand.Read(b[:])
	return base + "-" + hex.EncodeToString(b[:])
}

func newClient(cfg Config, mk func(rediscompat.Hooks, rediscompat.CacheLimits) (rediscompat.Backend, error)) (*Client, error) {
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("keyshift: invalid config: %w", err)
	}
	c := &Client{cfg: cfg, instanceID: cfg.InstanceID, local: cfg.localPolicy(), stop: make(chan struct{})}
	if c.instanceID == "" {
		c.instanceID = defaultInstanceID()
	}
	var err error
	if c.hasher, err = fingerprint.New(cfg.Telemetry.FingerprintSecret); err != nil {
		return nil, fmt.Errorf("keyshift: %w", err)
	}
	c.display = policy.NewMatcher(cfg.Telemetry.DisplayKeyPatterns)

	c.engine, err = admission.New(admission.Config{
		Detector: detector.Config{
			Capacity:    cfg.Detection.MaxTrackingCandidates,
			TopK:        cfg.Detection.TopK,
			SampleRate:  cfg.Detection.SampleRate,
			MaxKeyBytes: cfg.Detection.MaxKeyBytes,
		},
		MaxStaleness:  cfg.Cache.MaxStaleness,
		MaxReportKeys: max(cfg.Telemetry.MaxCandidates, 50),
		Fingerprint:   c.hasher.Fingerprint,
		OnDemote:      func(k string) { c.backend.Forget(k) },
		OnSuspend:     func() { c.backend.FlushLocal() },
		InstanceID:    c.instanceID,
	}, policy.Resolve(c.local, nil))
	if err != nil {
		return nil, fmt.Errorf("keyshift: %w", err)
	}

	hooks := rediscompat.Hooks{OnFill: c.engine.RecordFill, OnInvalidate: c.engine.RecordInvalidation}
	limits := rediscompat.CacheLimits{MaxEntries: cfg.Cache.MaxEntries, MaxMemoryBytes: cfg.Cache.MaxMemoryBytes, MaxStaleness: cfg.Cache.MaxStaleness}
	if c.backend, err = mk(hooks, limits); err != nil {
		return nil, fmt.Errorf("keyshift: %w", err)
	}
	c.goredis, _ = c.backend.(*rediscompat.GoRedis)
	c.rueidis, _ = c.backend.(*rediscompat.Rueidis)

	caps := c.backend.Capabilities()
	if caps.Caching {
		c.monitor = tracking.NewMonitor(tracking.Config{
			Probe:    c.backend.CheckTracking,
			Interval: cfg.Tracking.HealthInterval,
			Timeout:  cfg.Tracking.HealthTimeout,
			OnChange: c.onTrackingChange,
		})
		c.monitor.CheckNow(context.Background())
		c.monitor.Start()
	} else {
		state := tracking.StateUnsupported
		if cachingDisabledReason(cfg) != "" {
			state = tracking.StateDisabled
		}
		c.monitor = tracking.Fixed(state, caps.Reason)
		c.engine.SetUnavailable(caps.Reason)
		cfg.Logger.Info("keyshift local caching unavailable; reads go directly to Redis", "reason", caps.Reason)
	}

	c.agg = newTelemetryAgg(cfg.Telemetry.MaxCandidates)
	if cfg.Telemetry.Endpoint != "" {
		if err := c.startTelemetry(); err != nil {
			_ = c.backend.Close()
			c.monitor.Stop()
			return nil, err
		}
	}
	if cfg.Metrics.Registerer != nil {
		c.metrics = newCollector(c)
		if err := cfg.Metrics.Registerer.Register(c.metrics); err != nil {
			_ = c.shutdown()
			return nil, fmt.Errorf("keyshift: register metrics: %w", err)
		}
	} else if !cfg.Metrics.DisableLatencyHistograms {
		c.metrics = newCollector(c)
	}

	c.wg.Add(1)
	go c.loop()
	cfg.Logger.Info("keyshift client started", "service", cfg.ServiceName, "instance", c.instanceID,
		"driver", caps.Driver, "topology", caps.Topology, "caching", caps.Caching, "tracking", c.monitor.Status().State)
	return c, nil
}

func (c *Client) onTrackingChange(st tracking.Status) {
	if st.State == tracking.StateHealthy {
		// Entries cached before tracking was lost may have missed
		// invalidations: start from an empty cache.
		c.backend.FlushLocal()
		c.engine.SetHealthy(true)
		c.cfg.Logger.Info("keyshift client tracking healthy; local caching enabled")
		return
	}
	c.engine.SetHealthy(false)
	c.cfg.Logger.Warn("keyshift client tracking unhealthy; local caching suspended", "reason", st.Reason)
}

func (c *Client) startTelemetry() error {
	t := c.cfg.Telemetry
	hc := t.HTTPClient
	if hc == nil {
		tr := http.DefaultTransport.(*http.Transport).Clone()
		tr.TLSClientConfig = t.TLSConfig
		tr.MaxIdleConnsPerHost = 2
		hc = &http.Client{Timeout: 10 * time.Second, Transport: tr}
	}
	var err error
	c.reporter, err = telemetry.NewReporter(telemetry.ReporterConfig{
		Endpoint: strings.TrimRight(t.Endpoint, "/"), Token: t.Token, Interval: t.Interval,
		HTTPClient: hc, Build: c.buildBatch, Logger: c.cfg.Logger,
		OnPolicy:      func(env *policy.Envelope) { _ = c.applyEnvelope(env) },
		OnContact:     func() { c.lastContact.Store(time.Now().UnixNano()) },
		PolicyVersion: func() int64 { return c.engine.Policy().Version },
		Service:       c.cfg.ServiceName, Instance: c.instanceID,
	})
	if err != nil {
		return fmt.Errorf("keyshift: %w", err)
	}
	if !t.DisableRemotePolicy {
		// Best effort: the controller is optional and must not block startup.
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if env, err := c.reporter.FetchPolicy(ctx); err != nil {
			c.cfg.Logger.Warn("keyshift controller unreachable at startup; using local policy", "error", err)
		} else if env != nil {
			_ = c.applyEnvelope(env)
		}
		cancel()
	}
	c.reporter.Start()
	return nil
}

// applyEnvelope validates and installs a remote policy envelope. Invalid
// envelopes are rejected and the last valid policy is retained.
func (c *Client) applyEnvelope(env *policy.Envelope) error {
	if c.cfg.Telemetry.DisableRemotePolicy {
		return nil
	}
	if err := env.Validate(); err != nil {
		c.policyErrors.Add(1)
		c.cfg.Logger.Warn("keyshift rejected invalid policy from controller", "error", err)
		return err
	}
	if svc := env.Current.Service; svc != "" && svc != c.cfg.ServiceName {
		c.policyErrors.Add(1)
		return fmt.Errorf("keyshift: policy for service %q does not apply to %q", svc, c.cfg.ServiceName)
	}
	c.policyMu.Lock()
	defer c.policyMu.Unlock()
	if c.manualPolicy {
		return nil
	}
	sel := env.Select(c.instanceID)
	eff := policy.Resolve(c.local, sel)
	cur := c.engine.Policy()
	if cur.Source == eff.Source && cur.Version == eff.Version {
		return nil
	}
	c.engine.SetPolicy(eff, true)
	c.cfg.Logger.Info("keyshift policy applied", "version", eff.Version, "source", eff.Source, "mode", eff.Mode)
	return nil
}

// ApplyPolicy installs a policy programmatically. It is validated and merged
// with the local hard limits exactly like a controller policy, and it takes
// precedence over controller policies until ClearPolicy is called.
func (c *Client) ApplyPolicy(p *Policy) error {
	if err := p.Validate(); err != nil {
		return fmt.Errorf("keyshift: invalid policy: %w", err)
	}
	c.policyMu.Lock()
	defer c.policyMu.Unlock()
	c.manualPolicy = true
	c.engine.SetPolicy(policy.Resolve(c.local, p.Clone()), true)
	return nil
}

// ClearPolicy removes a programmatic policy and reverts to the local policy.
// Controller policies are applied again on the next controller contact.
func (c *Client) ClearPolicy() {
	c.policyMu.Lock()
	defer c.policyMu.Unlock()
	c.manualPolicy = false
	c.engine.SetPolicy(policy.Resolve(c.local, nil), true)
}

func (c *Client) checkPolicyStaleness(now time.Time) {
	c.policyMu.Lock()
	defer c.policyMu.Unlock()
	eff := c.engine.Policy()
	if c.manualPolicy || eff.Source != "remote" {
		return
	}
	last := time.Unix(0, c.lastContact.Load())
	if now.Sub(last) > eff.ValidFor {
		c.engine.SetPolicy(policy.Resolve(c.local, nil), true)
		c.cfg.Logger.Warn("keyshift remote policy expired without controller contact; reverted to local policy",
			"version", eff.Version, "valid_for", eff.ValidFor)
	}
}

func (c *Client) loop() {
	defer c.wg.Done()
	t := time.NewTicker(c.cfg.Detection.Window)
	defer t.Stop()
	for {
		select {
		case <-c.stop:
			return
		case now := <-t.C:
			c.rotate(now)
		}
	}
}

func (c *Client) rotate(now time.Time) {
	start := time.Now()
	rep := c.engine.Rotate(now)
	c.agg.add(rep, c)
	c.checkPolicyStaleness(now)
	if c.metrics != nil {
		c.metrics.rotation.Observe(time.Since(start).Seconds())
	}
}

// Get returns the string value of key, or Nil if it does not exist.
func (c *Client) Get(ctx context.Context, key string) (string, error) {
	b, err := c.GetBytes(ctx, key)
	return string(b), err
}

// GetBytes returns the value of key. Admitted keys are served from the local
// cache when present; all other reads go to Redis.
func (c *Client) GetBytes(ctx context.Context, key string) ([]byte, error) {
	if c.closed.Load() {
		return nil, ErrClosed
	}
	c.reads.Inc()
	if ks := c.engine.Lookup(key); ks != nil {
		start := c.now()
		v, err := c.backend.GetCached(ctx, key)
		if err == nil || errors.Is(err, Nil) {
			ks.AddRead(int64(len(v)))
			c.cachedReads.Inc()
			c.observe(start, pathCache)
			return v, err
		}
		if ctx.Err() != nil || !isTransportError(err) {
			// Redis replied with an error (or the caller gave up): return it
			// unchanged, as a direct read would.
			c.errorsTotal.Inc()
			return nil, err
		}
		// A transport failure on the tracked path may mean tracking was
		// lost: suspend caching and retry once on the direct path so the
		// caller sees ordinary Redis semantics.
		c.monitor.ReportError(err)
		c.fallbacks.Inc()
	}
	start := c.now()
	v, err := c.backend.GetDirect(ctx, key)
	c.directReads.Inc()
	if err != nil && !errors.Is(err, Nil) {
		c.errorsTotal.Inc()
		return v, err
	}
	c.observe(start, pathDirect)
	if !c.cfg.Detection.Disabled {
		c.engine.ObserveRead(key, int64(len(v)))
	}
	return v, err
}

// GetFresh reads key directly from Redis, never from the local cache. Use it
// for reads that must reflect the authoritative Redis state (for example
// before a conditional write). The access still counts toward detection.
func (c *Client) GetFresh(ctx context.Context, key string) (string, error) {
	if c.closed.Load() {
		return "", ErrClosed
	}
	c.reads.Inc()
	c.bypasses.Inc()
	v, err := c.backend.GetDirect(ctx, key)
	if err != nil && !errors.Is(err, Nil) {
		c.errorsTotal.Inc()
		return "", err
	}
	if !c.cfg.Detection.Disabled {
		c.engine.ObserveRead(key, int64(len(v)))
	}
	return string(v), err
}

// Set writes value with an optional TTL (0 means no expiry). The write always
// goes to Redis; any local entry for key is dropped.
func (c *Client) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if c.closed.Load() {
		return ErrClosed
	}
	c.writes.Inc()
	c.engine.ObserveWrite(key)
	err := c.backend.Set(ctx, key, value, ttl)
	if err != nil {
		c.errorsTotal.Inc()
	}
	return err
}

// SetString is Set for string values.
func (c *Client) SetString(ctx context.Context, key, value string, ttl time.Duration) error {
	return c.Set(ctx, key, []byte(value), ttl)
}

// Del deletes keys and drops their local entries. It returns the number of
// keys removed by Redis.
func (c *Client) Del(ctx context.Context, keys ...string) (int64, error) {
	if c.closed.Load() {
		return 0, ErrClosed
	}
	for _, k := range keys {
		c.deletes.Inc()
		c.engine.ObserveWrite(k)
	}
	n, err := c.backend.Del(ctx, keys...)
	if err != nil {
		c.errorsTotal.Inc()
	}
	return n, err
}

// Explain reports why key is or is not cached.
func (c *Client) Explain(key string) Decision { return c.engine.Explain(key) }

// InstanceID returns this client's instance identity.
func (c *Client) InstanceID() string { return c.instanceID }

// Fingerprint returns the telemetry fingerprint of key, which is how the
// controller and dashboard identify it.
func (c *Client) Fingerprint(key string) string { return c.hasher.Fingerprint(key) }

// GoRedis returns the application go-redis client (nil for other drivers).
// Writes through it bypass KeyShift's local-entry invalidation; server
// tracking still invalidates cached entries asynchronously.
func (c *Client) GoRedis() redis.UniversalClient {
	if c.goredis == nil {
		return nil
	}
	return c.goredis.Direct()
}

// Rueidis returns the rueidis client (nil for other drivers).
func (c *Client) Rueidis() rueidis.Client {
	if c.rueidis == nil {
		return nil
	}
	return c.rueidis.Client()
}

// Ping checks Redis connectivity.
func (c *Client) Ping(ctx context.Context) error { return c.backend.Ping(ctx) }

// FlushLocal drops every locally cached entry. Redis is not affected.
func (c *Client) FlushLocal() { c.backend.FlushLocal() }

// RotateNow closes the current observation window immediately. It is
// intended for tests and tools; normal operation rotates every
// Detection.Window.
func (c *Client) RotateNow() { c.rotate(time.Now()) }

// Close stops background work and releases resources. It is idempotent.
// In-flight calls complete normally; calls after Close return ErrClosed.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		c.closeErr = c.shutdown()
	})
	return c.closeErr
}

func (c *Client) shutdown() error {
	select {
	case <-c.stop:
	default:
		close(c.stop)
	}
	c.wg.Wait()
	if c.reporter != nil {
		c.reporter.Stop()
	}
	c.monitor.Stop()
	if c.metrics != nil && c.cfg.Metrics.Registerer != nil {
		c.cfg.Metrics.Registerer.Unregister(c.metrics)
	}
	return c.backend.Close()
}

func isTransportError(err error) bool {
	if err == nil {
		return false
	}
	var ne net.Error
	if errors.As(err, &ne) {
		return true
	}
	// ErrDoCacheAborted: the cached fetch failed as a whole (connection
	// closed or the wrapper aborted); a direct read is still meaningful.
	if errors.Is(err, redis.ErrClosed) || errors.Is(err, rueidis.ErrClosing) || errors.Is(err, rueidis.ErrDoCacheAborted) {
		return true
	}
	s := err.Error()
	return strings.Contains(s, "EOF") || strings.Contains(s, "connection reset") ||
		strings.Contains(s, "broken pipe") || strings.Contains(s, "i/o timeout") ||
		strings.Contains(s, "connection refused")
}
