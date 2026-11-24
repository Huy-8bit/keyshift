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
	"errors"
	"fmt"
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
	metrics  *collector // nil unless Metrics.Registerer is set

	policyMu     sync.Mutex
	manualPolicy bool
	lastContact  atomic.Int64
	policyErrors atomic.Uint64

	reads, cachedReads, directReads observability.Counter
	bypasses, writes, deletes       observability.Counter
	notifiedWrites                  observability.Counter
	errorsTotal, fallbacks          observability.Counter

	agg *telemetryAgg
	seq atomic.Uint64

	closed    atomic.Bool
	stop      chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
	closeErr  error
}

// backendFactory creates the Redis backend once the engine exists, so that
// the backend's hooks can report fills and invalidations to it.
type backendFactory func(rediscompat.Hooks, rediscompat.CacheLimits) (rediscompat.Backend, error)

// New creates a Client and the underlying Redis client described by
// cfg.Redis. Close releases both.
func New(cfg Config) (*Client, error) {
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("keyshift: invalid config: %w", err)
	}
	addrs := cfg.Redis.addrs()
	if len(addrs) == 0 {
		return nil, errors.New("keyshift: Redis.Addr or Redis.Addrs is required")
	}
	switch cfg.Redis.Driver {
	case DriverGoRedis:
		uc := newGoRedisClient(cfg.Redis, addrs)
		return newClient(cfg, func(h rediscompat.Hooks, l rediscompat.CacheLimits) (rediscompat.Backend, error) {
			return newGoRedisBackend(cfg, uc, true, h, l)
		})
	case DriverRueidis:
		return NewFromRueidis(rueidisOption(cfg.Redis, addrs), cfg)
	default:
		return nil, fmt.Errorf("keyshift: unknown driver %q", cfg.Redis.Driver)
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
	return newClient(cfg, func(h rediscompat.Hooks, l rediscompat.CacheLimits) (rediscompat.Backend, error) {
		return newRueidisBackend(cfg, opt, h, l)
	})
}

func newClient(cfg Config, mk backendFactory) (*Client, error) {
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
	if c.engine, err = c.newEngine(); err != nil {
		return nil, fmt.Errorf("keyshift: %w", err)
	}

	hooks := rediscompat.Hooks{OnFill: c.engine.RecordFill, OnInvalidate: c.engine.RecordInvalidation}
	if c.backend, err = mk(hooks, cfg.cacheLimits()); err != nil {
		return nil, fmt.Errorf("keyshift: %w", err)
	}
	c.goredis, _ = c.backend.(*rediscompat.GoRedis)
	c.rueidis, _ = c.backend.(*rediscompat.Rueidis)

	caps := c.backend.Capabilities()
	c.startMonitor(caps)
	c.agg = newTelemetryAgg(cfg.Telemetry.MaxCandidates)
	if cfg.Telemetry.Endpoint != "" {
		if err := c.startTelemetry(); err != nil {
			_ = c.shutdown()
			return nil, err
		}
	}
	if err := c.registerMetrics(); err != nil {
		_ = c.shutdown()
		return nil, err
	}

	c.wg.Add(1)
	go c.loop()
	cfg.Logger.Info("keyshift client started", "service", cfg.ServiceName, "instance", c.instanceID,
		"driver", caps.Driver, "topology", caps.Topology, "caching", caps.Caching, "tracking", c.monitor.Status().State)
	return c, nil
}

func (c *Client) newEngine() (*admission.Engine, error) {
	cfg := c.cfg
	return admission.New(admission.Config{
		Detector: detector.Config{
			Capacity:    cfg.Detection.MaxTrackingCandidates,
			TopK:        cfg.Detection.TopK,
			SampleRate:  cfg.Detection.SampleRate,
			MaxKeyBytes: cfg.Detection.MaxKeyBytes,
		},
		MaxStaleness:  cfg.Cache.MaxStaleness,
		MaxReportKeys: max(cfg.Telemetry.MaxCandidates, 50),
		Fingerprint:   c.hasher.Fingerprint,
		// The backend is created after the engine; these callbacks run only
		// once the client is fully constructed.
		OnDemote:   func(k string) { c.backend.Forget(k) },
		OnSuspend:  func() { c.backend.FlushLocal() },
		InstanceID: c.instanceID,
	}, policy.Resolve(c.local, nil))
}

// startMonitor confirms client tracking and keeps probing it. Without a
// working cache path, caching is reported as permanently unavailable.
func (c *Client) startMonitor(caps rediscompat.Capabilities) {
	if !caps.Caching {
		state := tracking.StateUnsupported
		if cachingDisabledReason(c.cfg) != "" {
			state = tracking.StateDisabled
		}
		c.monitor = tracking.Fixed(state, caps.Reason)
		c.engine.SetUnavailable(caps.Reason)
		c.cfg.Logger.Info("keyshift local caching unavailable; reads go directly to Redis", "reason", caps.Reason)
		return
	}
	c.monitor = tracking.NewMonitor(tracking.Config{
		Probe:    c.backend.CheckTracking,
		Interval: c.cfg.Tracking.HealthInterval,
		Timeout:  c.cfg.Tracking.HealthTimeout,
		OnChange: c.onTrackingChange,
	})
	c.monitor.CheckNow(context.Background())
	c.monitor.Start()
}

// registerMetrics registers the Prometheus collector. Nothing is measured on
// the request path unless a registerer is configured.
func (c *Client) registerMetrics() error {
	reg := c.cfg.Metrics.Registerer
	if reg == nil {
		return nil
	}
	col := newCollector(c)
	if err := reg.Register(col); err != nil {
		return fmt.Errorf("keyshift: register metrics: %w", err)
	}
	c.metrics = col
	return nil
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

// Explain reports why key is or is not cached.
func (c *Client) Explain(key string) Decision { return c.engine.Explain(key) }

// InstanceID returns this client's instance identity.
func (c *Client) InstanceID() string { return c.instanceID }

// Fingerprint returns the telemetry fingerprint of key, which is how the
// controller, CLI, and Grafana identify it.
func (c *Client) Fingerprint(key string) string { return c.hasher.Fingerprint(key) }

// GoRedis returns the application go-redis client (nil for other drivers).
// Writes through it bypass KeyShift's local-entry invalidation; call
// NotifyWrite after such writes, or rely on server tracking, which
// invalidates cached entries asynchronously.
func (c *Client) GoRedis() redis.UniversalClient {
	if c.goredis == nil {
		return nil
	}
	return c.goredis.Direct()
}

// Rueidis returns the rueidis client (nil for other drivers). The same
// caveat as for GoRedis applies to writes made through it.
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

// shutdown releases everything newClient started. It tolerates partially
// constructed clients.
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
	if c.monitor != nil {
		c.monitor.Stop()
	}
	if c.metrics != nil {
		c.cfg.Metrics.Registerer.Unregister(c.metrics)
	}
	return c.backend.Close()
}
