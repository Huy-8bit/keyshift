package keyshift

import (
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Huy-8bit/keyshift/internal/policy"
	"github.com/Huy-8bit/keyshift/internal/rediscompat"
)

// CacheMode selects how keys become eligible for local caching.
type CacheMode = policy.Mode

const (
	// Adaptive admits keys automatically from observed workload, plus keys
	// matching pin patterns.
	Adaptive CacheMode = policy.ModeAdaptive
	// Explicit admits only keys matching pin patterns.
	Explicit CacheMode = policy.ModeExplicit
	// Off disables local caching. Detection, metrics, and telemetry keep
	// working.
	Off CacheMode = policy.ModeOff
)

// Driver selects the Redis client library used by New.
type Driver string

const (
	// DriverGoRedis uses github.com/redis/go-redis/v9 (default). Local
	// caching is available for standalone Redis only.
	DriverGoRedis Driver = "go-redis"
	// DriverRueidis uses github.com/redis/rueidis with OPTIN tracking.
	// Local caching is available for standalone Redis and Redis Cluster.
	DriverRueidis Driver = "rueidis"
)

// Config configures a Client. The zero value of every optional field selects
// a documented default; see docs/configuration.md.
type Config struct {
	// ServiceName identifies the application (required). It is used in
	// metrics and telemetry and selects service-scoped policies.
	ServiceName string
	// InstanceID identifies this process. Default: $POD_NAME or hostname,
	// plus a random suffix so restarts are distinct instances.
	InstanceID string

	Redis     RedisConfig
	Detection DetectionConfig
	Cache     CacheConfig
	Tracking  TrackingConfig
	Telemetry TelemetryConfig
	Metrics   MetricsConfig

	// Logger receives structured logs. Default discards logs.
	Logger *slog.Logger
}

// RedisConfig describes the Redis connection used by New. It is ignored by
// NewFromGoRedis and NewFromRueidis.
type RedisConfig struct {
	Driver Driver
	// Addr is a single address. Addrs lists cluster seed nodes.
	Addr  string
	Addrs []string
	// Cluster selects Redis Cluster.
	Cluster  bool
	Username string
	// Password is never logged or exported.
	Password string
	// DB selects the logical database (standalone only). go-redis local
	// caching requires DB 0.
	DB        int
	TLSConfig *tls.Config
	// PoolSize sizes the application connection pool (go-redis).
	PoolSize                               int
	DialTimeout, ReadTimeout, WriteTimeout time.Duration
}

// DetectionConfig configures local hot-key detection.
type DetectionConfig struct {
	// Disabled turns off detection. Adaptive caching requires detection, so
	// disabling it also disables caching.
	Disabled bool
	// Window is the observation window. Default 10s.
	Window time.Duration
	// TopK is the number of candidates evaluated per window. Default 100.
	TopK int
	// SampleRate in (0, 1] is the fraction of uncached reads recorded.
	// Default 1.0.
	SampleRate float64
	// MaxTrackingCandidates bounds detector and admission memory. Default 1000.
	MaxTrackingCandidates int
	// MaxKeyBytes is the longest key tracked. Default 1024.
	MaxKeyBytes int
}

// CacheConfig configures the local cache and adaptive admission. Values are
// local hard limits: a remote policy can tighten but never loosen them.
type CacheConfig struct {
	// Mode defaults to Adaptive.
	Mode CacheMode
	// MaxMemoryBytes bounds the estimated local cache memory. Default 16 MiB.
	MaxMemoryBytes int64
	// MaxEntries bounds the number of cached entries. Default 1000.
	MaxEntries int
	// MaxEntryBytes is the largest value admitted. Default 64 KiB.
	MaxEntryBytes int64
	// MaxStaleness is the client-side staleness backstop: a cached entry is
	// refetched at least this often even if no invalidation arrives.
	// Default 3s. It bounds the best achievable hit ratio per key.
	MaxStaleness time.Duration
	// AdmissionCooldown is the base cooldown after ineffective caching.
	// Default 30s; repeated strikes back off exponentially.
	AdmissionCooldown time.Duration

	// Adaptive admission thresholds (zero selects defaults; see
	// docs/adaptive-admission.md).
	MinReadRate           float64
	MinHitRatio           float64
	ExitHitRatio          float64
	MinObservationWindows int
	ExitWindows           int

	// PinPatterns are key globs admitted without adaptive scoring. Pin only
	// read-mostly keys.
	PinPatterns []string
	// DenyPatterns are key globs that are never cached.
	DenyPatterns []string
}

// TrackingConfig configures Redis client tracking health checks.
type TrackingConfig struct {
	// HealthInterval between tracking probes. Default 1s.
	HealthInterval time.Duration
	// HealthTimeout per probe. Default 500ms. A probe that does not answer
	// in time suspends caching and flushes the local cache.
	HealthTimeout time.Duration
	// TrackedPoolSize sizes the dedicated tracked connection pool
	// (go-redis driver). Default: go-redis default.
	TrackedPoolSize int
	// DrainInterval is how often idle tracked connections are swept for
	// buffered invalidations (go-redis driver). Cache hits never touch the
	// network, so on idle connections this bounds how long an invalidation
	// that has already arrived waits before it is applied. Default 1ms
	// (go-redis uses 5ms); minimum 1ms.
	DrainInterval time.Duration
}

// TelemetryConfig configures optional reporting to a KeyShift controller.
type TelemetryConfig struct {
	// Endpoint is the controller base URL. Empty disables telemetry and
	// remote policies.
	Endpoint string
	// Token authenticates the SDK to the controller (ingest role).
	Token string
	// Interval between uploads. Default 30s (jittered).
	Interval time.Duration
	// MaxCandidates per upload. Default 20, maximum 200.
	MaxCandidates int
	// FingerprintSecret is the HMAC key for key fingerprints (>= 16 bytes).
	// Use the same secret for all instances whose keys should be aggregated
	// together. Empty uses a random per-process secret, which prevents
	// cross-instance aggregation.
	FingerprintSecret []byte
	// DisplayKeyPatterns lists key globs whose raw names may be sent to the
	// controller. Default none: only fingerprints are sent.
	DisplayKeyPatterns []string
	// DisableRemotePolicy ignores policies from the controller.
	DisableRemotePolicy bool
	// TLSConfig for controller connections. HTTPClient overrides it.
	TLSConfig  *tls.Config
	HTTPClient *http.Client
}

// MetricsConfig configures Prometheus metrics.
type MetricsConfig struct {
	// Registerer, when set, registers the client's collector. Metric labels
	// never contain Redis keys.
	Registerer prometheus.Registerer
	// DisableLatencyHistograms skips per-request latency histograms.
	DisableLatencyHistograms bool
}

func (c *Config) applyDefaults() {
	d := &c.Detection
	if d.Window == 0 {
		d.Window = 10 * time.Second
	}
	if d.TopK == 0 {
		d.TopK = 100
	}
	if d.SampleRate == 0 {
		d.SampleRate = 1
	}
	if d.MaxTrackingCandidates == 0 {
		d.MaxTrackingCandidates = 1000
	}
	if d.MaxKeyBytes == 0 {
		d.MaxKeyBytes = 1024
	}
	ca := &c.Cache
	if ca.Mode == "" {
		ca.Mode = Adaptive
	}
	if ca.MaxMemoryBytes == 0 {
		ca.MaxMemoryBytes = policy.DefaultBudget.MaxMemoryBytes
	}
	if ca.MaxEntries == 0 {
		ca.MaxEntries = policy.DefaultBudget.MaxEntries
	}
	if ca.MaxEntryBytes == 0 {
		ca.MaxEntryBytes = policy.DefaultAdmission.MaxEntryBytes
	}
	if ca.MaxStaleness == 0 {
		ca.MaxStaleness = 3 * time.Second
	}
	if ca.AdmissionCooldown == 0 {
		ca.AdmissionCooldown = time.Duration(policy.DefaultAdmission.CooldownSeconds) * time.Second
	}
	if c.Tracking.HealthInterval == 0 {
		c.Tracking.HealthInterval = time.Second
	}
	if c.Tracking.HealthTimeout == 0 {
		c.Tracking.HealthTimeout = 500 * time.Millisecond
	}
	if c.Tracking.DrainInterval == 0 {
		// Admitted keys mostly hit, so their tracked connections are idle
		// and invalidations wait for the drainer. 1ms (go-redis: 5ms) cut
		// stale reads 4-8x in the local-hot benchmark for ~6% client CPU.
		c.Tracking.DrainInterval = time.Millisecond
	}
	if c.Telemetry.Interval == 0 {
		c.Telemetry.Interval = 30 * time.Second
	}
	if c.Telemetry.MaxCandidates == 0 {
		c.Telemetry.MaxCandidates = 20
	}
	if c.Redis.Driver == "" {
		c.Redis.Driver = DriverGoRedis
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.DiscardHandler)
	}
}

// localPolicy converts cache settings into the local hard-limit policy.
func (c *Config) localPolicy() *policy.Policy {
	mode := c.Cache.Mode
	if c.Detection.Disabled {
		mode = Off
	}
	return &policy.Policy{
		Mode: mode,
		Admission: policy.Admission{
			MinReadRate:           c.Cache.MinReadRate,
			MinHitRatio:           c.Cache.MinHitRatio,
			ExitHitRatio:          c.Cache.ExitHitRatio,
			MinObservationWindows: c.Cache.MinObservationWindows,
			ExitWindows:           c.Cache.ExitWindows,
			CooldownSeconds:       int(c.Cache.AdmissionCooldown / time.Second),
			MaxEntryBytes:         c.Cache.MaxEntryBytes,
		},
		Budget:       policy.Budget{MaxEntries: c.Cache.MaxEntries, MaxMemoryBytes: c.Cache.MaxMemoryBytes},
		PinPatterns:  c.Cache.PinPatterns,
		DenyPatterns: c.Cache.DenyPatterns,
	}
}

// cacheLimits are the hard bounds of the client library's local cache.
func (c *Config) cacheLimits() rediscompat.CacheLimits {
	return rediscompat.CacheLimits{
		MaxEntries: c.Cache.MaxEntries, MaxMemoryBytes: c.Cache.MaxMemoryBytes, MaxStaleness: c.Cache.MaxStaleness,
	}
}

func (c *Config) validate() error {
	var errs []error
	if !policy.ValidName(c.ServiceName) || len(c.ServiceName) > policy.MaxServiceNameLen {
		errs = append(errs, fmt.Errorf("ServiceName %q must be 1-128 characters of [A-Za-z0-9._-]", c.ServiceName))
	}
	if c.InstanceID != "" && !policy.ValidName(c.InstanceID) {
		errs = append(errs, fmt.Errorf("InstanceID %q must use [A-Za-z0-9._-]", c.InstanceID))
	}
	if c.Detection.Window < 10*time.Millisecond || c.Detection.Window > time.Hour {
		errs = append(errs, errors.New("Detection.Window must be within [10ms, 1h]"))
	}
	if c.Detection.TopK < 1 || c.Detection.TopK > c.Detection.MaxTrackingCandidates {
		errs = append(errs, errors.New("Detection.TopK must be within [1, MaxTrackingCandidates]"))
	}
	if c.Detection.MaxTrackingCandidates > 1_000_000 {
		errs = append(errs, errors.New("Detection.MaxTrackingCandidates must be <= 1000000"))
	}
	if !(c.Detection.SampleRate > 0 && c.Detection.SampleRate <= 1) {
		errs = append(errs, errors.New("Detection.SampleRate must be within (0, 1]"))
	}
	if c.Cache.MaxMemoryBytes < 0 || c.Cache.MaxEntries < 0 || c.Cache.MaxEntryBytes < 0 {
		errs = append(errs, errors.New("cache limits must be positive"))
	}
	if c.Cache.MaxStaleness < 100*time.Millisecond {
		errs = append(errs, errors.New("Cache.MaxStaleness must be >= 100ms"))
	}
	if c.Telemetry.MaxCandidates < 0 || c.Telemetry.MaxCandidates > 200 {
		errs = append(errs, errors.New("Telemetry.MaxCandidates must be within [0, 200]"))
	}
	lp := c.localPolicy()
	if err := lp.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("cache policy settings: %w", err))
	}
	for _, p := range c.Telemetry.DisplayKeyPatterns {
		if err := policy.ValidatePattern(p); err != nil {
			errs = append(errs, fmt.Errorf("Telemetry.DisplayKeyPatterns: %w", err))
		}
	}
	return errors.Join(errs...)
}
