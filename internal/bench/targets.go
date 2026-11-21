package bench

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/rueidis"

	"github.com/Huy-8bit/keyshift/internal/rediscompat"
	"github.com/Huy-8bit/keyshift/pkg/keyshift"
)

// Mode names.
const (
	ModeNone            = "none"
	ModeCSC             = "csc"
	ModeKeyShift        = "keyshift"
	ModeKeyShiftRueidis = "keyshift-rueidis"
)

// AllModes lists the benchmark modes.
var AllModes = []string{ModeNone, ModeCSC, ModeKeyShift, ModeKeyShiftRueidis}

var errNil = errors.New("nil")

// targetStats are client-side statistics of a target.
type targetStats struct {
	LocalHits     uint64
	Invalidations uint64
	Entries       int64
	MemoryBytes   int64
	Admitted      int
}

// target is a client under test. Implementations are safe for concurrent use.
type target interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, v []byte, ttl time.Duration) error
	Stats() targetStats
	Close() error
}

type targetOptions struct {
	mode         string
	addr         string
	clusterAddrs []string
	name         string
	maxEntries   int
	maxBytes     int64
	maxStaleness time.Duration
	window       time.Duration
	telemetryURL string
	trackedPool  int
	drain        time.Duration
}

func newTarget(o targetOptions) (target, error) {
	limits := rediscompat.CacheLimits{MaxEntries: o.maxEntries, MaxMemoryBytes: o.maxBytes, MaxStaleness: o.maxStaleness}
	cluster := len(o.clusterAddrs) > 0
	switch o.mode {
	case ModeNone:
		if cluster {
			return &goredisTarget{c: redis.NewClusterClient(&redis.ClusterOptions{Addrs: o.clusterAddrs, ClientName: o.name})}, nil
		}
		return &goredisTarget{c: redis.NewClient(&redis.Options{Addr: o.addr, ClientName: o.name})}, nil
	case ModeCSC:
		if cluster {
			// go-redis has no cluster client-side caching; use rueidis
			// caching every read (its default tracking mode).
			c, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: o.clusterAddrs, ClientName: o.name,
				CacheSizeEachConn: int(o.maxBytes)})
			if err != nil {
				return nil, err
			}
			return &rueidisCSCTarget{c: c, ttl: o.maxStaleness}, nil
		}
		cache, stats := rediscompat.NewInstrumentedCache(limits)
		c := redis.NewClient(&redis.Options{Addr: o.addr, Protocol: 3, ClientName: o.name, ClientSideCache: cache})
		return &goredisTarget{c: c, stats: stats}, nil
	case ModeKeyShift, ModeKeyShiftRueidis:
		cfg := keyshift.Config{
			ServiceName: "bench", InstanceID: o.name,
			Detection: keyshift.DetectionConfig{Window: o.window},
			Cache:     keyshift.CacheConfig{MaxEntries: o.maxEntries, MaxMemoryBytes: o.maxBytes, MaxStaleness: o.maxStaleness},
			Metrics:   keyshift.MetricsConfig{DisableLatencyHistograms: true},
			Tracking:  keyshift.TrackingConfig{TrackedPoolSize: o.trackedPool, DrainInterval: o.drain},
		}
		if o.telemetryURL != "" {
			cfg.Telemetry = keyshift.TelemetryConfig{Endpoint: o.telemetryURL, Interval: 2 * time.Second}
		}
		var c *keyshift.Client
		var err error
		if o.mode == ModeKeyShiftRueidis || cluster {
			addrs := o.clusterAddrs
			if !cluster {
				addrs = []string{o.addr}
			}
			c, err = keyshift.NewFromRueidis(rueidis.ClientOption{InitAddress: addrs, ClientName: o.name, ForceSingleClient: !cluster}, cfg)
		} else {
			cfg.Redis = keyshift.RedisConfig{Addr: o.addr}
			c, err = keyshift.New(cfg)
		}
		if err != nil {
			return nil, err
		}
		return &keyshiftTarget{c: c}, nil
	}
	return nil, fmt.Errorf("unknown mode %q", o.mode)
}

type goredisTarget struct {
	c     redis.UniversalClient
	stats func() rediscompat.CacheStats
}

func (t *goredisTarget) Get(ctx context.Context, k string) ([]byte, error) {
	b, err := t.c.Get(ctx, k).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, errNil
	}
	return b, err
}

func (t *goredisTarget) Set(ctx context.Context, k string, v []byte, ttl time.Duration) error {
	return t.c.Set(ctx, k, v, ttl).Err()
}

func (t *goredisTarget) Stats() targetStats {
	if t.stats == nil {
		return targetStats{}
	}
	s := t.stats()
	return targetStats{LocalHits: s.Hits, Invalidations: s.Invalidations, Entries: s.Entries, MemoryBytes: s.MemoryBytes}
}

func (t *goredisTarget) Close() error { return t.c.Close() }

type rueidisCSCTarget struct {
	c    rueidis.Client
	ttl  time.Duration
	hits counter
}

func (t *rueidisCSCTarget) Get(ctx context.Context, k string) ([]byte, error) {
	res := t.c.DoCache(ctx, t.c.B().Get().Key(k).Cache(), t.ttl)
	if res.IsCacheHit() {
		t.hits.inc()
	}
	b, err := res.AsBytes()
	if rueidis.IsRedisNil(err) {
		return nil, errNil
	}
	return b, err
}

func (t *rueidisCSCTarget) Set(ctx context.Context, k string, v []byte, ttl time.Duration) error {
	b := t.c.B().Set().Key(k).Value(rueidis.BinaryString(v))
	if ttl > 0 {
		return t.c.Do(ctx, b.Px(ttl).Build()).Error()
	}
	return t.c.Do(ctx, b.Build()).Error()
}

func (t *rueidisCSCTarget) Stats() targetStats {
	return targetStats{LocalHits: t.hits.load(), Entries: -1, MemoryBytes: -1}
}

func (t *rueidisCSCTarget) Close() error { t.c.Close(); return nil }

type keyshiftTarget struct{ c *keyshift.Client }

func (t *keyshiftTarget) Get(ctx context.Context, k string) ([]byte, error) {
	b, err := t.c.GetBytes(ctx, k)
	if errors.Is(err, keyshift.Nil) {
		return nil, errNil
	}
	return b, err
}

func (t *keyshiftTarget) Set(ctx context.Context, k string, v []byte, ttl time.Duration) error {
	return t.c.Set(ctx, k, v, ttl)
}

func (t *keyshiftTarget) Stats() targetStats {
	s := t.c.Stats()
	return targetStats{LocalHits: s.LocalHits, Invalidations: s.Invalidations, Entries: s.Entries,
		MemoryBytes: s.MemoryBytes, Admitted: s.AdmittedKeys}
}

func (t *keyshiftTarget) Close() error { return t.c.Close() }
