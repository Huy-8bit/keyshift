package rediscompat

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// GoRedisOptions configures the go-redis adapter.
type GoRedisOptions struct {
	// Direct is the application's client. It serves writes and every read
	// that is not admitted for local caching. It is never put into tracking
	// mode by KeyShift.
	Direct redis.UniversalClient
	// OwnsDirect makes Close also close Direct.
	OwnsDirect bool
	// Limits bounds the local cache.
	Limits CacheLimits
	// TrackedPoolSize sizes the dedicated tracked client's pool (0 uses the
	// go-redis default).
	TrackedPoolSize int
	// DrainInterval is how often go-redis sweeps idle tracked connections
	// for buffered invalidations (0 uses the go-redis default of 5ms). Cached
	// reads do not touch the network, so for idle connections this interval
	// bounds how long an arrived invalidation waits before it is applied.
	DrainInterval time.Duration
	// DisableCaching skips creating the tracked client.
	DisableCaching bool
	Hooks          Hooks
}

// GoRedis is the go-redis adapter.
//
// Design: admitted keys are read through a dedicated go-redis client that
// has go-redis's native client-side cache (RESP3, CLIENT TRACKING) enabled.
// Every other read and every write goes through the application's ordinary
// client. Because Redis tracks only keys read by tracking-enabled
// connections, the server-side tracking table and invalidation traffic are
// limited to keys this instance actually decided to cache: the same effect as
// OPTIN tracking, without reimplementing the tracking protocol.
type GoRedis struct {
	direct     redis.UniversalClient
	ownsDirect bool
	tracked    *redis.Client
	cache      *gatedCache
	caps       Capabilities
}

// NewGoRedis builds the adapter and verifies that tracking works. When
// caching cannot be enabled the adapter still works for direct access and
// Capabilities().Caching is false with a reason.
func NewGoRedis(ctx context.Context, o GoRedisOptions) (*GoRedis, error) {
	if o.Direct == nil {
		return nil, errors.New("rediscompat: go-redis Direct client is required")
	}
	g := &GoRedis{direct: o.Direct, ownsDirect: o.OwnsDirect,
		caps: Capabilities{Driver: "go-redis", TrackingMode: "default (dedicated tracked client; admitted keys only)"}}

	var base *redis.Options
	switch c := o.Direct.(type) {
	case *redis.Client:
		g.caps.Topology = TopologyStandalone
		base = c.Options()
	case *redis.ClusterClient:
		g.caps.Topology = TopologyCluster
		g.caps.Reason = "go-redis " + redis.Version() + " supports client-side caching only on standalone clients; use the rueidis driver for Redis Cluster caching"
	default:
		g.caps.Topology = TopologyOther
		g.caps.Reason = fmt.Sprintf("client type %T does not support client-side caching", o.Direct)
	}
	g.caps.ServerVersion = serverVersion(ctx, o.Direct)

	switch {
	case o.DisableCaching:
		g.caps.Reason = "caching disabled by configuration"
	case base == nil:
	case base.DB != 0:
		g.caps.Reason = "go-redis client-side caching is restricted to DB 0"
	case base.CredentialsProvider != nil || base.CredentialsProviderContext != nil || base.StreamingCredentialsProvider != nil:
		g.caps.Reason = "go-redis disables client-side caching when a credentials provider is configured"
	default:
		g.cache = newGatedCache(o.Limits, base.DB, base.Username, o.Hooks)
		opts := *base
		// go-redis stores derived state in the Options of a constructed
		// client: NewClient writes its push-notification processor back into
		// Options.PushNotificationProcessor. The tracked client must own a
		// separate processor (client-side caching requires the built-in one
		// and registers its invalidation handler on it), and a separate
		// maintenance-notification config.
		opts.PushNotificationProcessor = nil
		if base.MaintNotificationsConfig != nil {
			mc := *base.MaintNotificationsConfig
			opts.MaintNotificationsConfig = &mc
		}
		opts.Protocol = 3
		opts.ClientSideCache = g.cache
		// ClientSideCache takes precedence for storage; the config is only
		// read for the drainer interval.
		opts.ClientSideCacheConfig = &redis.ClientSideCacheConfig{DrainInterval: o.DrainInterval}
		if o.TrackedPoolSize > 0 {
			opts.PoolSize = o.TrackedPoolSize
		}
		if opts.ClientName == "" {
			opts.ClientName = "keyshift-tracked"
		}
		g.tracked = redis.NewClient(&opts)
		if err := g.CheckTracking(ctx); err != nil {
			_ = g.tracked.Close()
			g.tracked, g.cache = nil, nil
			g.caps.Reason = "client tracking could not be verified: " + err.Error()
		} else {
			g.caps.Caching = true
		}
	}
	return g, nil
}

func serverVersion(ctx context.Context, c redis.UniversalClient) string {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	info, err := c.Info(ctx, "server").Result()
	if err != nil {
		return ""
	}
	return ParseInfoField(info, "redis_version")
}

// ParseInfoField extracts a field from INFO output.
func ParseInfoField(info, field string) string {
	for _, line := range strings.Split(info, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), field+":"); ok {
			return v
		}
	}
	return ""
}

// Tracked returns the dedicated tracking-enabled client (nil when caching is
// unavailable). Exposed for tests.
func (g *GoRedis) Tracked() *redis.Client { return g.tracked }

// Direct returns the application client.
func (g *GoRedis) Direct() redis.UniversalClient { return g.direct }

func (g *GoRedis) GetDirect(ctx context.Context, key string) ([]byte, error) {
	b, err := g.direct.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, ErrNil
	}
	return b, err
}

func (g *GoRedis) GetCached(ctx context.Context, key string) ([]byte, error) {
	if g.tracked == nil {
		return g.GetDirect(ctx, key)
	}
	b, err := g.tracked.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, ErrNil
	}
	return b, err
}

func (g *GoRedis) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	err := g.direct.Set(ctx, key, value, ttl).Err()
	// Drop the local entry regardless of the outcome: on error the write may
	// still have been applied.
	g.Forget(key)
	return err
}

func (g *GoRedis) Del(ctx context.Context, keys ...string) (int64, error) {
	n, err := g.direct.Del(ctx, keys...).Result()
	for _, k := range keys {
		g.Forget(k)
	}
	return n, err
}

func (g *GoRedis) Forget(key string) {
	if g.cache != nil {
		g.cache.forget(key)
	}
}

func (g *GoRedis) FlushLocal() int {
	if g.cache == nil {
		return 0
	}
	return g.cache.inner.Flush()
}

func (g *GoRedis) CacheStats() CacheStats {
	if g.cache == nil {
		return CacheStats{ResidencyKnown: true}
	}
	return g.cache.stats()
}

func (g *GoRedis) Capabilities() Capabilities { return g.caps }

// CheckTracking asks a tracked connection for CLIENT TRACKINGINFO.
func (g *GoRedis) CheckTracking(ctx context.Context) error {
	if g.tracked == nil {
		return errors.New("tracked client not available")
	}
	res, err := g.tracked.Do(ctx, "CLIENT", "TRACKINGINFO").Result()
	if err != nil {
		return err
	}
	return checkTrackingInfo(res)
}

func (g *GoRedis) Ping(ctx context.Context) error { return g.direct.Ping(ctx).Err() }

func (g *GoRedis) Close() error {
	var errs []error
	if g.tracked != nil {
		errs = append(errs, g.tracked.Close())
	}
	if g.ownsDirect {
		errs = append(errs, g.direct.Close())
	}
	return errors.Join(errs...)
}

// checkTrackingInfo validates a CLIENT TRACKINGINFO reply (RESP3 map or
// RESP2 flat array) and requires the "on" flag.
func checkTrackingInfo(res any) error {
	var flags any
	switch v := res.(type) {
	case map[any]any:
		flags = v["flags"]
	case map[string]any:
		flags = v["flags"]
	case []any:
		for i := 0; i+1 < len(v); i += 2 {
			if s, _ := v[i].(string); s == "flags" {
				flags = v[i+1]
			}
		}
	}
	list, ok := flags.([]any)
	if !ok {
		return fmt.Errorf("unexpected CLIENT TRACKINGINFO reply %v", res)
	}
	for _, f := range list {
		if s, _ := f.(string); s == "on" {
			return nil
		}
	}
	return fmt.Errorf("tracking is not on (flags %v)", list)
}

// gatedCache wraps go-redis's LocalCache to observe fills, invalidations,
// and removals. All correctness-critical behavior (reservations, races
// between fills and invalidations, connection-loss eviction, staleness
// backstop) is delegated to LocalCache.
type gatedCache struct {
	inner  *redis.LocalCache
	prefix string
	hooks  Hooks

	hits, misses, fillsOK      atomic.Uint64
	removed, invals, flushes   atomic.Uint64
	connEvicts, evictionsShown atomic.Uint64
	staleFills                 atomic.Uint64
	guard                      *fillGuard
}

var _ redis.Cache = (*gatedCache)(nil)

func newGatedCache(l CacheLimits, db int, username string, hooks Hooks) *gatedCache {
	return &gatedCache{
		inner: redis.NewLocalCache(redis.CacheConfig{
			MaxEntries:     l.MaxEntries,
			MaxMemoryBytes: l.MaxMemoryBytes,
			MaxStaleness:   l.MaxStaleness,
		}),
		// Mirrors go-redis's cache namespace (DB and fixed ACL username) so
		// Redis keys can be mapped back to application keys. Covered by
		// integration tests against the pinned go-redis version.
		prefix: strconv.Itoa(db) + "\x00" + strconv.Itoa(len(username)) + ":" + username + "\x00",
		hooks:  hooks,
		guard:  newFillGuard(),
	}
}

// NewInstrumentedCache returns a go-redis LocalCache wrapped with the same
// counters KeyShift uses, for use as Options.ClientSideCache. It caches every
// cacheable read (conventional client-side caching) and exists so benchmark
// baselines are measured with identical instrumentation.
func NewInstrumentedCache(l CacheLimits) (redis.Cache, func() CacheStats) {
	c := newGatedCache(l, 0, "", Hooks{})
	return c, c.stats
}

func (c *gatedCache) strip(nsKey string) string {
	if k, ok := strings.CutPrefix(nsKey, c.prefix); ok {
		return k
	}
	return nsKey
}

func (c *gatedCache) Get(ctx context.Context, cacheKey string) ([]byte, bool) {
	v, ok := c.inner.Get(ctx, cacheKey)
	if ok {
		c.hits.Add(1)
	}
	return v, ok
}

func (c *gatedCache) Reserve(cacheKey string, redisKeys []string) (uint64, bool) {
	token, should := c.inner.Reserve(cacheKey, redisKeys)
	if should {
		c.misses.Add(1)
		if len(redisKeys) == 1 {
			c.guard.track(token, c.strip(redisKeys[0]))
		}
		if c.hooks.OnFill != nil {
			for _, k := range redisKeys {
				c.hooks.OnFill(c.strip(k))
			}
		}
	}
	return token, should
}

func (c *gatedCache) FulfillOwned(cacheKey string, token, ownerConnID uint64, value []byte) bool {
	if c.guard.finish(token) {
		// A local write to this key completed after the fill started: the
		// value may predate it. Do not cache it (waiters refetch).
		c.staleFills.Add(1)
		c.inner.Cancel(cacheKey, token)
		return false
	}
	ok := c.inner.FulfillOwned(cacheKey, token, ownerConnID, value)
	if ok {
		c.fillsOK.Add(1)
	}
	return ok
}

func (c *gatedCache) Cancel(cacheKey string, token uint64) bool {
	c.guard.finish(token)
	return c.inner.Cancel(cacheKey, token)
}

// DeleteByRedisKey is invoked by go-redis for server invalidations.
func (c *gatedCache) DeleteByRedisKey(redisKey string) int {
	n := c.inner.DeleteByRedisKey(redisKey)
	c.removed.Add(uint64(n))
	c.invals.Add(1)
	if c.hooks.OnInvalidate != nil {
		c.hooks.OnInvalidate(c.strip(redisKey))
	}
	return n
}

func (c *gatedCache) DeleteByCacheKey(cacheKey string) bool {
	ok := c.inner.DeleteByCacheKey(cacheKey)
	if ok {
		c.removed.Add(1)
	}
	return ok
}

func (c *gatedCache) EvictByConn(connID uint64) int {
	n := c.inner.EvictByConn(connID)
	c.removed.Add(uint64(n))
	c.connEvicts.Add(uint64(n))
	return n
}

func (c *gatedCache) Flush() int {
	n := c.inner.Flush()
	c.removed.Add(uint64(n))
	c.flushes.Add(1)
	return n
}

// Stats lets go-redis's Client.CSCStats report through the wrapper.
func (c *gatedCache) Stats() redis.CSCStats {
	return redis.CSCStats{Hits: c.hits.Load(), Misses: c.misses.Load(), Entries: c.inner.Len(), MemoryUsageBytes: c.inner.MemoryUsage()}
}

// forget drops the local entry for an application key after a local write
// and invalidates fills of that key that are still in flight.
func (c *gatedCache) forget(key string) {
	c.guard.forgot(key)
	n := c.inner.DeleteByRedisKey(c.prefix + key)
	c.removed.Add(uint64(n))
}

func (c *gatedCache) stats() CacheStats {
	entries := int64(c.inner.Len())
	// LocalCache does not report evictions directly. Every successful fill
	// either is still resident or was removed by an observable path
	// (invalidation, local write, connection loss, flush) or evicted for
	// capacity/staleness. The derived counter is kept monotonic.
	derived := int64(c.fillsOK.Load()) - int64(c.removed.Load()) - entries
	if derived > 0 {
		for {
			shown := c.evictionsShown.Load()
			if uint64(derived) <= shown || c.evictionsShown.CompareAndSwap(shown, uint64(derived)) {
				break
			}
		}
	}
	return CacheStats{
		Hits: c.hits.Load(), Misses: c.misses.Load(),
		Entries: entries, MemoryBytes: c.inner.MemoryUsage(),
		Invalidations: c.invals.Load(), Evictions: c.evictionsShown.Load(),
		ConnEvictions: c.connEvicts.Load(), Flushes: c.flushes.Load(),
		ResidencyKnown: true,
	}
}
