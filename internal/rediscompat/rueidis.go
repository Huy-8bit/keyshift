package rediscompat

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/redis/rueidis"
)

// OptInTracking returns a copy of opt configured for OPTIN client tracking,
// which makes Redis track only keys read with DoCache.
func OptInTracking(opt rueidis.ClientOption) rueidis.ClientOption {
	if !slices.Contains(opt.ClientTrackingOptions, "OPTIN") {
		opt.ClientTrackingOptions = append(slices.Clone(opt.ClientTrackingOptions), "OPTIN")
	}
	return opt
}

// RueidisOptions configures the rueidis adapter.
type RueidisOptions struct {
	// ClientOption, when set, makes the adapter create and own the client.
	// OPTIN tracking and a KeyShift-managed cache store (global budget,
	// per-key forget after local writes, statistics) are installed.
	ClientOption *rueidis.ClientOption
	// Client is an existing client used when ClientOption is nil. It should
	// be created with OptInTracking; without OPTIN Redis tracks every key
	// read on its connections. Local entries cannot be dropped after local
	// writes for a caller-supplied client (invalidation is asynchronous).
	Client     rueidis.Client
	OwnsClient bool
	// Limits.MaxStaleness is used as the client-side TTL of cached reads.
	Limits CacheLimits
	// Cluster marks the topology for reporting.
	Cluster bool
	// CachingUnavailable, when non-empty, disables the cache path with the
	// given reason (for example when the client had to be created with
	// DisableCache because the server lacks RESP3).
	CachingUnavailable string
	Hooks              Hooks
}

// Rueidis is the rueidis adapter. rueidis implements RESP3 client tracking,
// OPTIN (CLIENT CACHING YES before each DoCache), cluster redirections, and
// cache flushing on connection loss.
// Commands are pinned (Completed.Pin) so rueidis never recycles their
// buffers. With rueidis v1.0.80 the race detector reports a command buffer
// being reused by the builder pool while the pipe's background writer still
// reads it after a connection is cut; pinning avoids the shared buffer at the
// cost of one allocation per command.
type Rueidis struct {
	c     rueidis.Client
	reg   *storeRegistry // nil for caller-supplied clients
	owns  bool
	ttl   time.Duration
	hooks Hooks
	caps  Capabilities

	hits, misses atomic.Uint64
}

// NewRueidis builds the adapter and verifies tracking.
func NewRueidis(ctx context.Context, o RueidisOptions) (*Rueidis, error) {
	if o.Limits.MaxStaleness <= 0 {
		return nil, errors.New("rediscompat: MaxStaleness must be > 0 for rueidis")
	}
	var reg *storeRegistry
	if o.ClientOption != nil {
		opt := OptInTracking(*o.ClientOption)
		reg = newStoreRegistry(o.Limits, o.Hooks)
		opt.NewCacheStoreFn = reg.NewCacheStoreFn
		c, err := rueidis.NewClient(opt)
		if errors.Is(err, rueidis.ErrNoCache) {
			// No RESP3 or CLIENT TRACKING (old server or ACL restrictions):
			// fall back to an uncached client.
			o.CachingUnavailable = "client tracking unavailable: the server lacks RESP3/CLIENT TRACKING or ACL denies CLIENT commands (" + err.Error() + ")"
			opt.DisableCache, opt.ClientTrackingOptions, opt.NewCacheStoreFn = true, nil, nil
			c, err = rueidis.NewClient(opt)
			reg = nil
		}
		if err != nil {
			return nil, err
		}
		o.Client, o.OwnsClient = c, true
	}
	if o.Client == nil {
		return nil, errors.New("rediscompat: rueidis Client or ClientOption is required")
	}
	r := &Rueidis{c: o.Client, reg: reg, owns: o.OwnsClient, ttl: o.Limits.MaxStaleness, hooks: o.Hooks,
		caps: Capabilities{Driver: "rueidis", Topology: TopologyStandalone}}
	if o.Cluster {
		r.caps.Topology = TopologyCluster
	}
	if info, err := o.Client.Do(ctx, o.Client.B().Info().Section("server").Build()).ToString(); err == nil {
		r.caps.ServerVersion = ParseInfoField(info, "redis_version")
	}
	if o.CachingUnavailable != "" {
		r.caps.Reason = o.CachingUnavailable
		return r, nil
	}
	if err := r.CheckTracking(ctx); err != nil {
		// Unverifiable tracking disables caching, not the adapter: direct
		// Redis access keeps working.
		r.caps.Reason = "client tracking could not be verified: " + err.Error()
		return r, nil //nolint:nilerr // degraded mode is reported through Capabilities
	}
	r.caps.Caching = true
	return r, nil
}

// Client returns the underlying rueidis client.
func (r *Rueidis) Client() rueidis.Client { return r.c }

func (r *Rueidis) GetDirect(ctx context.Context, key string) ([]byte, error) {
	b, err := r.c.Do(ctx, r.c.B().Get().Key(key).Build().Pin()).AsBytes()
	if rueidis.IsRedisNil(err) {
		return nil, ErrNil
	}
	return b, err
}

func (r *Rueidis) GetCached(ctx context.Context, key string) ([]byte, error) {
	if !r.caps.Caching {
		return r.GetDirect(ctx, key)
	}
	var start uint64
	if r.reg != nil {
		start = r.reg.guard.begin()
	}
	// ToStaticTTL skips rueidis's MULTI/PTTL/GET/EXEC wrapper: freshness
	// comes from tracking invalidations (including expiry) with max_staleness
	// as the bound, exactly like the go-redis path. It also saves three
	// commands per miss and avoids requiring MULTI/EXEC/PTTL ACL permissions.
	res := r.c.DoCache(ctx, r.c.B().Get().Key(key).Cache().ToStaticTTL().Pin(), r.ttl)
	if res.IsCacheHit() {
		r.hits.Add(1)
	} else if res.NonRedisError() == nil {
		r.misses.Add(1)
		if r.hooks.OnFill != nil {
			r.hooks.OnFill(key)
		}
		// rueidis exposes no reservation hook, so a fill that overlapped a
		// local write of this key is dropped right after it is stored.
		if r.reg != nil && r.reg.guard.stale(key, start) {
			r.reg.forget(key)
		}
	}
	b, err := res.AsBytes()
	if rueidis.IsRedisNil(err) {
		return nil, ErrNil
	}
	return b, err
}

func (r *Rueidis) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	b := r.c.B().Set().Key(key).Value(rueidis.BinaryString(value))
	var err error
	if ttl > 0 {
		err = r.c.Do(ctx, b.Px(ttl).Build().Pin()).Error()
	} else {
		err = r.c.Do(ctx, b.Build().Pin()).Error()
	}
	r.Forget(key)
	return err
}

func (r *Rueidis) Del(ctx context.Context, keys ...string) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	n, err := r.c.Do(ctx, r.c.B().Del().Key(keys...).Build().Pin()).AsInt64()
	for _, k := range keys {
		r.Forget(k)
	}
	return n, err
}

// Forget drops local entries for key on every connection. Server
// invalidations for a local write may arrive on a different multiplexed
// connection after the write returns, so this is required for
// read-your-writes within one instance. It is a no-op for caller-supplied
// clients.
func (r *Rueidis) Forget(key string) {
	if r.reg != nil {
		r.reg.forget(key)
	}
}

// FlushLocal drops all local entries (-1 for caller-supplied clients).
func (r *Rueidis) FlushLocal() int {
	if r.reg == nil {
		return -1
	}
	return r.reg.flushAll()
}

func (r *Rueidis) CacheStats() CacheStats {
	st := CacheStats{Hits: r.hits.Load(), Misses: r.misses.Load(), Entries: -1, MemoryBytes: -1}
	if r.reg != nil {
		st.Entries, st.MemoryBytes = r.reg.entries.Load(), r.reg.bytes.Load()
		st.Invalidations, st.Evictions, st.Flushes = r.reg.invals.Load(), r.reg.evictions.Load(), r.reg.flushes.Load()
		st.ResidencyKnown = true
	}
	return st
}

func (r *Rueidis) Capabilities() Capabilities {
	c := r.caps
	if c.Caching {
		c.TrackingMode = "optin"
	}
	return c
}

func (r *Rueidis) CheckTracking(ctx context.Context) error {
	res, err := r.c.Do(ctx, r.c.B().ClientTrackinginfo().Build()).ToAny()
	if err != nil {
		return err
	}
	return checkTrackingInfo(normalizeAny(res))
}

func (r *Rueidis) Ping(ctx context.Context) error {
	return r.c.Do(ctx, r.c.B().Ping().Build()).Error()
}

func (r *Rueidis) Close() error {
	if r.owns {
		r.c.Close()
	}
	return nil
}

// normalizeAny converts rueidis ToAny output (map[string]any) into the
// shapes checkTrackingInfo understands.
func normalizeAny(v any) any {
	switch t := v.(type) {
	case map[string]any:
		return t
	case []any:
		return t
	case string:
		return t
	case int64:
		return strconv.FormatInt(t, 10)
	}
	return v
}
