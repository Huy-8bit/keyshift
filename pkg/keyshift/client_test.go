package keyshift

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/rueidis"

	"github.com/Huy-8bit/keyshift/internal/policy"
	"github.com/Huy-8bit/keyshift/internal/rediscompat"
)

// fakeBackend is an in-memory Backend with a simulated tracked cache.
type fakeBackend struct {
	mu        sync.Mutex
	data      map[string][]byte
	cache     map[string][]byte
	hooks     rediscompat.Hooks
	caching   bool
	trackErr  atomic.Pointer[error]
	cachedErr atomic.Pointer[error]
	directErr atomic.Pointer[error]
	batches   atomic.Int32
	flushes   atomic.Int32
	hits      atomic.Uint64
	misses    atomic.Uint64
	direct    atomic.Uint64
	closed    atomic.Bool
}

func newFake(h rediscompat.Hooks) *fakeBackend {
	return &fakeBackend{data: map[string][]byte{}, cache: map[string][]byte{}, hooks: h, caching: true}
}

func (f *fakeBackend) GetDirect(_ context.Context, k string) (string, error) {
	f.direct.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.data[k]
	if !ok {
		return "", rediscompat.ErrNil
	}
	return string(v), nil
}

func (f *fakeBackend) GetDirectMulti(ctx context.Context, keys []string) []rediscompat.Result {
	f.batches.Add(1)
	out := make([]rediscompat.Result, len(keys))
	for i, k := range keys {
		if p := f.directErr.Load(); p != nil {
			out[i].Err = *p
			continue
		}
		out[i].Value, out[i].Err = f.GetDirect(ctx, k)
	}
	return out
}

func (f *fakeBackend) GetCached(ctx context.Context, k string) (string, error) {
	if p := f.cachedErr.Load(); p != nil {
		return "", *p
	}
	f.mu.Lock()
	if v, ok := f.cache[k]; ok {
		f.mu.Unlock()
		f.hits.Add(1)
		return string(v), nil
	}
	f.mu.Unlock()
	f.misses.Add(1)
	f.hooks.OnFill(k)
	v, err := f.GetDirect(ctx, k)
	if err == nil {
		f.mu.Lock()
		f.cache[k] = []byte(v)
		f.mu.Unlock()
	}
	return v, err
}

// externalWrite simulates another client writing, with server invalidation.
func (f *fakeBackend) externalWrite(k, v string) {
	f.mu.Lock()
	f.data[k] = []byte(v)
	_, cached := f.cache[k]
	delete(f.cache, k)
	f.mu.Unlock()
	if cached {
		f.hooks.OnInvalidate(k)
	}
}

func (f *fakeBackend) Set(_ context.Context, k string, v []byte, _ time.Duration) error {
	f.mu.Lock()
	f.data[k] = v
	delete(f.cache, k)
	f.mu.Unlock()
	return nil
}

func (f *fakeBackend) Del(_ context.Context, keys ...string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var n int64
	for _, k := range keys {
		if _, ok := f.data[k]; ok {
			n++
		}
		delete(f.data, k)
		delete(f.cache, k)
	}
	return n, nil
}

func (f *fakeBackend) Forget(k string) { f.mu.Lock(); delete(f.cache, k); f.mu.Unlock() }
func (f *fakeBackend) FlushLocal() int {
	f.flushes.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	n := len(f.cache)
	clear(f.cache)
	return n
}
func (f *fakeBackend) CacheStats() rediscompat.CacheStats {
	f.mu.Lock()
	defer f.mu.Unlock()
	return rediscompat.CacheStats{Hits: f.hits.Load(), Misses: f.misses.Load(), Entries: int64(len(f.cache)), ResidencyKnown: true}
}
func (f *fakeBackend) Capabilities() rediscompat.Capabilities {
	c := rediscompat.Capabilities{Driver: "fake", Topology: rediscompat.TopologyStandalone, Caching: f.caching, TrackingMode: "fake"}
	if !f.caching {
		c.Reason = "fake: unsupported"
	}
	return c
}
func (f *fakeBackend) CheckTracking(context.Context) error {
	if p := f.trackErr.Load(); p != nil {
		return *p
	}
	return nil
}
func (f *fakeBackend) Ping(context.Context) error { return nil }
func (f *fakeBackend) Close() error               { f.closed.Store(true); return nil }

func testConfig() Config {
	return Config{
		ServiceName: "test-svc", InstanceID: "pod-1",
		Detection: DetectionConfig{Window: time.Hour, TopK: 20, MaxTrackingCandidates: 100},
		Tracking:  TrackingConfig{HealthInterval: time.Hour},
		Cache:     CacheConfig{MaxStaleness: 10 * time.Second},
	}
}

func newTestClient(t *testing.T, cfg Config) (*Client, *fakeBackend) {
	t.Helper()
	var fb *fakeBackend
	cfg.applyDefaults()
	c, err := newClient(cfg, func(h rediscompat.Hooks, _ rediscompat.CacheLimits) (rediscompat.Backend, error) {
		fb = newFake(h)
		return fb, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, fb
}

func rotateAt(c *Client, now time.Time) { c.rotate(now) }

func TestConfigValidation(t *testing.T) {
	cases := []Config{
		{ServiceName: ""},
		{ServiceName: "bad name"},
		{ServiceName: "ok", Detection: DetectionConfig{SampleRate: 2}},
		{ServiceName: "ok", Cache: CacheConfig{MinHitRatio: 0.5, ExitHitRatio: 0.9}},
		{ServiceName: "ok", Cache: CacheConfig{DenyPatterns: []string{""}}},
		{ServiceName: "ok", Telemetry: TelemetryConfig{MaxCandidates: 500}},
		{ServiceName: "ok", Cache: CacheConfig{MaxStaleness: time.Millisecond}},
	}
	for i, cfg := range cases {
		cfg.applyDefaults()
		if err := cfg.validate(); err == nil {
			t.Errorf("case %d: expected validation error", i)
		}
	}
	if _, err := New(Config{ServiceName: "x"}); err == nil {
		t.Fatal("New without address must fail")
	}
}

func TestAdaptiveAdmissionEndToEnd(t *testing.T) {
	c, fb := newTestClient(t, testConfig())
	ctx := context.Background()
	fb.data["hot"] = []byte("v")
	fb.data["cold"] = []byte("v")
	now := time.Now()
	for range 2 {
		for range 500 {
			c.Get(ctx, "hot")
		}
		c.Get(ctx, "cold")
		now = now.Add(10 * time.Second)
		rotateAt(c, now)
	}
	if d := c.Explain("hot"); d.State != "admitted" {
		t.Fatalf("hot not admitted: %+v", d)
	}
	if d := c.Explain("cold"); d.State == "admitted" {
		t.Fatalf("cold admitted: %+v", d)
	}
	before := fb.direct.Load()
	for range 1000 {
		if v, _ := c.Get(ctx, "hot"); v != "v" {
			t.Fatal("wrong value")
		}
	}
	if extra := fb.direct.Load() - before; extra != 1 {
		t.Fatalf("expected exactly 1 Redis read (the fill), got %d", extra)
	}
	s := c.Stats()
	if s.LocalHits != 999 || s.CachedReads != 1000 || s.AdmittedKeys != 1 || s.RedisReadsAvoided != 999 {
		t.Fatalf("stats %+v", s)
	}
	// External write: invalidation reaches the cache and the engine.
	fb.externalWrite("hot", "v2")
	if v, _ := c.Get(ctx, "hot"); v != "v2" {
		t.Fatalf("stale read after invalidation: %q", v)
	}
	// Local write: read-your-writes.
	if err := c.SetString(ctx, "hot", "v3", 0); err != nil {
		t.Fatal(err)
	}
	if v, _ := c.Get(ctx, "hot"); v != "v3" {
		t.Fatalf("read-your-write violated: %q", v)
	}
	// Bypass always reaches Redis.
	before = fb.direct.Load()
	if v, _ := c.GetFresh(ctx, "hot"); v != "v3" || fb.direct.Load() != before+1 {
		t.Fatal("GetFresh did not bypass the cache")
	}
	if n, _ := c.Del(ctx, "hot"); n != 1 {
		t.Fatal("Del")
	}
	if _, err := c.Get(ctx, "hot"); !errors.Is(err, Nil) {
		t.Fatalf("expected Nil after Del, got %v", err)
	}
}

func TestTrackingUnhealthySuspendsCachingAndFlushes(t *testing.T) {
	c, fb := newTestClient(t, testConfig())
	ctx := context.Background()
	fb.data["k"] = []byte("v")
	now := time.Now()
	for range 2 {
		for range 500 {
			c.Get(ctx, "k")
		}
		now = now.Add(10 * time.Second)
		rotateAt(c, now)
	}
	c.Get(ctx, "k")
	if c.engine.Lookup("k") == nil {
		t.Fatal("precondition: k admitted")
	}
	boom := errors.New("tracking probe failed")
	fb.trackErr.Store(&boom)
	c.monitor.CheckNow(ctx)
	if c.Stats().Tracking.State != "unhealthy" || c.engine.Lookup("k") != nil {
		t.Fatal("unhealthy tracking must stop cache reads")
	}
	if fb.flushes.Load() == 0 || len(fb.cache) != 0 {
		t.Fatal("local cache must be flushed when tracking is lost")
	}
	before := fb.direct.Load()
	c.Get(ctx, "k")
	if fb.direct.Load() != before+1 {
		t.Fatal("read must go to Redis while unhealthy")
	}
	fb.trackErr.Store(nil)
	c.monitor.CheckNow(ctx)
	c.monitor.CheckNow(ctx)
	if c.Stats().Tracking.State != "healthy" {
		t.Fatal("tracking should recover")
	}
	for range 500 {
		c.Get(ctx, "k")
	}
	now = now.Add(10 * time.Second)
	rotateAt(c, now)
	if c.engine.Lookup("k") == nil {
		t.Fatalf("k should be re-admitted after recovery: %+v", c.Explain("k"))
	}
}

func TestTransportErrorFallsBackAndMarksUnhealthy(t *testing.T) {
	c, fb := newTestClient(t, testConfig())
	ctx := context.Background()
	fb.data["k"] = []byte("v")
	now := time.Now()
	for range 2 {
		for range 500 {
			c.Get(ctx, "k")
		}
		now = now.Add(10 * time.Second)
		rotateAt(c, now)
	}
	var netErr error = &net.OpError{Op: "read", Err: errors.New("connection reset by peer")}
	fb.cachedErr.Store(&netErr)
	v, err := c.Get(ctx, "k")
	if err != nil || v != "v" {
		t.Fatalf("fallback read failed: %q %v", v, err)
	}
	if c.Stats().CacheFallbacks != 1 || c.Stats().Tracking.State != "unhealthy" {
		t.Fatalf("stats %+v", c.Stats())
	}
}

func TestRedisErrorOnCachePathIsReturnedWithoutRetry(t *testing.T) {
	c, fb := newTestClient(t, testConfig())
	ctx := context.Background()
	fb.data["k"] = []byte("v")
	now := time.Now()
	for range 2 {
		for range 500 {
			c.Get(ctx, "k")
		}
		now = now.Add(10 * time.Second)
		rotateAt(c, now)
	}
	wrongType := errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	fb.cachedErr.Store(&wrongType)
	before := fb.direct.Load()
	if _, err := c.Get(ctx, "k"); err == nil || !strings.Contains(err.Error(), "WRONGTYPE") {
		t.Fatalf("expected the Redis error, got %v", err)
	}
	if fb.direct.Load() != before || c.Stats().CacheFallbacks != 0 || c.Stats().Tracking.State != "healthy" {
		t.Fatalf("Redis errors must not trigger fallback or mark tracking unhealthy: %+v", c.Stats())
	}
}

func TestAbortedCachedFetchFallsBackToDirect(t *testing.T) {
	if !isTransportError(fmt.Errorf("wrapped: %w", rueidis.ErrDoCacheAborted)) {
		t.Fatal("aborted DoCache must fall back to a direct read")
	}
	if isTransportError(errors.New("WRONGTYPE Operation against a key")) {
		t.Fatal("Redis reply errors must not fall back")
	}
}

func TestUnsupportedCachingStillServesReads(t *testing.T) {
	cfg := testConfig()
	cfg.applyDefaults()
	c, err := newClient(cfg, func(h rediscompat.Hooks, _ rediscompat.CacheLimits) (rediscompat.Backend, error) {
		fb := newFake(h)
		fb.caching = false
		fb.data["k"] = []byte("v")
		return fb, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	now := time.Now()
	for range 3 {
		for range 500 {
			if v, _ := c.Get(context.Background(), "k"); v != "v" {
				t.Fatal("read failed")
			}
		}
		now = now.Add(10 * time.Second)
		rotateAt(c, now)
	}
	s := c.Stats()
	if s.Tracking.State != "unsupported" || s.AdmittedKeys != 0 || s.Tracking.Reason == "" {
		t.Fatalf("stats %+v", s)
	}
	if d := c.Explain("k"); d.State != "disabled" || !strings.Contains(d.Reason, "fake: unsupported") {
		t.Fatalf("decision %+v", d)
	}
}

func TestRemotePolicyLifecycle(t *testing.T) {
	c, fb := newTestClient(t, testConfig())
	ctx := context.Background()
	fb.data["balance:1"] = []byte("100")
	now := time.Now()
	for range 2 {
		for range 500 {
			c.Get(ctx, "balance:1")
		}
		now = now.Add(10 * time.Second)
		rotateAt(c, now)
	}
	if c.engine.Lookup("balance:1") == nil {
		t.Fatal("precondition")
	}
	c.lastContact.Store(time.Now().UnixNano())
	env := &policy.Envelope{RolloutPercent: 100, Current: &policy.Policy{Version: 5, Mode: policy.ModeAdaptive,
		DenyPatterns: []string{"balance:*"}, ValidForSeconds: 60}}
	if err := c.applyEnvelope(env); err != nil {
		t.Fatal(err)
	}
	if c.engine.Lookup("balance:1") != nil {
		t.Fatal("deny policy not applied immediately")
	}
	if s := c.Stats(); s.PolicyVersion != 5 || s.PolicySource != "remote" {
		t.Fatalf("stats %+v", s)
	}
	// Invalid policy is rejected; previous remains.
	bad := &policy.Envelope{RolloutPercent: 100, Current: &policy.Policy{Version: 6, Mode: "bogus"}}
	if err := c.applyEnvelope(bad); err == nil || c.Stats().PolicyVersion != 5 || c.Stats().PolicyRejected != 1 {
		t.Fatal("invalid policy must be rejected and last valid retained")
	}
	// Policy for another service is rejected.
	other := &policy.Envelope{RolloutPercent: 100, Current: &policy.Policy{Version: 7, Service: "other", Mode: policy.ModeOff}}
	if err := c.applyEnvelope(other); err == nil {
		t.Fatal("foreign service policy accepted")
	}
	// The controller reporting "no policy" (e.g. after deletion) reverts at once.
	c.revertToLocalPolicy()
	if s := c.Stats(); s.PolicySource != "local" {
		t.Fatalf("no-policy response did not revert: %+v", s)
	}
	if err := c.applyEnvelope(env); err != nil || c.Stats().PolicyVersion != 5 {
		t.Fatal("re-applying the policy failed")
	}
	// Staleness: no contact for longer than ValidFor reverts to local.
	c.lastContact.Store(time.Now().Add(-2 * time.Minute).UnixNano())
	c.checkPolicyStaleness(time.Now())
	if s := c.Stats(); s.PolicySource != "local" || s.PolicyVersion != 0 {
		t.Fatalf("stale policy not reverted: %+v", s)
	}
	// Programmatic policy overrides remote ones.
	if err := c.ApplyPolicy(&Policy{Version: 42, Mode: Off}); err != nil {
		t.Fatal(err)
	}
	_ = c.applyEnvelope(env)
	if c.Stats().PolicyVersion != 42 {
		t.Fatal("manual policy overridden by remote")
	}
	c.ClearPolicy()
	if c.Stats().PolicySource != "local" {
		t.Fatal("ClearPolicy")
	}
	if err := c.ApplyPolicy(&Policy{Mode: "x"}); err == nil {
		t.Fatal("invalid manual policy accepted")
	}
}

func TestRolloutSelectsPreviousForExcludedInstances(t *testing.T) {
	cfg := testConfig()
	// Find an instance ID outside a 10% rollout.
	for i := 0; ; i++ {
		id := "pod-" + strconv.Itoa(i)
		if policy.RolloutBucket(id) >= 10 {
			cfg.InstanceID = id
			break
		}
	}
	c, _ := newTestClient(t, cfg)
	env := &policy.Envelope{RolloutPercent: 10,
		Current:  &policy.Policy{Version: 2, Mode: policy.ModeOff},
		Previous: &policy.Policy{Version: 1, Mode: policy.ModeAdaptive}}
	if err := c.applyEnvelope(env); err != nil {
		t.Fatal(err)
	}
	if v := c.Stats().PolicyVersion; v != 1 {
		t.Fatalf("instance outside rollout should run previous policy, got v%d", v)
	}
}

func TestTelemetryBatchContents(t *testing.T) {
	cfg := testConfig()
	cfg.Telemetry.FingerprintSecret = []byte("0123456789abcdef-secret")
	cfg.Telemetry.DisplayKeyPatterns = []string{"public:*"}
	c, fb := newTestClient(t, cfg)
	ctx := context.Background()
	fb.data["public:1"] = []byte("v")
	fb.data["secret:1"] = []byte("v")
	if c.buildBatch() != nil {
		t.Fatal("no window closed yet: batch must be nil")
	}
	now := time.Now()
	for range 3 {
		for range 300 {
			c.Get(ctx, "public:1")
			c.Get(ctx, "secret:1")
		}
		c.SetString(ctx, "w", "x", 0)
		now = now.Add(10 * time.Second)
		rotateAt(c, now)
	}
	b := c.buildBatch()
	if err := b.Validate(time.Now().Add(time.Minute)); err != nil {
		t.Fatalf("batch invalid: %v", err)
	}
	if b.Seq != 1 || b.FingerprintScope != "shared" || b.Totals.Reads != 1800 || b.Totals.Writes != 3 {
		t.Fatalf("batch %+v", b.Totals)
	}
	if b.Totals.LocalHits == 0 || b.Totals.RedisReads+b.Totals.LocalHits != b.Totals.Reads {
		t.Fatalf("read accounting inconsistent: %+v", b.Totals)
	}
	// Reads after the last rotation must not leak into the batch.
	for range 100 {
		c.Get(ctx, "public:1")
	}
	now = now.Add(10 * time.Second)
	rotateAt(c, now)
	b2 := c.buildBatch()
	if b2.Totals.Reads != 100 || b2.Totals.LocalHits+b2.Totals.RedisReads != 100 {
		t.Fatalf("second batch totals inconsistent: %+v", b2.Totals)
	}
	for range 50 {
		c.Get(ctx, "public:1") // not yet rotated
	}
	if b3 := c.buildBatch(); b3 != nil {
		t.Fatalf("no closed window: expected nil batch, got %+v", b3.Totals)
	}
	found := map[string]bool{}
	for _, cand := range b.Candidates {
		if strings.Contains(cand.Display, "secret") {
			t.Fatal("non-display key leaked")
		}
		if cand.Display != "" {
			found[cand.Display] = true
		}
		if cand.FP == c.Fingerprint("secret:1") {
			found["secret-fp"] = true
		}
	}
	if !found["public:1"] || !found["secret-fp"] {
		t.Fatalf("candidates %+v", b.Candidates)
	}

}

func TestMetricsCollectorHasNoKeyLabels(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	cfg := testConfig()
	cfg.Metrics.Registerer = reg
	c, fb := newTestClient(t, cfg)
	fb.data["user:secret-key"] = []byte("v")
	for range 10 {
		c.Get(context.Background(), "user:secret-key")
	}
	n, err := testutil.GatherAndCount(reg)
	if err != nil {
		t.Fatal(err)
	}
	if n < 20 {
		t.Fatalf("expected metrics, got %d", n)
	}
	mfs, _ := reg.Gather()
	for _, mf := range mfs {
		for _, m := range mf.Metric {
			for _, lp := range m.Label {
				if strings.Contains(lp.GetValue(), "secret-key") {
					t.Fatalf("key leaked into label %s", mf.GetName())
				}
			}
		}
	}
	c.Close()
	if n, _ := testutil.GatherAndCount(reg); n != 0 {
		t.Fatalf("collector not unregistered on Close: %d metrics", n)
	}
}

func TestCloseStopsGoroutinesAndRejectsCalls(t *testing.T) {
	before := runtime.NumGoroutine()
	cfg := testConfig()
	cfg.Detection.Window = 10 * time.Millisecond
	cfg.Tracking.HealthInterval = 5 * time.Millisecond
	cfg.applyDefaults()
	var fb *fakeBackend
	c, err := newClient(cfg, func(h rediscompat.Hooks, _ rediscompat.CacheLimits) (rediscompat.Backend, error) {
		fb = newFake(h)
		return fb, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 2000 {
				c.Get(context.Background(), "k")
			}
		})
	}
	time.Sleep(30 * time.Millisecond)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if !fb.closed.Load() {
		t.Fatal("backend not closed")
	}
	if _, err := c.Get(context.Background(), "k"); !errors.Is(err, ErrClosed) {
		t.Fatal("expected ErrClosed")
	}
	if c.Close() != nil {
		t.Fatal("Close not idempotent")
	}
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if g := runtime.NumGoroutine(); g > before {
		buf := make([]byte, 1<<16)
		n := runtime.Stack(buf, true)
		t.Fatalf("goroutine leak: before=%d after=%d\n%s", before, g, buf[:n])
	}
}

func TestConcurrentUseWithPolicyUpdates(t *testing.T) {
	cfg := testConfig()
	c, fb := newTestClient(t, cfg)
	for i := range 20 {
		fb.data["k"+strconv.Itoa(i)] = []byte("v")
	}
	ctx := context.Background()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				k := "k" + strconv.Itoa((i*7+g)%20)
				switch i % 50 {
				case 0:
					c.SetString(ctx, k, "v", 0)
				case 1:
					c.GetFresh(ctx, k)
				default:
					if _, err := c.Get(ctx, k); err != nil {
						t.Error(err)
						return
					}
				}
			}
		})
	}
	now := time.Now()
	for i := range 100 {
		now = now.Add(10 * time.Second)
		rotateAt(c, now)
		mode := policy.ModeAdaptive
		if i%10 == 0 {
			mode = policy.ModeOff
		}
		c.lastContact.Store(time.Now().UnixNano())
		_ = c.applyEnvelope(&policy.Envelope{RolloutPercent: 100, Current: &policy.Policy{Version: int64(i + 1), Mode: mode}})
		_ = c.Stats()
		_ = c.Explain("k1")
		if i%25 == 0 {
			c.buildBatch()
		}
	}
	close(stop)
	wg.Wait()
}
