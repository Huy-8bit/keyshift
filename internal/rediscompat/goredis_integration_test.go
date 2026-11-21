//go:build integration

package rediscompat

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Huy-8bit/keyshift/internal/testutil"
)

type recorder struct {
	mu     sync.Mutex
	fills  map[string]int
	invals map[string]int
}

func newRecorder() *recorder { return &recorder{fills: map[string]int{}, invals: map[string]int{}} }

func (r *recorder) hooks() Hooks {
	return Hooks{
		OnFill:       func(k string) { r.mu.Lock(); r.fills[k]++; r.mu.Unlock() },
		OnInvalidate: func(k string) { r.mu.Lock(); r.invals[k]++; r.mu.Unlock() },
	}
}

func (r *recorder) get(m map[string]int, k string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return m[k]
}

func newGoRedis(t *testing.T, addr string, rec *recorder) *GoRedis {
	t.Helper()
	direct := redis.NewClient(&redis.Options{Addr: addr})
	g, err := NewGoRedis(context.Background(), GoRedisOptions{
		Direct: direct, OwnsDirect: true,
		Limits: CacheLimits{MaxEntries: 1000, MaxMemoryBytes: 16 << 20, MaxStaleness: 30 * time.Second},
		Hooks:  rec.hooks(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	caps := g.Capabilities()
	if !caps.Caching {
		t.Fatalf("caching unavailable: %+v", caps)
	}
	return g
}

func TestGoRedisMissThenHitAndKeyNamespace(t *testing.T) {
	addr := testutil.RedisAddr(t)
	rec := newRecorder()
	g := newGoRedis(t, addr, rec)
	ext := testutil.Plain(t, addr)
	ctx := context.Background()
	k := testutil.Key(t, "k")
	if err := ext.Set(ctx, k, "v1", 0).Err(); err != nil {
		t.Fatal(err)
	}
	for range 5 {
		v, err := g.GetCached(ctx, k)
		if err != nil || string(v) != "v1" {
			t.Fatalf("got %q %v", v, err)
		}
	}
	st := g.CacheStats()
	if st.Misses != 1 || st.Hits != 4 || st.Entries != 1 {
		t.Fatalf("stats %+v", st)
	}
	// The hook must receive the application key, proving the namespace
	// prefix mirrors go-redis's internal layout for this version.
	if rec.get(rec.fills, k) != 1 {
		t.Fatalf("fill hook not called with the application key: %v", rec.fills)
	}
	g.Forget(k)
	if st := g.CacheStats(); st.Entries != 0 {
		t.Fatalf("Forget did not drop the local entry: %+v", st)
	}
}

func TestGoRedisTrackedClientHasOwnPushProcessor(t *testing.T) {
	addr := testutil.RedisAddr(t)
	g := newGoRedis(t, addr, newRecorder())
	direct := g.Direct().(*redis.Client)
	if direct.Options().PushNotificationProcessor == g.Tracked().Options().PushNotificationProcessor {
		t.Fatal("tracked client shares the application client's push processor")
	}
	if g.Tracked().Options().Protocol != 3 || g.Tracked().Options().ClientSideCache == nil {
		t.Fatal("tracked client not configured for RESP3 client-side caching")
	}
}

func TestGoRedisExternalWriteAndDeleteInvalidate(t *testing.T) {
	addr := testutil.RedisAddr(t)
	rec := newRecorder()
	g := newGoRedis(t, addr, rec)
	ext := testutil.Plain(t, addr)
	ctx := context.Background()
	k := testutil.Key(t, "k")
	ext.Set(ctx, k, "v1", 0)
	if v, _ := g.GetCached(ctx, k); string(v) != "v1" {
		t.Fatalf("got %q", v)
	}
	ext.Set(ctx, k, "v2", 0)
	testutil.Eventually(t, 2*time.Second, func() bool {
		v, _ := g.GetCached(ctx, k)
		return string(v) == "v2"
	}, "external SET not invalidated")
	if rec.get(rec.invals, k) == 0 {
		t.Fatal("invalidation hook not called")
	}
	ext.Del(ctx, k)
	testutil.Eventually(t, 2*time.Second, func() bool {
		_, err := g.GetCached(ctx, k)
		return errors.Is(err, ErrNil)
	}, "external DEL not invalidated")
}

func TestGoRedisExpiration(t *testing.T) {
	addr := testutil.RedisAddr(t)
	g := newGoRedis(t, addr, newRecorder())
	ext := testutil.Plain(t, addr)
	ctx := context.Background()
	k := testutil.Key(t, "k")
	ext.Set(ctx, k, "v", 300*time.Millisecond)
	if v, _ := g.GetCached(ctx, k); string(v) != "v" {
		t.Fatal("initial read failed")
	}
	// Redis sends an invalidation when the key is expired (active expiry
	// cycle or lazy expiry).
	testutil.Eventually(t, 3*time.Second, func() bool {
		_, err := g.GetCached(ctx, k)
		return errors.Is(err, ErrNil)
	}, "expired key still served from local cache")
}

func TestGoRedisLocalWriteThenRead(t *testing.T) {
	addr := testutil.RedisAddr(t)
	g := newGoRedis(t, addr, newRecorder())
	ctx := context.Background()
	k := testutil.Key(t, "k")
	for i := range 200 {
		want := "v" + strconv.Itoa(i)
		if err := g.Set(ctx, k, []byte(want), 0); err != nil {
			t.Fatal(err)
		}
		v, err := g.GetCached(ctx, k)
		if err != nil || string(v) != want {
			t.Fatalf("iteration %d: read-your-write violated: got %q want %q (%v)", i, v, want, err)
		}
	}
	if _, err := g.Del(ctx, k); err != nil {
		t.Fatal(err)
	}
	if _, err := g.GetCached(ctx, k); !errors.Is(err, ErrNil) {
		t.Fatalf("read after local DEL: %v", err)
	}
}

// TestGoRedisServerTracksOnlyCachedKeys verifies the core overhead property:
// keys read through the direct client do not enter the server's tracking
// table.
func TestGoRedisServerTracksOnlyCachedKeys(t *testing.T) {
	addr := testutil.RedisAddr(t)
	ext := testutil.Plain(t, addr)
	ctx := context.Background()
	ext.FlushAll(ctx) // tracking table is cleared by FLUSHALL invalidation
	g := newGoRedis(t, addr, newRecorder())
	before := testutil.InfoInt(t, ext, "stats", "tracking_total_keys")
	for i := range 200 {
		k := testutil.Key(t, "direct"+strconv.Itoa(i))
		ext.Set(ctx, k, "x", 0)
		if _, err := g.GetDirect(ctx, k); err != nil {
			t.Fatal(err)
		}
	}
	cached := testutil.Key(t, "cached")
	ext.Set(ctx, cached, "x", 0)
	if _, err := g.GetCached(ctx, cached); err != nil {
		t.Fatal(err)
	}
	after := testutil.InfoInt(t, ext, "stats", "tracking_total_keys")
	if after-before != 1 {
		t.Fatalf("expected exactly 1 tracked key, tracking_total_keys went %d -> %d", before, after)
	}
}

// TestGoRedisConnectionLossEvictsEntries kills the tracked connections while
// a value is cached, writes a new value while they are down, and requires
// that the stale value is never served afterwards.
func TestGoRedisConnectionLossEvictsEntries(t *testing.T) {
	addr := testutil.RedisAddr(t)
	g := newGoRedis(t, addr, newRecorder())
	ext := testutil.Plain(t, addr)
	ctx := context.Background()
	k := testutil.Key(t, "k")
	ext.Set(ctx, k, "old", 0)
	if v, _ := g.GetCached(ctx, k); string(v) != "old" {
		t.Fatal("prime failed")
	}
	if n := testutil.KillClientsByName(t, ext, "keyshift-tracked"); n == 0 {
		t.Fatal("no tracked connections found to kill")
	}
	// The killed connection lost its tracking state, so this write produces
	// no invalidation for it. The client learns about the closed socket
	// asynchronously (go-redis's background drainer sweeps idle tracked
	// connections every few milliseconds), so a short window of stale reads
	// is expected. It must close quickly and never reopen.
	ext.Set(ctx, k, "new", 0)
	start := time.Now()
	var staleFor time.Duration
	for time.Since(start) < 500*time.Millisecond {
		v, err := g.GetCached(ctx, k)
		if err == nil && string(v) == "old" {
			staleFor = time.Since(start)
		}
		time.Sleep(200 * time.Microsecond)
	}
	t.Logf("stale reads observed for %s after connection loss", staleFor)
	if staleFor > 100*time.Millisecond {
		t.Fatalf("stale value served for %s after tracking connection loss", staleFor)
	}
	if err := g.CheckTracking(ctx); err != nil {
		t.Fatalf("tracking did not recover: %v", err)
	}
}

func TestGoRedisConcurrentReadersAndWriters(t *testing.T) {
	addr := testutil.RedisAddr(t)
	g := newGoRedis(t, addr, newRecorder())
	ext := testutil.Plain(t, addr)
	ctx := context.Background()
	k := testutil.Key(t, "k")
	ext.Set(ctx, k, "0", 0)
	var latest atomicVersion
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for i := 1; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			ext.Set(ctx, k, strconv.Itoa(i), 0)
			latest.set(i)
			time.Sleep(time.Millisecond)
		}
	})
	var maxLag int
	var mu sync.Mutex
	for range 8 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				floor := latest.get() // value committed before this read started
				v, err := g.GetCached(ctx, k)
				if err != nil {
					t.Error(err)
					return
				}
				n, _ := strconv.Atoi(string(v))
				if lag := floor - n; lag > 0 {
					mu.Lock()
					maxLag = max(maxLag, lag)
					mu.Unlock()
				}
			}
		})
	}
	time.Sleep(time.Second)
	close(stop)
	wg.Wait()
	// Eventual consistency: stale reads are possible only for a short
	// invalidation delay. With ~1ms between writes, a lag of more than a few
	// hundred versions would indicate a lost invalidation.
	t.Logf("max observed version lag: %d", maxLag)
	if maxLag > 200 {
		t.Fatalf("version lag %d suggests lost invalidations", maxLag)
	}
	// After writes stop, the cache must converge to the final value.
	final := strconv.Itoa(latest.get())
	testutil.Eventually(t, 2*time.Second, func() bool {
		v, _ := g.GetCached(ctx, k)
		return string(v) == final
	}, "cache did not converge to final value %s", final)
}

type atomicVersion struct {
	mu sync.Mutex
	v  int
}

func (a *atomicVersion) set(v int) { a.mu.Lock(); a.v = v; a.mu.Unlock() }
func (a *atomicVersion) get() int  { a.mu.Lock(); defer a.mu.Unlock(); return a.v }

func TestGoRedisFlushAllInvalidatesEverything(t *testing.T) {
	addr := testutil.RedisAddr(t)
	g := newGoRedis(t, addr, newRecorder())
	ext := testutil.Plain(t, addr)
	ctx := context.Background()
	k := testutil.Key(t, "k")
	ext.Set(ctx, k, "v", 0)
	g.GetCached(ctx, k)
	ext.FlushAll(ctx)
	testutil.Eventually(t, 2*time.Second, func() bool {
		_, err := g.GetCached(ctx, k)
		return errors.Is(err, ErrNil)
	}, "FLUSHALL did not invalidate")
}

func TestGoRedisClusterClientReportsUnsupported(t *testing.T) {
	addrs := testutil.ClusterAddrs(t)
	cc := redis.NewClusterClient(&redis.ClusterOptions{Addrs: addrs})
	g, err := NewGoRedis(context.Background(), GoRedisOptions{Direct: cc, OwnsDirect: true,
		Limits: CacheLimits{MaxEntries: 10, MaxStaleness: time.Second}})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	caps := g.Capabilities()
	if caps.Caching || caps.Topology != TopologyCluster || caps.Reason == "" {
		t.Fatalf("unexpected caps %+v", caps)
	}
	// Cached reads degrade to direct reads.
	k := testutil.Key(t, "k")
	if err := g.Set(context.Background(), k, []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	if v, err := g.GetCached(context.Background(), k); err != nil || string(v) != "v" {
		t.Fatalf("got %q %v", v, err)
	}
}

func TestGoRedisNonZeroDBUnsupported(t *testing.T) {
	addr := testutil.RedisAddr(t)
	g, err := NewGoRedis(context.Background(), GoRedisOptions{
		Direct: redis.NewClient(&redis.Options{Addr: addr, DB: 1}), OwnsDirect: true,
		Limits: CacheLimits{MaxEntries: 10, MaxStaleness: time.Second}})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	if g.Capabilities().Caching {
		t.Fatal("DB 1 must not enable caching")
	}
}
