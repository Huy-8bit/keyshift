//go:build integration

package rediscompat

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/rueidis"

	"github.com/Huy-8bit/keyshift/internal/testutil"
)

func newRueidis(t *testing.T, addrs []string, cluster bool, rec *recorder) *Rueidis {
	t.Helper()
	opt := OptInTracking(rueidis.ClientOption{InitAddress: addrs, CacheSizeEachConn: 16 << 20, ClientName: "keyshift-rueidis"})
	if !cluster {
		opt.ForceSingleClient = true
	}
	r, err := NewRueidis(context.Background(), RueidisOptions{ClientOption: &opt, Cluster: cluster,
		Limits: CacheLimits{MaxEntries: 1000, MaxMemoryBytes: 16 << 20, MaxStaleness: 30 * time.Second}, Hooks: rec.hooks()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	if caps := r.Capabilities(); !caps.Caching || caps.TrackingMode != "optin" {
		t.Fatalf("unexpected caps %+v", caps)
	}
	return r
}

func TestRueidisMissHitInvalidate(t *testing.T) {
	addr := testutil.RedisAddr(t)
	rec := newRecorder()
	r := newRueidis(t, []string{addr}, false, rec)
	ext := testutil.Plain(t, addr)
	ctx := context.Background()
	k := testutil.Key(t, "k")
	ext.Set(ctx, k, "v1", 0)
	for range 3 {
		if v, err := r.GetCached(ctx, k); err != nil || v != "v1" {
			t.Fatalf("got %q %v", v, err)
		}
	}
	if st := r.CacheStats(); st.Hits != 2 || st.Misses != 1 || st.Entries != 1 || rec.get(rec.fills, k) != 1 {
		t.Fatalf("stats %+v fills %v", st, rec.fills)
	}
	ext.Set(ctx, k, "v2", 0)
	testutil.Eventually(t, 2*time.Second, func() bool {
		v, _ := r.GetCached(ctx, k)
		return v == "v2"
	}, "external SET not invalidated")
	if rec.get(rec.invals, k) == 0 {
		t.Fatal("invalidation hook not called with the application key")
	}
	ext.Del(ctx, k)
	testutil.Eventually(t, 2*time.Second, func() bool {
		_, err := r.GetCached(ctx, k)
		return errors.Is(err, ErrNil)
	}, "external DEL not invalidated")
}

func TestRueidisBudgetEvicts(t *testing.T) {
	addr := testutil.RedisAddr(t)
	opt := rueidis.ClientOption{InitAddress: []string{addr}, ForceSingleClient: true}
	r, err := NewRueidis(context.Background(), RueidisOptions{ClientOption: &opt,
		Limits: CacheLimits{MaxEntries: 10, MaxMemoryBytes: 1 << 20, MaxStaleness: time.Minute}})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	ext := testutil.Plain(t, addr)
	ctx := context.Background()
	for i := range 50 {
		k := testutil.Key(t, strconv.Itoa(i))
		ext.Set(ctx, k, "v", 0)
		r.GetCached(ctx, k)
	}
	st := r.CacheStats()
	if st.Entries > 10 || st.Evictions < 40 {
		t.Fatalf("budget not enforced: %+v", st)
	}
	if n := r.FlushLocal(); n == 0 || r.CacheStats().Entries != 0 {
		t.Fatalf("flush failed: %d %+v", n, r.CacheStats())
	}
}

func TestRueidisOptInTracksOnlyCachedKeys(t *testing.T) {
	addr := testutil.RedisAddr(t)
	ext := testutil.Plain(t, addr)
	ctx := context.Background()
	ext.FlushAll(ctx)
	r := newRueidis(t, []string{addr}, false, newRecorder())
	before := testutil.InfoInt(t, ext, "stats", "tracking_total_keys")
	for i := range 100 {
		k := testutil.Key(t, "direct"+strconv.Itoa(i))
		ext.Set(ctx, k, "x", 0)
		if _, err := r.GetDirect(ctx, k); err != nil {
			t.Fatal(err)
		}
	}
	k := testutil.Key(t, "cached")
	ext.Set(ctx, k, "x", 0)
	r.GetCached(ctx, k)
	if d := testutil.InfoInt(t, ext, "stats", "tracking_total_keys") - before; d != 1 {
		t.Fatalf("OPTIN should track exactly 1 key, got %d", d)
	}
}

func TestRueidisLocalWriteThenRead(t *testing.T) {
	addr := testutil.RedisAddr(t)
	r := newRueidis(t, []string{addr}, false, newRecorder())
	ctx := context.Background()
	k := testutil.Key(t, "k")
	for i := range 200 {
		want := "v" + strconv.Itoa(i)
		if err := r.Set(ctx, k, []byte(want), 0); err != nil {
			t.Fatal(err)
		}
		if v, err := r.GetCached(ctx, k); err != nil || v != want {
			t.Fatalf("iteration %d: got %q want %q (%v)", i, v, want, err)
		}
	}
}

func TestRueidisConnectionLoss(t *testing.T) {
	addr := testutil.RedisAddr(t)
	r := newRueidis(t, []string{addr}, false, newRecorder())
	ext := testutil.Plain(t, addr)
	ctx := context.Background()
	k := testutil.Key(t, "k")
	ext.Set(ctx, k, "old", 0)
	r.GetCached(ctx, k)
	if testutil.KillClientsByName(t, ext, "keyshift-rueidis") == 0 {
		t.Fatal("no rueidis connections found")
	}
	ext.Set(ctx, k, "new", 0)
	start := time.Now()
	var staleFor time.Duration
	for time.Since(start) < 300*time.Millisecond {
		if v, err := r.GetCached(ctx, k); err == nil && v == "old" {
			staleFor = time.Since(start)
		}
		time.Sleep(200 * time.Microsecond)
	}
	t.Logf("stale reads observed for %s after connection loss", staleFor)
	if staleFor > 100*time.Millisecond {
		t.Fatalf("stale value served for %s", staleFor)
	}
}

func TestRueidisClusterCaching(t *testing.T) {
	addrs := testutil.ClusterAddrs(t)
	rec := newRecorder()
	r := newRueidis(t, addrs, true, rec)
	ext := redis.NewClusterClient(&redis.ClusterOptions{Addrs: addrs})
	defer ext.Close()
	ctx := context.Background()
	// Keys spread across slots and therefore across nodes.
	for i := range 30 {
		k := testutil.Key(t, "k"+strconv.Itoa(i))
		if err := ext.Set(ctx, k, "v1", 0).Err(); err != nil {
			t.Fatal(err)
		}
		r.GetCached(ctx, k)
		if v, err := r.GetCached(ctx, k); err != nil || v != "v1" {
			t.Fatalf("got %q %v", v, err)
		}
		ext.Set(ctx, k, "v2", 0)
		testutil.Eventually(t, 2*time.Second, func() bool {
			v, _ := r.GetCached(ctx, k)
			return v == "v2"
		}, "cluster invalidation for %s", k)
	}
	if st := r.CacheStats(); st.Hits < 30 {
		t.Fatalf("expected cache hits across cluster nodes, got %+v", st)
	}
}
