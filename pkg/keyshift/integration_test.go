//go:build integration

package keyshift_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/rueidis"

	"github.com/Huy-8bit/keyshift/internal/testutil"
	"github.com/Huy-8bit/keyshift/pkg/keyshift"
)

func cfg(name string) keyshift.Config {
	return keyshift.Config{
		ServiceName: name,
		Detection:   keyshift.DetectionConfig{Window: time.Hour, TopK: 50, MaxTrackingCandidates: 500},
		Cache:       keyshift.CacheConfig{MaxStaleness: 30 * time.Second},
		Tracking:    keyshift.TrackingConfig{HealthInterval: 100 * time.Millisecond, HealthTimeout: 100 * time.Millisecond},
	}
}

// admit drives enough reads through two windows to admit key.
func admit(t *testing.T, c *keyshift.Client, key string) {
	t.Helper()
	ctx := context.Background()
	for range 2 {
		for range 300 {
			if _, err := c.Get(ctx, key); err != nil && !errors.Is(err, keyshift.Nil) {
				t.Fatal(err)
			}
		}
		// RotateNow uses wall time; the elapsed window is short, so the
		// measured read rate is high.
		c.RotateNow()
	}
	if d := c.Explain(key); d.State != "admitted" {
		t.Fatalf("%s not admitted: %+v", key, d)
	}
}

type driverCase struct {
	name string
	mk   func(t *testing.T, addr string, c keyshift.Config) *keyshift.Client
}

var drivers = []driverCase{
	{"go-redis", func(t *testing.T, addr string, c keyshift.Config) *keyshift.Client {
		c.Redis = keyshift.RedisConfig{Addr: addr}
		cl, err := keyshift.New(c)
		if err != nil {
			t.Fatal(err)
		}
		return cl
	}},
	{"rueidis", func(t *testing.T, addr string, c keyshift.Config) *keyshift.Client {
		c.Redis = keyshift.RedisConfig{Addr: addr, Driver: keyshift.DriverRueidis}
		cl, err := keyshift.New(c)
		if err != nil {
			t.Fatal(err)
		}
		return cl
	}},
}

func TestSDKEndToEnd(t *testing.T) {
	addr := testutil.RedisAddr(t)
	for _, d := range drivers {
		t.Run(d.name, func(t *testing.T) {
			c := d.mk(t, addr, cfg("e2e"))
			defer c.Close()
			ext := testutil.Plain(t, addr)
			ctx := context.Background()
			k := testutil.Key(t, "hot")
			ext.Set(ctx, k, "v1", 0)
			admit(t, c, k)

			before := c.Stats()
			for range 1000 {
				if v, err := c.Get(ctx, k); err != nil || v != "v1" {
					t.Fatalf("got %q %v", v, err)
				}
			}
			after := c.Stats()
			if hits := after.LocalHits - before.LocalHits; hits < 999 {
				t.Fatalf("expected local hits, got %d (stats %+v)", hits, after)
			}
			// External SET and DEL invalidate.
			ext.Set(ctx, k, "v2", 0)
			testutil.Eventually(t, 2*time.Second, func() bool { v, _ := c.Get(ctx, k); return v == "v2" }, "SET invalidation")
			ext.Del(ctx, k)
			testutil.Eventually(t, 2*time.Second, func() bool { _, err := c.Get(ctx, k); return errors.Is(err, keyshift.Nil) }, "DEL invalidation")
			// Expiry.
			ext.Set(ctx, k, "short", 200*time.Millisecond)
			testutil.Eventually(t, 2*time.Second, func() bool { v, _ := c.Get(ctx, k); return v == "short" }, "re-read")
			testutil.Eventually(t, 3*time.Second, func() bool { _, err := c.Get(ctx, k); return errors.Is(err, keyshift.Nil) }, "expiry")
			// Local write then read.
			for i := range 50 {
				want := "local" + strconv.Itoa(i)
				if err := c.SetString(ctx, k, want, 0); err != nil {
					t.Fatal(err)
				}
				if v, _ := c.Get(ctx, k); v != want {
					t.Fatalf("read-your-write: got %q want %q", v, want)
				}
			}
			// Bypass.
			if v, err := c.GetFresh(ctx, k); err != nil || v != "local49" {
				t.Fatalf("GetFresh: %q %v", v, err)
			}
			if st := c.Stats(); st.Tracking.State != "healthy" || !st.Capabilities.Caching {
				t.Fatalf("status %+v", st)
			}
		})
	}
}

// TestManyClientsConverge runs several SDK instances (like pods) caching the
// same hot key while an external writer updates it.
func TestManyClientsConverge(t *testing.T) {
	addr := testutil.RedisAddr(t)
	ext := testutil.Plain(t, addr)
	ctx := context.Background()
	k := testutil.Key(t, "shared")
	ext.Set(ctx, k, "0", 0)
	var clients []*keyshift.Client
	for i := range 6 {
		c := drivers[i%2].mk(t, addr, cfg("pods"))
		defer c.Close()
		admit(t, c, k)
		clients = append(clients, c)
	}
	for v := 1; v <= 20; v++ {
		ext.Set(ctx, k, strconv.Itoa(v), 0)
		want := strconv.Itoa(v)
		for i, c := range clients {
			testutil.Eventually(t, 2*time.Second, func() bool { got, _ := c.Get(ctx, k); return got == want },
				"client %d did not converge to %s", i, want)
		}
	}
}

// TestSilentPartitionStopsStaleReads blackholes the connection between the
// SDK and Redis without closing it. Redis client libraries cannot detect
// this; KeyShift's tracking probe must time out, suspend caching, and flush.
func TestSilentPartitionStopsStaleReads(t *testing.T) {
	addr := testutil.RedisAddr(t)
	for _, d := range drivers {
		t.Run(d.name, func(t *testing.T) {
			proxy := testutil.NewProxy(t, addr)
			conf := cfg("partition")
			conf.Redis.ReadTimeout = 300 * time.Millisecond
			conf.Redis.WriteTimeout = 300 * time.Millisecond
			c := d.mk(t, proxy.Addr(), conf)
			defer c.Close()
			ext := testutil.Plain(t, addr)
			ctx := context.Background()
			k := testutil.Key(t, "k")
			ext.Set(ctx, k, "old", 0)
			admit(t, c, k)
			c.Get(ctx, k)

			proxy.Blackhole(true)
			ext.Set(ctx, k, "new", 0) // invalidation cannot reach the SDK
			start := time.Now()
			var lastStale time.Duration
			for time.Since(start) < 1500*time.Millisecond {
				rctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
				v, err := c.Get(rctx, k)
				cancel()
				if err == nil && v == "old" {
					lastStale = time.Since(start)
				}
				time.Sleep(5 * time.Millisecond)
			}
			st := c.Stats()
			t.Logf("stale reads served for %s after partition; tracking=%s", lastStale, st.Tracking.State)
			if st.Tracking.State != "unhealthy" {
				t.Fatalf("partition not detected: %+v", st.Tracking)
			}
			// Probe interval (100ms, jittered) + probe timeout (100ms) + slack.
			if lastStale > 600*time.Millisecond {
				t.Fatalf("stale value served for %s, expected bounded by health probe", lastStale)
			}
			proxy.Blackhole(false)
			healed := time.Now()
			testutil.Eventually(t, 20*time.Second, func() bool {
				v, err := c.Get(ctx, k)
				return err == nil && v == "new"
			}, "did not recover after partition healed")
			testutil.Eventually(t, 20*time.Second, func() bool { return c.Stats().Tracking.State == "healthy" }, "tracking recovery")
			t.Logf("reads and tracking recovered %s after the partition healed", time.Since(healed).Round(time.Millisecond))
		})
	}
}

// TestConnectionCutRecovers closes every connection (similar to a Redis
// restart from the client's point of view) while values are cached.
func TestConnectionCutRecovers(t *testing.T) {
	addr := testutil.RedisAddr(t)
	for _, d := range drivers {
		t.Run(d.name, func(t *testing.T) {
			proxy := testutil.NewProxy(t, addr)
			c := d.mk(t, proxy.Addr(), cfg("cut"))
			defer c.Close()
			ext := testutil.Plain(t, addr)
			ctx := context.Background()
			k := testutil.Key(t, "k")
			ext.Set(ctx, k, "old", 0)
			admit(t, c, k)
			proxy.CutAll()
			ext.Set(ctx, k, "new", 0)
			start := time.Now()
			var lastStale time.Duration
			for time.Since(start) < 500*time.Millisecond {
				if v, err := c.Get(ctx, k); err == nil && v == "old" {
					lastStale = time.Since(start)
				}
				time.Sleep(time.Millisecond)
			}
			t.Logf("stale window after connection cut: %s", lastStale)
			if lastStale > 100*time.Millisecond {
				t.Fatalf("stale value served for %s after connection loss", lastStale)
			}
			testutil.Eventually(t, 3*time.Second, func() bool { v, _ := c.Get(ctx, k); return v == "new" }, "recovery")
		})
	}
}

func TestControllerOutageDoesNotAffectRedisAccess(t *testing.T) {
	addr := testutil.RedisAddr(t)
	// A controller that hangs, then one that is unreachable.
	hang := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(5 * time.Second)
	}))
	defer hang.Close()
	for _, ep := range []string{hang.URL, "http://127.0.0.1:1"} {
		conf := cfg("ctl-outage")
		conf.Redis.Addr = addr
		conf.Telemetry = keyshift.TelemetryConfig{Endpoint: ep, Interval: 50 * time.Millisecond}
		start := time.Now()
		c, err := keyshift.New(conf)
		if err != nil {
			t.Fatal(err)
		}
		if el := time.Since(start); el > 4*time.Second {
			t.Fatalf("startup blocked by controller for %s", el)
		}
		ctx := context.Background()
		k := testutil.Key(t, "k")
		if err := c.SetString(ctx, k, "v", 0); err != nil {
			t.Fatal(err)
		}
		admit(t, c, k)
		for range 1000 {
			if v, err := c.Get(ctx, k); err != nil || v != "v" {
				t.Fatalf("Get failed during controller outage: %v", err)
			}
		}
		if st := c.Stats(); st.PolicySource != "local" {
			t.Fatalf("expected local policy, got %+v", st)
		}
		closeStart := time.Now()
		c.Close()
		if el := time.Since(closeStart); el > 2*time.Second {
			t.Fatalf("Close blocked for %s", el)
		}
	}
}

func TestACLWithoutClientCommandsDegradesGracefully(t *testing.T) {
	addr := testutil.RedisAddr(t)
	admin := testutil.Plain(t, addr)
	ctx := context.Background()
	user := fmt.Sprintf("ks_limited_%d", time.Now().UnixNano())
	if err := admin.Do(ctx, "ACL", "SETUSER", user, "on", ">pw-123456", "~*", "+@read", "+@write", "+@connection", "-client").Err(); err != nil {
		t.Fatal(err)
	}
	defer admin.Do(ctx, "ACL", "DELUSER", user)
	for _, d := range drivers {
		t.Run(d.name, func(t *testing.T) {
			conf := cfg("acl")
			conf.Redis = keyshift.RedisConfig{Addr: addr, Username: user, Password: "pw-123456"}
			if d.name == "rueidis" {
				conf.Redis.Driver = keyshift.DriverRueidis
			}
			c, err := keyshift.New(conf)
			if err != nil {
				t.Skipf("driver cannot connect without CLIENT permissions: %v", err)
			}
			defer c.Close()
			k := testutil.Key(t, "k")
			if err := c.SetString(ctx, k, "v", 0); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				for range 300 {
					if v, err := c.Get(ctx, k); err != nil || v != "v" {
						t.Fatalf("Get: %q %v", v, err)
					}
				}
				c.RotateNow()
			}
			st := c.Stats()
			t.Logf("caps=%+v tracking=%+v", st.Capabilities, st.Tracking)
			if st.Capabilities.Caching || st.AdmittedKeys != 0 {
				t.Fatalf("caching must be disabled without CLIENT TRACKING permission: %+v", st)
			}
		})
	}
}

// TestDocumentedMinimumACL verifies the ACL documented in
// docs/compatibility.md is sufficient for local caching with both drivers.
func TestDocumentedMinimumACL(t *testing.T) {
	addr := testutil.RedisAddr(t)
	admin := testutil.Plain(t, addr)
	ctx := context.Background()
	user := fmt.Sprintf("ks_min_%d", time.Now().UnixNano())
	rules := []any{"ACL", "SETUSER", user, "on", ">pw-123456", "~kstest:*", "-@all",
		"+get", "+set", "+del", "+ping", "+info", "+hello",
		"+client|tracking", "+client|trackinginfo", "+client|setname", "+client|setinfo", "+client|caching"}
	if err := admin.Do(ctx, rules...).Err(); err != nil {
		t.Fatal(err)
	}
	defer admin.Do(ctx, "ACL", "DELUSER", user)
	for _, d := range drivers {
		t.Run(d.name, func(t *testing.T) {
			conf := cfg("acl-min")
			conf.Redis = keyshift.RedisConfig{Addr: addr, Username: user, Password: "pw-123456"}
			if d.name == "rueidis" {
				conf.Redis.Driver = keyshift.DriverRueidis
			}
			c, err := keyshift.New(conf)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			k := testutil.Key(t, "k")
			if err := c.SetString(ctx, k, "v1", 0); err != nil {
				t.Fatal(err)
			}
			admit(t, c, k)
			if st := c.Stats(); !st.Capabilities.Caching || st.Tracking.State != "healthy" {
				t.Fatalf("caching should work with the documented ACL: %+v %+v", st.Capabilities, st.Tracking)
			}
			admin.Set(ctx, k, "v2", 0)
			testutil.Eventually(t, 2*time.Second, func() bool { v, _ := c.Get(ctx, k); return v == "v2" }, "invalidation under minimal ACL")
		})
	}
}

func TestNewFromGoRedisDoesNotOwnClient(t *testing.T) {
	addr := testutil.RedisAddr(t)
	rc := redis.NewClient(&redis.Options{Addr: addr})
	defer rc.Close()
	c, err := keyshift.NewFromGoRedis(rc, cfg("wrap"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	k := testutil.Key(t, "k")
	c.SetString(ctx, k, "v", 0)
	admit(t, c, k)
	c.Close()
	if err := rc.Ping(ctx).Err(); err != nil {
		t.Fatalf("application client closed by KeyShift: %v", err)
	}
	if c.GoRedis() != rc {
		t.Fatal("GoRedis() should return the wrapped client")
	}
}

func TestRueidisClusterSDK(t *testing.T) {
	addrs := testutil.ClusterAddrs(t)
	c, err := keyshift.NewFromRueidis(rueidis.ClientOption{InitAddress: addrs}, cfg("cluster"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ext := redis.NewClusterClient(&redis.ClusterOptions{Addrs: addrs})
	defer ext.Close()
	ctx := context.Background()
	keys := make([]string, 10)
	for i := range keys {
		keys[i] = testutil.Key(t, strconv.Itoa(i))
		ext.Set(ctx, keys[i], "v1", 0)
	}
	for range 2 {
		for range 100 {
			for _, k := range keys {
				c.Get(ctx, k)
			}
		}
		c.RotateNow()
	}
	if st := c.Stats(); st.AdmittedKeys != len(keys) || st.Capabilities.Topology != "cluster" {
		t.Fatalf("stats %+v", st)
	}
	for _, k := range keys {
		ext.Set(ctx, k, "v2", 0)
		testutil.Eventually(t, 2*time.Second, func() bool { v, _ := c.Get(ctx, k); return v == "v2" }, "cluster invalidation %s", k)
	}
}

func TestGoRedisClusterSDKDetectsWithoutCaching(t *testing.T) {
	addrs := testutil.ClusterAddrs(t)
	conf := cfg("gocluster")
	conf.Redis = keyshift.RedisConfig{Addrs: addrs, Cluster: true}
	c, err := keyshift.New(conf)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	k := testutil.Key(t, "k")
	c.SetString(ctx, k, "v", 0)
	for range 2 {
		for range 300 {
			c.Get(ctx, k)
		}
		c.RotateNow()
	}
	st := c.Stats()
	if st.Capabilities.Caching || st.Tracking.State != "unsupported" || st.LastWindow.Reads == 0 {
		t.Fatalf("stats %+v", st)
	}
	if d := c.Explain(k); d.ReadRate == 0 {
		t.Fatalf("detection should still work: %+v", d)
	}
}

// TestConcurrentReadersWritersStaleness measures staleness under concurrent
// writes through the SDK and from an external client.
func TestConcurrentReadersWritersStaleness(t *testing.T) {
	addr := testutil.RedisAddr(t)
	for _, d := range drivers {
		t.Run(d.name, func(t *testing.T) {
			c := d.mk(t, addr, cfg("concurrency"))
			defer c.Close()
			ext := testutil.Plain(t, addr)
			ctx := context.Background()
			k := testutil.Key(t, "k")
			ext.Set(ctx, k, "0", 0)
			admit(t, c, k)
			var latest atomic.Int64
			stop := make(chan struct{})
			var wg sync.WaitGroup
			wg.Go(func() {
				for i := int64(1); ; i++ {
					select {
					case <-stop:
						return
					default:
					}
					var err error
					if i%2 == 0 {
						err = ext.Set(ctx, k, strconv.FormatInt(i, 10), 0).Err()
					} else {
						err = c.SetString(ctx, k, strconv.FormatInt(i, 10), 0)
					}
					if err != nil {
						t.Error(err)
						return
					}
					latest.Store(i)
					time.Sleep(500 * time.Microsecond)
				}
			})
			var maxLag atomic.Int64
			for range 8 {
				wg.Go(func() {
					for {
						select {
						case <-stop:
							return
						default:
						}
						floor := latest.Load()
						v, err := c.Get(ctx, k)
						if err != nil {
							t.Error(err)
							return
						}
						n, _ := strconv.ParseInt(v, 10, 64)
						if lag := floor - n; lag > maxLag.Load() {
							maxLag.Store(lag)
						}
					}
				})
			}
			time.Sleep(time.Second)
			close(stop)
			wg.Wait()
			t.Logf("max version lag: %d", maxLag.Load())
			if maxLag.Load() > 200 {
				t.Fatalf("lag %d indicates lost invalidations", maxLag.Load())
			}
			final := strconv.FormatInt(latest.Load(), 10)
			testutil.Eventually(t, 2*time.Second, func() bool { v, _ := c.Get(ctx, k); return v == final }, "convergence")
		})
	}
}

func TestCloseWhileOperationsActive(t *testing.T) {
	addr := testutil.RedisAddr(t)
	c := drivers[0].mk(t, addr, cfg("close"))
	ctx := context.Background()
	k := testutil.Key(t, "k")
	c.SetString(ctx, k, "v", 0)
	admit(t, c, k)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 5000 {
				_, err := c.Get(ctx, k)
				if err != nil && !errors.Is(err, keyshift.ErrClosed) && !errors.Is(err, redis.ErrClosed) {
					t.Errorf("unexpected error during close: %v", err)
					return
				}
			}
		})
	}
	time.Sleep(5 * time.Millisecond)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
}
