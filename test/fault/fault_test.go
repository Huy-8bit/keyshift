//go:build integration

// Package fault contains fault-injection tests that manipulate real Redis
// processes (restart, cluster slot migration). They require Docker-managed
// Redis instances; see the Makefile target test-integration.
package fault

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/rueidis"

	"github.com/Huy-8bit/keyshift/internal/testutil"
	"github.com/Huy-8bit/keyshift/pkg/keyshift"
)

func baseConfig(name string) keyshift.Config {
	return keyshift.Config{
		ServiceName: name,
		Detection:   keyshift.DetectionConfig{Window: time.Hour},
		Cache:       keyshift.CacheConfig{MaxStaleness: 30 * time.Second},
		Tracking:    keyshift.TrackingConfig{HealthInterval: 100 * time.Millisecond, HealthTimeout: 200 * time.Millisecond},
	}
}

func admit(t *testing.T, c *keyshift.Client, key string) {
	t.Helper()
	ctx := context.Background()
	for range 2 {
		for range 300 {
			c.Get(ctx, key)
		}
		c.RotateNow()
	}
	if d := c.Explain(key); d.State != "admitted" {
		t.Fatalf("not admitted: %+v", d)
	}
}

// TestRedisRestart restarts a real Redis process (without persistence) while
// values are cached. After the restart the key no longer exists; a cached
// value must never be served.
func TestRedisRestart(t *testing.T) {
	container := os.Getenv("KEYSHIFT_TEST_RESTART_CONTAINER")
	addr := os.Getenv("KEYSHIFT_TEST_RESTART_ADDR")
	if container == "" || addr == "" {
		t.Skip("KEYSHIFT_TEST_RESTART_CONTAINER/ADDR not set")
	}
	for _, driver := range []keyshift.Driver{keyshift.DriverGoRedis, keyshift.DriverRueidis} {
		t.Run(string(driver), func(t *testing.T) {
			conf := baseConfig("restart")
			conf.Redis = keyshift.RedisConfig{Addr: addr, Driver: driver}
			c, err := keyshift.New(conf)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			ctx := context.Background()
			k := testutil.Key(t, "k")
			if err := c.SetString(ctx, k, "before-restart", 0); err != nil {
				t.Fatal(err)
			}
			admit(t, c, k)
			if out, err := exec.Command("docker", "restart", "-t", "0", container).CombinedOutput(); err != nil {
				t.Fatalf("docker restart: %v %s", err, out)
			}
			restarted := time.Now()
			// During and after the restart: errors are acceptable (normal
			// Redis semantics), the old cached value is not.
			var errorsSeen int
			for time.Since(restarted) < 3*time.Second {
				rctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
				v, err := c.Get(rctx, k)
				cancel()
				switch {
				case err == nil && v == "before-restart":
					t.Fatalf("served pre-restart value %s after restart", time.Since(restarted))
				case err != nil && !errors.Is(err, keyshift.Nil):
					errorsSeen++
				}
				time.Sleep(10 * time.Millisecond)
			}
			t.Logf("errors returned while Redis restarted: %d", errorsSeen)
			if _, err := c.Get(ctx, k); !errors.Is(err, keyshift.Nil) {
				t.Fatalf("expected Nil after restart without persistence, got %v", err)
			}
			testutil.Eventually(t, 10*time.Second, func() bool { return c.Stats().Tracking.State == "healthy" }, "tracking recovery")
			// Caching works again after recovery.
			if err := c.SetString(ctx, k, "after", 0); err != nil {
				t.Fatal(err)
			}
			admit(t, c, k)
			before := c.Stats().LocalHits
			for range 100 {
				c.Get(ctx, k)
			}
			if c.Stats().LocalHits-before < 99 {
				t.Fatal("cache not used after recovery")
			}
		})
	}
}

// TestClusterSlotMigration moves the slot of a cached key to another node
// while it is cached (MOVED redirect) and verifies invalidation and
// convergence on the new owner.
func TestClusterSlotMigration(t *testing.T) {
	addrs := testutil.ClusterAddrs(t)
	ctx := context.Background()
	admin := redis.NewClusterClient(&redis.ClusterOptions{Addrs: addrs})
	defer admin.Close()
	c, err := keyshift.NewFromRueidis(rueidis.ClientOption{InitAddress: addrs}, baseConfig("migration"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	k := testutil.Key(t, "migrating")
	if err := admin.Set(ctx, k, "v1", 0).Err(); err != nil {
		t.Fatal(err)
	}
	admit(t, c, k)

	slot, err := admin.ClusterKeySlot(ctx, k).Result()
	if err != nil {
		t.Fatal(err)
	}
	nodes := clusterNodes(t, admin)
	var src, dst node
	for _, n := range nodes {
		if n.owns(int(slot)) {
			src = n
		}
	}
	for _, n := range nodes {
		if n.id != src.id {
			dst = n
			break
		}
	}
	if src.id == "" || dst.id == "" {
		t.Fatalf("could not determine source/target nodes: %+v", nodes)
	}
	srcC := redis.NewClient(&redis.Options{Addr: src.addr})
	dstC := redis.NewClient(&redis.Options{Addr: dst.addr})
	defer srcC.Close()
	defer dstC.Close()
	s := strconv.FormatInt(slot, 10)
	host, port, _ := strings.Cut(dst.addr, ":")
	steps := [][]any{
		{dstC, []any{"CLUSTER", "SETSLOT", s, "IMPORTING", src.id}},
		{srcC, []any{"CLUSTER", "SETSLOT", s, "MIGRATING", dst.id}},
		{srcC, []any{"MIGRATE", host, port, "", 0, 5000, "KEYS", k}},
	}
	for _, st := range steps {
		if err := st[0].(*redis.Client).Do(ctx, st[1].([]any)...).Err(); err != nil {
			t.Fatalf("%v: %v", st[1], err)
		}
	}
	for _, n := range nodes {
		nc := redis.NewClient(&redis.Options{Addr: n.addr})
		if err := nc.Do(ctx, "CLUSTER", "SETSLOT", s, "NODE", dst.id).Err(); err != nil {
			t.Fatalf("setslot node on %s: %v", n.addr, err)
		}
		nc.Close()
	}
	// Restore the original slot owner when done so the cluster layout is stable.
	t.Cleanup(func() {
		migrateBack(ctx, t, nodes, dst, src, s, k)
	})

	// The value moved, not changed; reads follow MOVED and stay correct.
	testutil.Eventually(t, 5*time.Second, func() bool { v, err := c.Get(ctx, k); return err == nil && v == "v1" }, "read after MOVED")
	// A write on the new owner must invalidate the entry cached from it.
	admit(t, c, k)
	if err := dstC.Set(ctx, k, "v2", 0).Err(); err != nil {
		t.Fatal(err)
	}
	testutil.Eventually(t, 3*time.Second, func() bool { v, _ := c.Get(ctx, k); return v == "v2" }, "invalidation on new owner")
}

type node struct {
	id, addr string
	slots    [][2]int
}

func (n node) owns(slot int) bool {
	for _, r := range n.slots {
		if slot >= r[0] && slot <= r[1] {
			return true
		}
	}
	return false
}

func clusterNodes(t *testing.T, admin *redis.ClusterClient) []node {
	t.Helper()
	out, err := admin.ClusterNodes(context.Background()).Result()
	if err != nil {
		t.Fatal(err)
	}
	var nodes []node
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		f := strings.Fields(line)
		if len(f) < 8 || !strings.Contains(f[2], "master") {
			continue
		}
		n := node{id: f[0], addr: strings.Split(f[1], "@")[0]}
		for _, r := range f[8:] {
			if strings.HasPrefix(r, "[") {
				continue
			}
			a, b, ok := strings.Cut(r, "-")
			lo, _ := strconv.Atoi(a)
			hi := lo
			if ok {
				hi, _ = strconv.Atoi(b)
			}
			n.slots = append(n.slots, [2]int{lo, hi})
		}
		nodes = append(nodes, n)
	}
	return nodes
}

func migrateBack(ctx context.Context, t *testing.T, nodes []node, from, to node, slot, key string) {
	fromC := redis.NewClient(&redis.Options{Addr: from.addr})
	toC := redis.NewClient(&redis.Options{Addr: to.addr})
	defer fromC.Close()
	defer toC.Close()
	host, port, _ := strings.Cut(to.addr, ":")
	toC.Do(ctx, "CLUSTER", "SETSLOT", slot, "IMPORTING", from.id)
	fromC.Do(ctx, "CLUSTER", "SETSLOT", slot, "MIGRATING", to.id)
	fromC.Do(ctx, "MIGRATE", host, port, "", 0, 5000, "KEYS", key)
	for _, n := range nodes {
		nc := redis.NewClient(&redis.Options{Addr: n.addr})
		if err := nc.Do(ctx, "CLUSTER", "SETSLOT", slot, "NODE", to.id).Err(); err != nil {
			t.Logf("restore slot on %s: %v", n.addr, err)
		}
		nc.Close()
	}
}
