//go:build integration

package hotkeys

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Huy-8bit/keyshift/internal/testutil"
)

func TestRunCollectsRedisSideHotKeys(t *testing.T) {
	addr := testutil.RedisAddr(t)
	c := redis.NewClient(&redis.Options{Addr: addr})
	defer c.Close()
	ctx := context.Background()
	cap := Detect(ctx, c)
	if !cap.Supported || !cap.Permitted {
		t.Skipf("HOTKEYS not available on %s: %+v", addr, cap)
	}
	c.Do(ctx, "HOTKEYS", "RESET")
	hot := testutil.Key(t, "hot")
	c.Set(ctx, hot, string(make([]byte, 4096)), 0)
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
				c.Get(ctx, hot)
			}
		}
	})
	res, err := Run(ctx, c, Options{Duration: time.Second, Count: 10})
	close(stop)
	wg.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if res.Source != "redis-hotkeys" || len(res.ByNet) == 0 || res.ByNet[0].Key != hot {
		t.Fatalf("unexpected result %+v", res)
	}
	// The session was reset: a new one can start immediately.
	if err := c.HotKeysStart(ctx, &redis.HotKeysStartArgs{Metrics: []redis.HotKeysMetric{redis.HotKeysMetricCPU}, Duration: 1}).Err(); err != nil {
		t.Fatalf("session not cleaned up: %v", err)
	}
	c.HotKeysStop(ctx)
	c.HotKeysReset(ctx)
}

func TestConflictingSessionIsNotTouched(t *testing.T) {
	addr := testutil.RedisAddr(t)
	c := redis.NewClient(&redis.Options{Addr: addr})
	defer c.Close()
	ctx := context.Background()
	if cap := Detect(ctx, c); !cap.Supported {
		t.Skip("HOTKEYS unsupported")
	}
	c.Do(ctx, "HOTKEYS", "RESET")
	if err := c.HotKeysStart(ctx, &redis.HotKeysStartArgs{Metrics: []redis.HotKeysMetric{redis.HotKeysMetricCPU}, Duration: 30}).Err(); err != nil {
		t.Fatal(err)
	}
	defer func() { c.HotKeysStop(ctx); c.HotKeysReset(ctx) }()
	_, err := Run(ctx, c, Options{Duration: time.Second})
	if !errors.Is(err, ErrSessionActive) {
		t.Fatalf("expected ErrSessionActive, got %v", err)
	}
	r, err := c.HotKeysGet(ctx).Result()
	if err != nil || !r.TrackingActive {
		t.Fatalf("foreign session was stopped: %v %+v", err, r)
	}
}

func TestCancelStopsOwnSession(t *testing.T) {
	addr := testutil.RedisAddr(t)
	c := redis.NewClient(&redis.Options{Addr: addr})
	defer c.Close()
	if cap := Detect(context.Background(), c); !cap.Supported {
		t.Skip("HOTKEYS unsupported")
	}
	c.Do(context.Background(), "HOTKEYS", "RESET")
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, err := Run(ctx, c, Options{Duration: time.Minute}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline, got %v", err)
	}
	if err := c.HotKeysStart(context.Background(), &redis.HotKeysStartArgs{Metrics: []redis.HotKeysMetric{redis.HotKeysMetricCPU}, Duration: 1}).Err(); err != nil {
		t.Fatalf("cancelled session not cleaned up: %v", err)
	}
	c.HotKeysStop(context.Background())
	c.HotKeysReset(context.Background())
}

func TestUnsupportedServer(t *testing.T) {
	addr := os.Getenv("KEYSHIFT_TEST_REDIS_OLD_ADDR")
	if addr == "" {
		t.Skip("KEYSHIFT_TEST_REDIS_OLD_ADDR not set")
	}
	c := redis.NewClient(&redis.Options{Addr: addr})
	defer c.Close()
	cap := Detect(context.Background(), c)
	if cap.Supported {
		t.Fatalf("expected unsupported: %+v", cap)
	}
	if _, err := Run(context.Background(), c, Options{Duration: time.Second}); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("expected ErrUnsupported, got %v", err)
	}
}

func TestNotPermitted(t *testing.T) {
	addr := testutil.RedisAddr(t)
	admin := redis.NewClient(&redis.Options{Addr: addr})
	defer admin.Close()
	ctx := context.Background()
	if cap := Detect(ctx, admin); !cap.Supported {
		t.Skip("HOTKEYS unsupported")
	}
	user := fmt.Sprintf("ks_nohotkeys_%d", time.Now().UnixNano())
	admin.Do(ctx, "ACL", "SETUSER", user, "on", ">pw-123456", "~*", "+@read", "+info", "+command", "+hello", "+client")
	defer admin.Do(ctx, "ACL", "DELUSER", user)
	c := redis.NewClient(&redis.Options{Addr: addr, Username: user, Password: "pw-123456"})
	defer c.Close()
	cap := Detect(ctx, c)
	if !cap.Supported || cap.Permitted {
		t.Fatalf("expected supported but not permitted: %+v", cap)
	}
	if _, err := Run(ctx, c, Options{Duration: time.Second}); !errors.Is(err, ErrNotPermitted) {
		t.Fatalf("expected ErrNotPermitted, got %v", err)
	}
}
