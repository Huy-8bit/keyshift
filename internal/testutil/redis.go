// Package testutil contains helpers shared by KeyShift tests.
package testutil

import (
	"context"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisAddr returns the standalone Redis address for integration tests or
// skips the test when KEYSHIFT_TEST_REDIS_ADDR is unset.
func RedisAddr(t testing.TB) string {
	t.Helper()
	addr := os.Getenv("KEYSHIFT_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("KEYSHIFT_TEST_REDIS_ADDR not set; run `make test-integration`")
	}
	return addr
}

// ClusterAddrs returns Redis Cluster seed addresses or skips the test.
func ClusterAddrs(t testing.TB) []string {
	t.Helper()
	v := os.Getenv("KEYSHIFT_TEST_CLUSTER_ADDRS")
	if v == "" {
		t.Skip("KEYSHIFT_TEST_CLUSTER_ADDRS not set; run `make test-integration`")
	}
	return strings.Split(v, ",")
}

var keySeq atomic.Uint64

// Key returns a key unique to the running test.
func Key(t testing.TB, suffix string) string {
	return "kstest:" + t.Name() + ":" + time.Now().Format("150405.000000") + ":" + itoa(keySeq.Add(1)) + ":" + suffix
}

func itoa(v uint64) string {
	if v == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	return string(b[i:])
}

// Plain returns an untracked RESP2 client used to simulate external writers.
func Plain(t testing.TB, addr string) *redis.Client {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: addr, Protocol: 2})
	if err := c.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("redis at %s unavailable: %v", addr, err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// Eventually polls cond until it returns true or timeout elapses.
func Eventually(t testing.TB, timeout time.Duration, cond func() bool, msg string, args ...any) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s: "+msg, append([]any{timeout}, args...)...)
}

// InfoInt reads an integer INFO field.
func InfoInt(t testing.TB, c redis.UniversalClient, section, field string) int64 {
	t.Helper()
	info, err := c.Info(context.Background(), section).Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(info, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), field+":"); ok {
			var n int64
			for _, ch := range v {
				if ch < '0' || ch > '9' {
					break
				}
				n = n*10 + int64(ch-'0')
			}
			return n
		}
	}
	t.Fatalf("INFO %s has no field %s", section, field)
	return 0
}

// KillClientsByName closes every server connection whose CLIENT SETNAME is
// name, simulating a network interruption for those connections.
func KillClientsByName(t testing.TB, admin *redis.Client, name string) int {
	t.Helper()
	ctx := context.Background()
	list, err := admin.ClientList(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	killed := 0
	for _, line := range strings.Split(list, "\n") {
		if !strings.Contains(line, " name="+name+" ") {
			continue
		}
		for _, f := range strings.Fields(line) {
			if id, ok := strings.CutPrefix(f, "id="); ok {
				if err := admin.Do(ctx, "CLIENT", "KILL", "ID", id).Err(); err == nil {
					killed++
				}
			}
		}
	}
	return killed
}
