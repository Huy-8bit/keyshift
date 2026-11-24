//go:build integration

package rediscompat

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/redis/go-redis/v9"

	"github.com/Huy-8bit/keyshift/internal/testutil"
)

// checkGetDirectMulti writes keys spread over many hash slots plus a hash,
// then verifies values, missing keys, per-key errors, and result order.
func checkGetDirectMulti(t *testing.T, b Backend, writer redis.UniversalClient) {
	t.Helper()
	ctx := context.Background()
	var keys []string
	for i := range 20 {
		k := testutil.Key(t, "m"+strconv.Itoa(i))
		if err := writer.Set(ctx, k, "v"+strconv.Itoa(i), 0).Err(); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, k)
	}
	missing := testutil.Key(t, "missing")
	keys = append(keys, missing)

	res := b.GetDirectMulti(ctx, keys)
	if len(res) != len(keys) {
		t.Fatalf("got %d results for %d keys", len(res), len(keys))
	}
	for i := range 20 {
		if res[i].Err != nil || res[i].Value != "v"+strconv.Itoa(i) {
			t.Fatalf("key %d: %+v", i, res[i])
		}
	}
	if !errors.Is(res[20].Err, ErrNil) {
		t.Fatalf("missing key: %+v", res[20])
	}

	hash := testutil.Key(t, "hash")
	if err := writer.HSet(ctx, hash, "f", "v").Err(); err != nil {
		t.Fatal(err)
	}
	res = b.GetDirectMulti(ctx, []string{keys[0], hash})
	if res[0].Err != nil || res[1].Err == nil || errors.Is(res[1].Err, ErrNil) {
		t.Fatalf("expected WRONGTYPE for the hash only: %+v", res)
	}
}

func TestGetDirectMultiGoRedis(t *testing.T) {
	addr := testutil.RedisAddr(t)
	g := newGoRedis(t, addr, newRecorder())
	checkGetDirectMulti(t, g, g.Direct())
}

func TestGetDirectMultiRueidis(t *testing.T) {
	addr := testutil.RedisAddr(t)
	r := newRueidis(t, []string{addr}, false, newRecorder())
	w := redis.NewClient(&redis.Options{Addr: addr})
	defer w.Close()
	checkGetDirectMulti(t, r, w)
}

func TestGetDirectMultiClusterCrossSlot(t *testing.T) {
	addrs := testutil.ClusterAddrs(t)
	w := redis.NewClusterClient(&redis.ClusterOptions{Addrs: addrs})
	defer w.Close()
	t.Run("rueidis", func(t *testing.T) {
		checkGetDirectMulti(t, newRueidis(t, addrs, true, newRecorder()), w)
	})
	t.Run("go-redis", func(t *testing.T) {
		g, err := NewGoRedis(context.Background(), GoRedisOptions{Direct: redis.NewClusterClient(&redis.ClusterOptions{Addrs: addrs}), OwnsDirect: true})
		if err != nil {
			t.Fatal(err)
		}
		defer g.Close()
		checkGetDirectMulti(t, g, w)
	})
}
