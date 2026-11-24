//go:build integration

package keyshift_test

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Huy-8bit/keyshift/internal/testutil"
	"github.com/Huy-8bit/keyshift/pkg/keyshift"
)

// BenchmarkCacheHit measures a parallel local cache hit through the SDK for
// both drivers. Compare with BenchmarkGoRedisCacheHit in internal/rediscompat
// for the cost of the client library alone.
//
//	make test-infra-up
//	KEYSHIFT_TEST_REDIS_ADDR=localhost:16379 go test -tags integration -run '^$' \
//	  -bench CacheHit -benchmem ./pkg/keyshift/
func BenchmarkCacheHit(b *testing.B) {
	addr := testutil.RedisAddr(b)
	for _, driver := range []keyshift.Driver{keyshift.DriverGoRedis, keyshift.DriverRueidis} {
		for _, size := range []int{64, 1024, 16384} {
			b.Run(string(driver)+"/"+strconv.Itoa(size), func(b *testing.B) {
				c, err := keyshift.New(keyshift.Config{
					ServiceName: "bench",
					Redis:       keyshift.RedisConfig{Addr: addr, Driver: driver},
					Cache:       keyshift.CacheConfig{Mode: keyshift.Explicit, PinPatterns: []string{"kstest:*"}, MaxStaleness: time.Minute},
				})
				if err != nil {
					b.Fatal(err)
				}
				defer c.Close()
				ctx := context.Background()
				key := testutil.Key(b, "hot")
				if err := c.SetString(ctx, key, strings.Repeat("x", size), 0); err != nil {
					b.Fatal(err)
				}
				_, _ = c.Get(ctx, key)
				c.RotateNow() // admits the pinned key
				_, _ = c.Get(ctx, key)
				if d := c.Explain(key); d.State != "admitted" {
					b.Fatalf("not admitted: %+v", d)
				}
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						if _, err := c.Get(ctx, key); err != nil {
							b.Error(err)
							return
						}
					}
				})
				b.StopTimer()
				if s := c.Stats(); s.LocalHits < uint64(b.N)*9/10 {
					b.Fatalf("expected local hits, got %+v", s)
				}
			})
		}
	}
}
