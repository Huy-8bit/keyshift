//go:build integration

package rediscompat

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Huy-8bit/keyshift/internal/testutil"
)

// BenchmarkGoRedisCacheHit measures a parallel local cache hit with go-redis's
// built-in LocalCache and with the instrumented wrapper KeyShift uses. The
// difference is the cost of the wrapper: go-redis skips its zero-copy read
// path for caches other than its own *LocalCache, so every hit copies the
// value once more. See docs/engineering-notes.md.
//
//	make test-infra-up
//	KEYSHIFT_TEST_REDIS_ADDR=localhost:16379 go test -tags integration -run '^$' \
//	  -bench GoRedisCacheHit -benchmem ./internal/rediscompat/
func BenchmarkGoRedisCacheHit(b *testing.B) {
	addr := testutil.RedisAddr(b)
	limits := CacheLimits{MaxEntries: 1000, MaxMemoryBytes: 64 << 20, MaxStaleness: time.Minute}
	caches := map[string]func() redis.Cache{
		"builtin": func() redis.Cache {
			return redis.NewLocalCache(redis.CacheConfig{MaxEntries: limits.MaxEntries, MaxMemoryBytes: limits.MaxMemoryBytes, MaxStaleness: limits.MaxStaleness})
		},
		"keyshift-wrapper": func() redis.Cache { c, _ := NewInstrumentedCache(limits); return c },
	}
	for _, size := range []int{64, 1024, 16384} {
		for _, name := range []string{"builtin", "keyshift-wrapper"} {
			b.Run(name+"/"+strconv.Itoa(size), func(b *testing.B) {
				rc := redis.NewClient(&redis.Options{Addr: addr, Protocol: 3, ClientSideCache: caches[name](),
					ClientSideCacheConfig: &redis.ClientSideCacheConfig{}})
				defer rc.Close()
				ctx := context.Background()
				key := testutil.Key(b, name)
				if err := rc.Set(ctx, key, strings.Repeat("x", size), 0).Err(); err != nil {
					b.Fatal(err)
				}
				rc.Get(ctx, key) // fill
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						if err := rc.Get(ctx, key).Err(); err != nil {
							b.Error(err)
							return
						}
					}
				})
			})
		}
	}
}
