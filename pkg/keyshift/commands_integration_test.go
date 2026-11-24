//go:build integration

package keyshift_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/Huy-8bit/keyshift/internal/testutil"
)

func TestMGetAgainstRedis(t *testing.T) {
	addr := testutil.RedisAddr(t)
	for _, d := range drivers {
		t.Run(d.name, func(t *testing.T) {
			c := d.mk(t, addr, cfg("mget"))
			defer c.Close()
			ext := testutil.Plain(t, addr)
			ctx := context.Background()

			hot := testutil.Key(t, "hot")
			ext.Set(ctx, hot, "h", 0)
			admit(t, c, hot)
			keys := []string{hot}
			for i := range 10 {
				k := testutil.Key(t, "k"+strconv.Itoa(i))
				ext.Set(ctx, k, "v"+strconv.Itoa(i), 0)
				keys = append(keys, k)
			}
			keys = append(keys, testutil.Key(t, "missing"))

			before := c.Stats()
			got, err := c.MGet(ctx, keys...)
			if err != nil {
				t.Fatal(err)
			}
			if got[0].Value != "h" || !got[0].Found {
				t.Fatalf("hot key: %+v", got[0])
			}
			for i := range 10 {
				if kv := got[i+1]; !kv.Found || kv.Value != "v"+strconv.Itoa(i) || kv.Key != keys[i+1] {
					t.Fatalf("key %d: %+v", i, kv)
				}
			}
			if got[11].Found {
				t.Fatalf("missing key reported found: %+v", got[11])
			}
			after := c.Stats()
			if after.Reads-before.Reads != 12 || after.DirectReads-before.DirectReads != 11 || after.CachedReads-before.CachedReads != 1 {
				t.Fatalf("read accounting: before %+v after %+v", before, after)
			}
			// A cached key written elsewhere is invalidated for MGet too.
			ext.Set(ctx, hot, "h2", 0)
			testutil.Eventually(t, 2*time.Second, func() bool {
				kv, err := c.MGet(ctx, hot)
				return err == nil && kv[0].Value == "h2"
			}, "MGet after invalidation")
		})
	}
}

func TestNotifyWriteAgainstRedis(t *testing.T) {
	addr := testutil.RedisAddr(t)
	for _, d := range drivers {
		t.Run(d.name, func(t *testing.T) {
			c := d.mk(t, addr, cfg("notify"))
			defer c.Close()
			ctx := context.Background()
			k := testutil.Key(t, "counter")
			if err := c.SetString(ctx, k, "1", 0); err != nil {
				t.Fatal(err)
			}
			admit(t, c, k)
			// INCR through the underlying client, then NotifyWrite: the next
			// read must see the new value without waiting for invalidation.
			for i := 2; i <= 50; i++ {
				if d.name == "go-redis" {
					if err := c.GoRedis().Incr(ctx, k).Err(); err != nil {
						t.Fatal(err)
					}
				} else {
					r := c.Rueidis()
					if err := r.Do(ctx, r.B().Incr().Key(k).Build()).Error(); err != nil {
						t.Fatal(err)
					}
				}
				c.NotifyWrite(k)
				if v, err := c.Get(ctx, k); err != nil || v != strconv.Itoa(i) {
					t.Fatalf("after INCR %d: got %q %v", i, v, err)
				}
			}
			if s := c.Stats(); s.Writes != 1 || s.NotifiedWrites != 49 {
				t.Fatalf("writes = %d, notified = %d; want 1 and 49", s.Writes, s.NotifiedWrites)
			}
		})
	}
}
