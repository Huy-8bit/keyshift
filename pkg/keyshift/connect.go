package keyshift

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/rueidis"

	"github.com/Huy-8bit/keyshift/internal/rediscompat"
)

// backendInitTimeout bounds the Redis round trips made while a backend is
// created (server version, tracking verification).
const backendInitTimeout = 10 * time.Second

// addrs returns the configured seed addresses.
func (r RedisConfig) addrs() []string {
	if len(r.Addrs) > 0 {
		return r.Addrs
	}
	if r.Addr != "" {
		return []string{r.Addr}
	}
	return nil
}

// newGoRedisClient creates the application go-redis client for New.
func newGoRedisClient(r RedisConfig, addrs []string) redis.UniversalClient {
	if r.Cluster {
		return redis.NewClusterClient(&redis.ClusterOptions{
			Addrs: addrs, Username: r.Username, Password: r.Password, TLSConfig: r.TLSConfig, PoolSize: r.PoolSize,
			DialTimeout: r.DialTimeout, ReadTimeout: r.ReadTimeout, WriteTimeout: r.WriteTimeout,
		})
	}
	return redis.NewClient(&redis.Options{
		Addr: addrs[0], Username: r.Username, Password: r.Password, DB: r.DB, TLSConfig: r.TLSConfig, PoolSize: r.PoolSize,
		DialTimeout: r.DialTimeout, ReadTimeout: r.ReadTimeout, WriteTimeout: r.WriteTimeout,
	})
}

// rueidisOption translates RedisConfig for the rueidis driver.
func rueidisOption(r RedisConfig, addrs []string) rueidis.ClientOption {
	opt := rueidis.ClientOption{
		InitAddress: addrs, Username: r.Username, Password: r.Password,
		SelectDB: r.DB, TLSConfig: r.TLSConfig, ForceSingleClient: !r.Cluster,
	}
	if r.DialTimeout > 0 {
		opt.Dialer.Timeout = r.DialTimeout
	}
	if r.WriteTimeout > 0 {
		opt.ConnWriteTimeout = r.WriteTimeout
	}
	return opt
}

func newGoRedisBackend(cfg Config, rc redis.UniversalClient, owns bool, h rediscompat.Hooks, l rediscompat.CacheLimits) (rediscompat.Backend, error) {
	ctx, cancel := context.WithTimeout(context.Background(), backendInitTimeout)
	defer cancel()
	return rediscompat.NewGoRedis(ctx, rediscompat.GoRedisOptions{
		Direct: rc, OwnsDirect: owns, Limits: l,
		TrackedPoolSize: cfg.Tracking.TrackedPoolSize, DrainInterval: cfg.Tracking.DrainInterval,
		DisableCaching: cachingDisabledReason(cfg) != "", Hooks: h,
	})
}

func newRueidisBackend(cfg Config, opt rueidis.ClientOption, h rediscompat.Hooks, l rediscompat.CacheLimits) (rediscompat.Backend, error) {
	ctx, cancel := context.WithTimeout(context.Background(), backendInitTimeout)
	defer cancel()
	cluster := !opt.ForceSingleClient && (cfg.Redis.Cluster || len(opt.InitAddress) > 1)
	return rediscompat.NewRueidis(ctx, rediscompat.RueidisOptions{
		ClientOption: &opt, Cluster: cluster, Limits: l, Hooks: h,
		CachingUnavailable: cachingDisabledReason(cfg),
	})
}

// cachingDisabledReason is non-empty when configuration rules out local
// caching entirely.
func cachingDisabledReason(cfg Config) string {
	if cfg.Cache.Mode == Off || cfg.Detection.Disabled {
		return "caching disabled by configuration"
	}
	return ""
}

// defaultInstanceID derives an instance identity from POD_NAME or the host
// name plus a random suffix, restricted to a safe character set.
func defaultInstanceID() string {
	base := os.Getenv("POD_NAME")
	if base == "" {
		base, _ = os.Hostname()
	}
	base = strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.' {
			return r
		}
		return '-'
	}, base)
	if base == "" {
		base = "instance"
	}
	if len(base) > 200 {
		base = base[:200]
	}
	var b [3]byte
	_, _ = rand.Read(b[:])
	return base + "-" + hex.EncodeToString(b[:])
}
