package bench

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

// Config configures a benchmark run.
type Config struct {
	RedisAddr    string
	ClusterAddrs []string
	Modes        []string
	Scenarios    []string
	Duration     time.Duration
	Warmup       time.Duration
	Concurrency  int
	Instances    int
	Seed         uint64
	// Cache limits applied identically to csc and keyshift modes.
	MaxEntries   int
	MaxBytes     int64
	MaxStaleness time.Duration
	// Window is KeyShift's detection window.
	Window time.Duration
	// TrackedPoolSize and DrainInterval tune KeyShift's go-redis tracked
	// client (0 = SDK defaults).
	TrackedPoolSize int
	DrainInterval   time.Duration
	// Rate, when > 0, runs open-loop at this many total operations per
	// second instead of closed-loop. Latency is then measured from each
	// operation's scheduled start time, so queueing delay is included.
	Rate float64
	// RestartContainer is the Docker container restarted by the
	// redis-restart scenario.
	RestartContainer string
	Log              io.Writer
}

// DefaultConfig returns the CI-friendly configuration.
func DefaultConfig() Config {
	return Config{RedisAddr: "localhost:6379", Modes: []string{ModeNone, ModeCSC, ModeKeyShift},
		Duration: 10 * time.Second, Warmup: 5 * time.Second, Concurrency: 32, Instances: 1, Seed: 1,
		MaxEntries: 1000, MaxBytes: 16 << 20, MaxStaleness: 3 * time.Second, Window: time.Second}
}

// Result is the measurement of one scenario in one mode.
type Result struct {
	Scenario    string  `json:"scenario"`
	Mode        string  `json:"mode"`
	Unfavorable bool    `json:"unfavorable"`
	Seconds     float64 `json:"seconds"`
	TargetRate  float64 `json:"target_rate,omitempty"`
	Instances   int     `json:"instances"`
	Concurrency int     `json:"concurrency"`

	Ops        uint64  `json:"ops"`
	Reads      uint64  `json:"reads"`
	Writes     uint64  `json:"writes"`
	Errors     uint64  `json:"errors"`
	Throughput float64 `json:"ops_per_sec"`
	P50us      float64 `json:"p50_us"`
	P95us      float64 `json:"p95_us"`
	P99us      float64 `json:"p99_us"`
	P999us     float64 `json:"p999_us"`

	LocalHits     uint64  `json:"local_hits"`
	HitRatio      float64 `json:"hit_ratio"`
	Invalidations uint64  `json:"invalidations"`
	CacheEntries  int64   `json:"cache_entries"`
	CacheMemory   int64   `json:"cache_memory_bytes"`
	AdmittedKeys  int     `json:"admitted_keys"`

	RedisGetCalls   uint64  `json:"redis_get_calls"`
	RedisGetPerSec  float64 `json:"redis_get_per_sec"`
	RedisCmdPerSec  float64 `json:"redis_cmds_per_sec"`
	RedisCPUSeconds float64 `json:"redis_cpu_seconds"`
	RedisUsedMemory int64   `json:"redis_used_memory_bytes"`
	TrackingKeys    int64   `json:"redis_tracking_keys"`
	TrackingItems   int64   `json:"redis_tracking_items"`
	TrackingClients int64   `json:"redis_tracking_clients"`

	StaleReads      uint64  `json:"stale_reads"`
	StaleRatio      float64 `json:"stale_ratio"`
	MaxStaleMs      float64 `json:"max_stale_ms"`
	AllocBytesPerOp float64 `json:"alloc_bytes_per_op"`
	AllocsPerOp     float64 `json:"allocs_per_op"`
	GCCycles        uint32  `json:"gc_cycles"`
	GCPauseMs       float64 `json:"gc_pause_ms"`
	PeakHeapMB      float64 `json:"peak_heap_mb"`
	ClientCPUusOp   float64 `json:"client_cpu_us_per_op"`
	StartupMsMax    float64 `json:"startup_ms_max,omitempty"`

	Notes []string `json:"notes,omitempty"`
}

// Run executes every selected scenario in every selected mode.
func Run(ctx context.Context, cfg Config) ([]Result, error) {
	if cfg.Log == nil {
		cfg.Log = io.Discard
	}
	all := Scenarios()
	var selected []Scenario
	if len(cfg.Scenarios) == 0 {
		for _, s := range all {
			if !s.Cluster && s.Fault != FaultRedisRestart {
				selected = append(selected, s)
			}
		}
	} else {
		for _, name := range cfg.Scenarios {
			found := false
			for _, s := range all {
				if s.Name == name {
					selected, found = append(selected, s), true
				}
			}
			if !found {
				return nil, fmt.Errorf("unknown scenario %q", name)
			}
		}
	}
	var results []Result
	for _, s := range selected {
		if s.Cluster && len(cfg.ClusterAddrs) == 0 {
			fmt.Fprintf(cfg.Log, "skip %s: --cluster addresses not set\n", s.Name)
			continue
		}
		if s.Fault == FaultRedisRestart && cfg.RestartContainer == "" {
			fmt.Fprintf(cfg.Log, "skip %s: --restart-container not set\n", s.Name)
			continue
		}
		for _, mode := range cfg.Modes {
			if mode == ModeKeyShiftRueidis && s.Cluster {
				continue // cluster keyshift already uses rueidis
			}
			fmt.Fprintf(cfg.Log, "running %-22s mode=%-16s ", s.Name, mode)
			r, err := runOne(ctx, cfg, s, mode)
			if err != nil {
				return results, fmt.Errorf("%s/%s: %w", s.Name, mode, err)
			}
			fmt.Fprintf(cfg.Log, "%10.0f ops/s  p99=%7.0fus  hit=%5.1f%%  redis_get/s=%9.0f  stale=%d\n",
				r.Throughput, r.P99us, 100*r.HitRatio, r.RedisGetPerSec, r.StaleReads)
			results = append(results, r)
			if ctx.Err() != nil {
				return results, ctx.Err()
			}
		}
	}
	return results, nil
}

// redisStats is a server-side snapshot summed across nodes.
type redisStats struct {
	getCalls, cmdCalls   uint64
	cpu                  float64
	usedMemory           int64
	trackKeys, trackItem int64
	trackClients         int64
}

func infoFields(info string) map[string]string {
	m := map[string]string{}
	for _, line := range strings.Split(info, "\n") {
		if k, v, ok := strings.Cut(strings.TrimSpace(line), ":"); ok {
			m[k] = v
		}
	}
	return m
}

func snapshot(ctx context.Context, nodes []*redis.Client) (redisStats, error) {
	var s redisStats
	for _, n := range nodes {
		info, err := n.Info(ctx, "commandstats", "cpu", "memory", "stats", "clients").Result()
		if err != nil {
			return s, err
		}
		f := infoFields(info)
		for k, v := range f {
			if strings.HasPrefix(k, "cmdstat_") {
				calls := parseStat(v, "calls")
				s.cmdCalls += calls
				if k == "cmdstat_get" {
					s.getCalls += calls
				}
			}
		}
		u, _ := strconv.ParseFloat(f["used_cpu_user"], 64)
		sy, _ := strconv.ParseFloat(f["used_cpu_sys"], 64)
		s.cpu += u + sy
		mem, _ := strconv.ParseInt(f["used_memory"], 10, 64)
		s.usedMemory += mem
		tk, _ := strconv.ParseInt(f["tracking_total_keys"], 10, 64)
		ti, _ := strconv.ParseInt(f["tracking_total_items"], 10, 64)
		tc, _ := strconv.ParseInt(f["tracking_clients"], 10, 64)
		s.trackKeys += tk
		s.trackItem += ti
		s.trackClients += tc
	}
	return s, nil
}

func parseStat(v, field string) uint64 {
	for _, kv := range strings.Split(v, ",") {
		if k, val, ok := strings.Cut(kv, "="); ok && k == field {
			n, _ := strconv.ParseUint(val, 10, 64)
			return n
		}
	}
	return 0
}

func adminNodes(ctx context.Context, cfg Config, cluster bool) ([]*redis.Client, func(), error) {
	if !cluster {
		c := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, Protocol: 2})
		return []*redis.Client{c}, func() { c.Close() }, c.Ping(ctx).Err()
	}
	cc := redis.NewClusterClient(&redis.ClusterOptions{Addrs: cfg.ClusterAddrs})
	defer cc.Close()
	var mu sync.Mutex
	var nodes []*redis.Client
	err := cc.ForEachMaster(ctx, func(ctx context.Context, m *redis.Client) error {
		mu.Lock()
		nodes = append(nodes, redis.NewClient(&redis.Options{Addr: m.Options().Addr, Protocol: 2}))
		mu.Unlock()
		return nil
	})
	return nodes, func() {
		for _, n := range nodes {
			n.Close()
		}
	}, err
}

// value encodes a version into a value of the given size.
func value(version uint64, size int) []byte {
	b := make([]byte, max(size, 8))
	binary.BigEndian.PutUint64(b, version)
	for i := 8; i < len(b); i++ {
		b[i] = byte('a' + i%26)
	}
	return b
}

func versionOf(b []byte) uint64 {
	if len(b) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}

func preload(ctx context.Context, cfg Config, s Scenario, cluster bool) error {
	var c redis.UniversalClient
	if cluster {
		c = redis.NewClusterClient(&redis.ClusterOptions{Addrs: cfg.ClusterAddrs})
	} else {
		c = redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	}
	defer c.Close()
	if cluster {
		cc := c.(*redis.ClusterClient)
		if err := cc.ForEachMaster(ctx, func(ctx context.Context, m *redis.Client) error { return m.FlushAll(ctx).Err() }); err != nil {
			return err
		}
	} else if err := c.FlushAll(ctx).Err(); err != nil {
		return err
	}
	const batch = 1000
	for i := 0; i < s.Keys; i += batch {
		p := c.Pipeline()
		for j := i; j < min(i+batch, s.Keys); j++ {
			p.Set(ctx, keyName(j), value(1, s.ValueSize), s.TTL)
		}
		if _, err := p.Exec(ctx); err != nil {
			return err
		}
	}
	return nil
}

type counter struct{ v atomic.Uint64 }

func (c *counter) inc()         { c.v.Add(1) }
func (c *counter) load() uint64 { return c.v.Load() }

// versions tracks the latest committed version per key and when it was
// committed, for stale-read detection.
type versions struct {
	v     []atomic.Uint64
	at    []atomic.Int64
	locks [1024]sync.Mutex
}

func newVersions(n int) *versions {
	vs := &versions{v: make([]atomic.Uint64, n), at: make([]atomic.Int64, n)}
	for i := range vs.v {
		vs.v[i].Store(1)
	}
	return vs
}

func write(ctx context.Context, t target, vs *versions, seq *atomic.Uint64, k int, key string, s Scenario) error {
	l := &vs.locks[k%len(vs.locks)]
	l.Lock()
	defer l.Unlock()
	v := seq.Add(1)
	if err := t.Set(ctx, key, value(v, s.ValueSize), s.TTL); err != nil {
		return err
	}
	vs.v[k].Store(v)
	vs.at[k].Store(time.Now().UnixNano())
	return nil
}

func injectFault(ctx context.Context, cfg Config, s Scenario, nodes []*redis.Client) string {
	switch s.Fault {
	case FaultTrackingKill:
		var killed int64
		for _, n := range nodes {
			k, _ := n.Do(ctx, "CLIENT", "KILL", "TYPE", "normal", "SKIPME", "yes").Int64()
			killed += k
		}
		return fmt.Sprintf("killed %d client connections at mid-run", killed)
	case FaultRedisRestart:
		out, err := exec.CommandContext(ctx, "docker", "restart", "-t", "0", cfg.RestartContainer).CombinedOutput()
		if err != nil {
			return fmt.Sprintf("docker restart failed: %v %s", err, out)
		}
		return "restarted Redis (no persistence) at mid-run; keys repopulated by cache-aside"
	case FaultControllerDown:
		return "KeyShift telemetry endpoint unreachable for the whole run"
	}
	return ""
}

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

func processCPU() time.Duration {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}
