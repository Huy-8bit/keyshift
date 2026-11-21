// Package hotkeys runs optional, privileged Redis-native hot-key
// diagnostics with the HOTKEYS command (Redis Open Source 8.6+).
//
// HOTKEYS measures server-side CPU time and network bytes per key. These are
// Redis-side observations and are reported separately from SDK-side access
// frequencies: a key can be expensive for Redis without being frequently
// read by any single application instance, and vice versa.
//
// Sessions are opt-in and time-bounded. KeyShift never stops or resets a
// session it did not start. The ordinary SDK never uses HOTKEYS.
package hotkeys

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Errors returned by Run.
var (
	ErrUnsupported   = errors.New("hotkeys: HOTKEYS is not supported by this server (requires Redis Open Source 8.6+)")
	ErrNotPermitted  = errors.New("hotkeys: the ACL user is not permitted to run HOTKEYS")
	ErrSessionActive = errors.New("hotkeys: another HOTKEYS session is already running on this node; not interfering with it")
)

// Limits on session parameters.
const (
	MinDuration = time.Second
	MaxDuration = 10 * time.Minute
)

// Options configure a diagnostic session.
type Options struct {
	// Metrics: "cpu", "net", or both. Default both.
	Metrics []string
	// Count of top keys per metric (10..64). Default 10.
	Count int
	// Duration of the session (required, 1s..10m). The server auto-stops it.
	Duration time.Duration
	// Sample tracks keys with probability 1/Sample. Default 1.
	Sample int64
}

// Capability describes HOTKEYS availability on a node.
type Capability struct {
	Node          string `json:"node"`
	ServerVersion string `json:"server_version"`
	Supported     bool   `json:"supported"`
	Permitted     bool   `json:"permitted"`
	Reason        string `json:"reason,omitempty"`
}

// Entry is one key's Redis-side cost.
type Entry struct {
	Key   string `json:"key"`
	Value int64  `json:"value"`
}

// Result is a normalized HOTKEYS session result for one node.
type Result struct {
	Source        string        `json:"source"` // always "redis-hotkeys"
	Node          string        `json:"node"`
	StartedAt     time.Time     `json:"started_at"`
	Duration      time.Duration `json:"duration"`
	SampleRatio   int           `json:"sample_ratio"`
	CPUUser       time.Duration `json:"cpu_user"`
	CPUSys        time.Duration `json:"cpu_sys"`
	TotalNetBytes int64         `json:"total_net_bytes"`
	// ByCPU values are CPU microseconds; ByNet values are bytes.
	ByCPU []Entry `json:"by_cpu_us"`
	ByNet []Entry `json:"by_net_bytes"`
}

// Validate normalizes and checks options.
func (o *Options) Validate() error {
	if len(o.Metrics) == 0 {
		o.Metrics = []string{"cpu", "net"}
	}
	for _, m := range o.Metrics {
		if m != "cpu" && m != "net" {
			return fmt.Errorf("hotkeys: unknown metric %q (want cpu or net)", m)
		}
	}
	if o.Count == 0 {
		o.Count = 10
	}
	if o.Count < 10 || o.Count > 64 {
		return errors.New("hotkeys: count must be within [10, 64]")
	}
	if o.Duration < MinDuration || o.Duration > MaxDuration {
		return fmt.Errorf("hotkeys: duration must be within [%s, %s]", MinDuration, MaxDuration)
	}
	if o.Sample == 0 {
		o.Sample = 1
	}
	if o.Sample < 1 {
		return errors.New("hotkeys: sample must be >= 1")
	}
	return nil
}

func classify(err error) error {
	if err == nil {
		return nil
	}
	s := err.Error()
	switch {
	case strings.Contains(s, "unknown command"):
		return ErrUnsupported
	case strings.Contains(s, "NOPERM"):
		return ErrNotPermitted
	case strings.Contains(s, "already in progress"):
		return ErrSessionActive
	}
	return err
}

// Detect checks whether HOTKEYS is available and permitted on c's node.
// It only uses read-only probes (COMMAND INFO and ACL DRYRUN-free checks).
func Detect(ctx context.Context, c *redis.Client) Capability {
	cap := Capability{Node: c.Options().Addr}
	if info, err := c.Info(ctx, "server").Result(); err == nil {
		for _, line := range strings.Split(info, "\n") {
			if v, ok := strings.CutPrefix(strings.TrimSpace(line), "redis_version:"); ok {
				cap.ServerVersion = v
			}
		}
	}
	res, err := c.Do(ctx, "COMMAND", "INFO", "HOTKEYS").Slice()
	if err != nil {
		cap.Reason = classify(err).Error()
		return cap
	}
	if len(res) == 0 || res[0] == nil {
		cap.Reason = ErrUnsupported.Error()
		return cap
	}
	cap.Supported = true
	// HOTKEYS GET is side-effect free; it reveals ACL permission.
	if err := c.Do(ctx, "HOTKEYS", "GET").Err(); err != nil && !errors.Is(err, redis.Nil) {
		if e := classify(err); errors.Is(e, ErrNotPermitted) {
			cap.Reason = e.Error()
			return cap
		}
	}
	cap.Permitted = true
	return cap
}

// Run starts a session, waits for it to finish, collects results, and
// resets the session it started. If ctx is cancelled the session is stopped
// and reset. A session started by someone else is never touched.
func Run(ctx context.Context, c *redis.Client, o Options) (*Result, error) {
	if err := o.Validate(); err != nil {
		return nil, err
	}
	args := &redis.HotKeysStartArgs{Count: uint8(o.Count), Duration: int64(o.Duration / time.Second), Sample: o.Sample}
	for _, m := range o.Metrics {
		args.Metrics = append(args.Metrics, redis.HotKeysMetric(strings.ToUpper(m)))
	}
	if err := c.HotKeysStart(ctx, args).Err(); err != nil {
		return nil, classify(err)
	}
	cleanup := func(stop bool) {
		cctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if stop {
			_ = c.HotKeysStop(cctx).Err()
		}
		_ = c.HotKeysReset(cctx).Err()
	}
	t := time.NewTimer(o.Duration + 200*time.Millisecond)
	defer t.Stop()
	select {
	case <-ctx.Done():
		cleanup(true)
		return nil, ctx.Err()
	case <-t.C:
	}
	// The server auto-stops after DURATION; wait briefly if it has not yet.
	var res *redis.HotKeysResult
	for range 20 {
		r, err := c.HotKeysGet(ctx).Result()
		if err != nil {
			cleanup(true)
			return nil, classify(err)
		}
		res = r
		if !r.TrackingActive {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	cleanup(res != nil && res.TrackingActive)
	return normalize(c.Options().Addr, res), nil
}

func normalize(node string, r *redis.HotKeysResult) *Result {
	out := &Result{Source: "redis-hotkeys", Node: node}
	if r == nil {
		return out
	}
	out.StartedAt = r.CollectionStartTime
	out.Duration = r.CollectionDuration
	out.SampleRatio = int(r.SampleRatio)
	out.CPUUser, out.CPUSys = r.UsedCPUUser, r.UsedCPUSys
	out.TotalNetBytes = r.TotalNetBytes
	conv := func(in []redis.HotKeysKeyEntry) []Entry {
		res := make([]Entry, 0, len(in))
		for _, e := range in {
			var v int64
			switch x := e.Value.(type) {
			case int64:
				v = x
			case string:
				v, _ = strconv.ParseInt(x, 10, 64)
			}
			res = append(res, Entry{Key: e.Key, Value: v})
		}
		return res
	}
	out.ByCPU = conv(r.ByCPUTime)
	out.ByNet = conv(r.ByNetBytes)
	return out
}
