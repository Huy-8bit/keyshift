package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/rueidis"

	"github.com/Huy-8bit/keyshift/internal/hotkeys"
	"github.com/Huy-8bit/keyshift/internal/rediscompat"
)

// Doctor check statuses. Only FAIL makes the command exit non-zero.
const (
	statusPass = "PASS"
	statusInfo = "INFO"
	statusWarn = "WARN"
	statusFail = "FAIL"
)

type check struct{ status, name, detail string }

// doctor runs compatibility checks against Redis and, optionally, the
// controller.
type doctor struct {
	rf     redisFlags
	cf     controllerFlags
	checks []check
}

func (d *doctor) add(status, name, format string, a ...any) {
	d.checks = append(d.checks, check{status, name, fmt.Sprintf(format, a...)})
}

func runDoctor(ctx context.Context, args []string, out io.Writer) error {
	fs := newFlags("doctor", "[flags]", out)
	var d doctor
	d.rf.register(fs)
	d.cf.register(fs)
	skipController := fs.Bool("skip-controller", false, "do not check the controller")
	if err := fs.Parse(args); err != nil {
		return err
	}

	c := d.rf.standalone(2)
	defer c.Close()
	if !d.checkServer(ctx, c) {
		return d.report(out)
	}
	d.checkLatency(ctx, c)
	if who, err := c.Do(ctx, "ACL", "WHOAMI").Text(); err == nil {
		d.add(statusInfo, "acl user", "%s", who)
	}
	d.checkTracking(ctx)
	if d.rf.cluster {
		d.checkRueidisCluster(ctx)
	} else {
		d.checkGoRedis(ctx)
	}
	d.checkTrackingTable(ctx, c)
	d.checkHotKeys(ctx)
	if !*skipController {
		d.checkController(ctx)
	}
	return d.report(out)
}

// checkServer identifies the server; nothing else is checked if it fails.
func (d *doctor) checkServer(ctx context.Context, c *redis.Client) bool {
	info, err := c.Info(ctx, "server").Result()
	if err != nil {
		d.add(statusFail, "redis connectivity", "%v", err)
		return false
	}
	srv := rediscompat.ParseServer(info)
	d.add(statusPass, "redis connectivity", "%s (%s, mode %s)", d.rf.addr, srv, srv.Mode)
	if srv.Mode == "cluster" && !d.rf.cluster {
		d.add(statusWarn, "topology", "server runs in cluster mode; re-run with -cluster")
	}
	return true
}

func (d *doctor) checkLatency(ctx context.Context, c *redis.Client) {
	var lat []time.Duration
	for range 20 {
		t0 := time.Now()
		if c.Ping(ctx).Err() == nil {
			lat = append(lat, time.Since(t0))
		}
	}
	if len(lat) == 0 {
		return
	}
	slices.Sort(lat)
	d.add(statusInfo, "round-trip latency", "p50 %s, max %s over %d PINGs (a local cache hit avoids one round trip)",
		lat[len(lat)/2], lat[len(lat)-1], len(lat))
}

// checkTracking verifies RESP3 and that this user may enable tracking.
func (d *doctor) checkTracking(ctx context.Context) {
	c3 := d.rf.standalone(3)
	defer c3.Close()
	conn := c3.Conn()
	defer func() { _ = conn.Close() }()
	if err := conn.Ping(ctx).Err(); err != nil {
		d.add(statusFail, "resp3", "HELLO 3 failed: %v (client-side caching requires RESP3, Redis 6+)", err)
		return
	}
	d.add(statusPass, "resp3", "HELLO 3 accepted")
	if err := conn.Do(ctx, "CLIENT", "TRACKING", "ON", "OPTIN").Err(); err != nil {
		d.add(statusFail, "client tracking", "CLIENT TRACKING ON OPTIN failed: %v", err)
		return
	}
	d.add(statusPass, "client tracking", "CLIENT TRACKING ON OPTIN accepted")
	_ = conn.Do(ctx, "CLIENT", "TRACKING", "OFF").Err()
}

// doctorLimits are tiny cache limits for driver probes.
var doctorLimits = rediscompat.CacheLimits{MaxEntries: 10, MaxMemoryBytes: 1 << 20, MaxStaleness: time.Second}

func (d *doctor) checkGoRedis(ctx context.Context) {
	g, err := rediscompat.NewGoRedis(ctx, rediscompat.GoRedisOptions{Direct: d.rf.standalone(0), OwnsDirect: true, Limits: doctorLimits})
	if err != nil {
		d.add(statusFail, "keyshift go-redis driver", "%v", err)
		return
	}
	defer func() { _ = g.Close() }()
	if caps := g.Capabilities(); caps.Caching {
		d.add(statusPass, "keyshift go-redis driver", "local caching available (go-redis %s client-side cache)", redis.Version())
	} else {
		d.add(statusWarn, "keyshift go-redis driver", "%s", caps.Reason)
	}
}

func (d *doctor) checkRueidisCluster(ctx context.Context) {
	opt := rueidis.ClientOption{InitAddress: strings.Split(d.rf.addr, ","), Username: d.rf.username,
		Password: os.Getenv("KEYSHIFT_REDIS_PASSWORD"), TLSConfig: d.rf.tlsConfig()}
	r, err := rediscompat.NewRueidis(ctx, rediscompat.RueidisOptions{ClientOption: &opt, Cluster: true, Limits: doctorLimits})
	switch {
	case err != nil:
		d.add(statusFail, "keyshift rueidis driver", "%v", err)
	case r.Capabilities().Caching:
		d.add(statusPass, "keyshift rueidis driver", "local caching available (OPTIN tracking, cluster)")
	default:
		d.add(statusWarn, "keyshift rueidis driver", "%s", r.Capabilities().Reason)
	}
	if r != nil {
		_ = r.Close()
	}
	d.add(statusInfo, "keyshift go-redis driver", "caching is not available on go-redis ClusterClient; detection and telemetry still work")
}

func (d *doctor) checkTrackingTable(ctx context.Context, c *redis.Client) {
	cfg, err := c.ConfigGet(ctx, "tracking-table-max-keys").Result()
	if err != nil {
		d.add(statusWarn, "tracking table limit", "cannot read tracking-table-max-keys (%v); ensure it is sized for cached keys", err)
		return
	}
	v := cfg["tracking-table-max-keys"]
	status := statusPass
	if v != "0" && v != "" {
		status = statusInfo
	}
	d.add(status, "tracking table limit", "tracking-table-max-keys=%s (0 = unlimited; when the limit is hit Redis evicts tracked keys and sends invalidations)", v)
}

func (d *doctor) checkHotKeys(ctx context.Context) {
	nodes, closeNodes, err := d.rf.nodes(ctx)
	if err != nil {
		d.add(statusWarn, "hotkeys", "cannot enumerate nodes: %v", err)
		return
	}
	defer closeNodes()
	for _, n := range nodes {
		hc := hotkeys.Detect(ctx, n)
		switch {
		case hc.Supported && hc.Permitted:
			d.add(statusPass, "hotkeys "+hc.Node, "HOTKEYS available (%s); use `keyshift inspect hotkeys`", hc.ServerVersion)
		case hc.Supported:
			d.add(statusInfo, "hotkeys "+hc.Node, "HOTKEYS exists but this user lacks permission (optional diagnostics only)")
		default:
			d.add(statusInfo, "hotkeys "+hc.Node, "HOTKEYS unavailable (%s); optional, requires Redis 8.6+", hc.ServerVersion)
		}
	}
}

func (d *doctor) checkController(ctx context.Context) {
	if _, err := rawGet(ctx, &d.cf, "/healthz"); err != nil {
		d.add(statusInfo, "controller", "%s unreachable (%v); the controller is optional", d.cf.url, err)
		return
	}
	d.add(statusPass, "controller", "%s healthy", d.cf.url)
	var who struct{ Role string }
	if err := d.cf.do(ctx, "GET", "/api/v1/whoami", nil, &who); err != nil {
		d.add(statusWarn, "controller auth", "%v", err)
		return
	}
	d.add(statusPass, "controller auth", "token role: %s", who.Role)
}

// report prints the checks and fails the command if any check failed.
func (d *doctor) report(out io.Writer) error {
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	failed := false
	for _, c := range d.checks {
		fmt.Fprintf(tw, "[%s]\t%s\t%s\n", c.status, c.name, c.detail)
		failed = failed || c.status == statusFail
	}
	tw.Flush()
	if failed {
		return exitCode(1)
	}
	return nil
}
