package cli

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/rueidis"

	"github.com/Huy-8bit/keyshift/internal/fingerprint"
	"github.com/Huy-8bit/keyshift/internal/hotkeys"
	"github.com/Huy-8bit/keyshift/internal/rediscompat"
)

type redisFlags struct {
	addr     string
	cluster  bool
	username string
	tls      bool
}

func (r *redisFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&r.addr, "redis", envOr("KEYSHIFT_REDIS_ADDR", "localhost:6379"), "Redis address (comma-separated seeds with -cluster)")
	fs.BoolVar(&r.cluster, "cluster", false, "Redis Cluster")
	fs.StringVar(&r.username, "username", os.Getenv("KEYSHIFT_REDIS_USERNAME"), "ACL username (password from $KEYSHIFT_REDIS_PASSWORD)")
	fs.BoolVar(&r.tls, "tls", false, "use TLS")
}

func (r *redisFlags) tlsConfig() *tls.Config {
	if !r.tls {
		return nil
	}
	return &tls.Config{MinVersion: tls.VersionTLS12}
}

func (r *redisFlags) standalone(protocol int) *redis.Client {
	return redis.NewClient(&redis.Options{Addr: strings.Split(r.addr, ",")[0], Username: r.username,
		Password: os.Getenv("KEYSHIFT_REDIS_PASSWORD"), TLSConfig: r.tlsConfig(), Protocol: protocol,
		DialTimeout: 3 * time.Second, ReadTimeout: 3 * time.Second, MaxRetries: 1})
}

// nodes returns one client per master (or the single standalone node).
func (r *redisFlags) nodes(ctx context.Context) ([]*redis.Client, func(), error) {
	if !r.cluster {
		c := r.standalone(3)
		return []*redis.Client{c}, func() { c.Close() }, nil
	}
	cc := redis.NewClusterClient(&redis.ClusterOptions{Addrs: strings.Split(r.addr, ","), Username: r.username,
		Password: os.Getenv("KEYSHIFT_REDIS_PASSWORD"), TLSConfig: r.tlsConfig()})
	defer cc.Close()
	var mu sync.Mutex
	var out []*redis.Client
	err := cc.ForEachMaster(ctx, func(_ context.Context, m *redis.Client) error {
		// Build fresh options: a live client's Options carry internal state.
		c := redis.NewClient(&redis.Options{Addr: m.Options().Addr, Username: r.username,
			Password: os.Getenv("KEYSHIFT_REDIS_PASSWORD"), TLSConfig: r.tlsConfig(), Protocol: 3})
		mu.Lock()
		out = append(out, c)
		mu.Unlock()
		return nil
	})
	slices.SortFunc(out, func(a, b *redis.Client) int { return strings.Compare(a.Options().Addr, b.Options().Addr) })
	return out, func() {
		for _, c := range out {
			c.Close()
		}
	}, err
}

type check struct{ status, name, detail string }

func runDoctor(ctx context.Context, args []string, out io.Writer) error {
	fs := newFlags("doctor", "[flags]", out)
	var rf redisFlags
	rf.register(fs)
	var cf controllerFlags
	cf.register(fs)
	skipController := fs.Bool("skip-controller", false, "do not check the controller")
	if err := fs.Parse(args); err != nil {
		return err
	}
	var checks []check
	add := func(status, name, format string, a ...any) {
		checks = append(checks, check{status, name, fmt.Sprintf(format, a...)})
	}

	c := rf.standalone(2)
	defer c.Close()
	info, err := c.Info(ctx, "server").Result()
	if err != nil {
		add("FAIL", "redis connectivity", "%v", err)
		return finishDoctor(out, checks)
	}
	version := rediscompat.ParseInfoField(info, "redis_version")
	mode := rediscompat.ParseInfoField(info, "redis_mode")
	add("PASS", "redis connectivity", "%s (Redis %s, mode %s)", rf.addr, version, mode)
	if mode == "cluster" && !rf.cluster {
		add("WARN", "topology", "server runs in cluster mode; re-run with -cluster")
	}

	var lat []time.Duration
	for range 20 {
		t0 := time.Now()
		if c.Ping(ctx).Err() == nil {
			lat = append(lat, time.Since(t0))
		}
	}
	if len(lat) > 0 {
		slices.Sort(lat)
		add("INFO", "round-trip latency", "p50 %s, max %s over %d PINGs (a local cache hit avoids one round trip)", lat[len(lat)/2], lat[len(lat)-1], len(lat))
	}

	if who, err := c.Do(ctx, "ACL", "WHOAMI").Text(); err == nil {
		add("INFO", "acl user", "%s", who)
	}

	c3 := rf.standalone(3)
	defer c3.Close()
	conn := c3.Conn()
	if err := conn.Ping(ctx).Err(); err != nil {
		add("FAIL", "resp3", "HELLO 3 failed: %v (client-side caching requires RESP3, Redis 6+)", err)
	} else {
		add("PASS", "resp3", "HELLO 3 accepted")
		if err := conn.Do(ctx, "CLIENT", "TRACKING", "ON", "OPTIN").Err(); err != nil {
			add("FAIL", "client tracking", "CLIENT TRACKING ON OPTIN failed: %v", err)
		} else {
			add("PASS", "client tracking", "CLIENT TRACKING ON OPTIN accepted")
			_ = conn.Do(ctx, "CLIENT", "TRACKING", "OFF").Err()
		}
	}
	_ = conn.Close()

	if rf.cluster {
		opt := rueidis.ClientOption{InitAddress: strings.Split(rf.addr, ","), Username: rf.username,
			Password: os.Getenv("KEYSHIFT_REDIS_PASSWORD"), TLSConfig: rf.tlsConfig()}
		r, err := rediscompat.NewRueidis(ctx, rediscompat.RueidisOptions{ClientOption: &opt, Cluster: true,
			Limits: rediscompat.CacheLimits{MaxEntries: 10, MaxMemoryBytes: 1 << 20, MaxStaleness: time.Second}})
		switch {
		case err != nil:
			add("FAIL", "keyshift rueidis driver", "%v", err)
		case r.Capabilities().Caching:
			add("PASS", "keyshift rueidis driver", "local caching available (OPTIN tracking, cluster)")
		default:
			add("WARN", "keyshift rueidis driver", "%s", r.Capabilities().Reason)
		}
		if r != nil {
			_ = r.Close()
		}
		add("INFO", "keyshift go-redis driver", "caching is not available on go-redis ClusterClient; detection and telemetry still work")
	} else {
		g, err := rediscompat.NewGoRedis(ctx, rediscompat.GoRedisOptions{Direct: rf.standalone(0), OwnsDirect: true,
			Limits: rediscompat.CacheLimits{MaxEntries: 10, MaxMemoryBytes: 1 << 20, MaxStaleness: time.Second}})
		switch {
		case err != nil:
			add("FAIL", "keyshift go-redis driver", "%v", err)
		case g.Capabilities().Caching:
			add("PASS", "keyshift go-redis driver", "local caching available (go-redis %s client-side cache)", redis.Version())
		default:
			add("WARN", "keyshift go-redis driver", "%s", g.Capabilities().Reason)
		}
		if g != nil {
			_ = g.Close()
		}
	}

	if cfgv, err := c.ConfigGet(ctx, "tracking-table-max-keys").Result(); err != nil {
		add("WARN", "tracking table limit", "cannot read tracking-table-max-keys (%v); ensure it is sized for cached keys", err)
	} else {
		v := cfgv["tracking-table-max-keys"]
		status := "PASS"
		if v != "0" && v != "" {
			status = "INFO"
		}
		add(status, "tracking table limit", "tracking-table-max-keys=%s (0 = unlimited; when the limit is hit Redis evicts tracked keys and sends invalidations)", v)
	}

	nodes, closeNodes, err := rf.nodes(ctx)
	if err != nil {
		add("WARN", "hotkeys", "cannot enumerate nodes: %v", err)
	} else {
		for _, n := range nodes {
			cap := hotkeys.Detect(ctx, n)
			switch {
			case cap.Supported && cap.Permitted:
				add("PASS", "hotkeys "+cap.Node, "HOTKEYS available (Redis %s); use `keyshift inspect hotkeys`", cap.ServerVersion)
			case cap.Supported:
				add("INFO", "hotkeys "+cap.Node, "HOTKEYS exists but this user lacks permission (optional diagnostics only)")
			default:
				add("INFO", "hotkeys "+cap.Node, "HOTKEYS unavailable (Redis %s); optional, requires Redis 8.6+", cap.ServerVersion)
			}
		}
		closeNodes()
	}

	if !*skipController {
		if _, err := rawGet(ctx, &cf, "/healthz"); err != nil {
			add("INFO", "controller", "%s unreachable (%v); the controller is optional", cf.url, err)
		} else {
			add("PASS", "controller", "%s healthy", cf.url)
			var who struct{ Role string }
			if err := cf.do(ctx, "GET", "/api/v1/whoami", nil, &who); err != nil {
				add("WARN", "controller auth", "%v", err)
			} else {
				add("PASS", "controller auth", "token role: %s", who.Role)
			}
		}
	}
	return finishDoctor(out, checks)
}

func finishDoctor(out io.Writer, checks []check) error {
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	failed := false
	for _, c := range checks {
		fmt.Fprintf(tw, "[%s]\t%s\t%s\n", c.status, c.name, c.detail)
		failed = failed || c.status == "FAIL"
	}
	tw.Flush()
	if failed {
		return exitCode(1)
	}
	return nil
}

func runInspect(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 {
		fmt.Fprintln(out, `Usage: keyshift inspect <hotkeys|key|fingerprint> [flags]

  hotkeys      Run a time-bounded Redis HOTKEYS session (Redis 8.6+, privileged)
  key KEY      Show type, size, TTL, and fingerprint of one key
  fingerprint  Compute the telemetry fingerprint of keys (reads secret from $KEYSHIFT_FINGERPRINT_SECRET)`)
		return nil
	}
	sub, args := args[0], args[1:]
	switch sub {
	case "hotkeys":
		return inspectHotKeys(ctx, args, out)
	case "key":
		return inspectKey(ctx, args, out)
	case "fingerprint":
		fs := newFlags("inspect fingerprint", "KEY...", out)
		if err := fs.Parse(args); err != nil {
			return err
		}
		h, err := hasherFromEnv()
		if err != nil {
			return err
		}
		for _, k := range fs.Args() {
			fmt.Fprintf(out, "%s  %s\n", h.Fingerprint(k), k)
		}
		return nil
	}
	return fmt.Errorf("unknown inspect subcommand %q", sub)
}

func hasherFromEnv() (*fingerprint.Hasher, error) {
	secret := os.Getenv("KEYSHIFT_FINGERPRINT_SECRET")
	if secret == "" {
		return nil, errors.New("set KEYSHIFT_FINGERPRINT_SECRET to the secret used by SDK instances")
	}
	return fingerprint.New([]byte(secret))
}

func inspectHotKeys(ctx context.Context, args []string, out io.Writer) error {
	fs := newFlags("inspect hotkeys", "[flags]", out)
	var rf redisFlags
	rf.register(fs)
	dur := fs.Duration("duration", 10*time.Second, "session duration (1s-10m)")
	metrics := fs.String("metrics", "cpu,net", "metrics: cpu, net, or both")
	count := fs.Int("count", 10, "top keys per metric (10-64)")
	sample := fs.Int64("sample", 1, "track keys with probability 1/sample")
	redact := fs.Bool("redact", false, "print fingerprints instead of key names (needs $KEYSHIFT_FINGERPRINT_SECRET)")
	asJSON := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	var h *fingerprint.Hasher
	if *redact {
		var err error
		if h, err = hasherFromEnv(); err != nil {
			return err
		}
	}
	nodes, closeNodes, err := rf.nodes(ctx)
	if err != nil {
		return err
	}
	defer closeNodes()
	opts := hotkeys.Options{Metrics: strings.Split(*metrics, ","), Count: *count, Duration: *dur, Sample: *sample}
	if err := opts.Validate(); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "collecting Redis-side hot keys on %d node(s) for %s...\n", len(nodes), *dur)
	type nodeRes struct {
		res *hotkeys.Result
		err error
	}
	results := make([]nodeRes, len(nodes))
	var wg sync.WaitGroup
	for i, n := range nodes {
		wg.Go(func() {
			r, err := hotkeys.Run(ctx, n, opts)
			results[i] = nodeRes{r, err}
		})
	}
	wg.Wait()
	name := func(k string) string {
		if h != nil {
			return "fp:" + h.Fingerprint(k)
		}
		return k
	}
	var all []*hotkeys.Result
	for i, r := range results {
		if r.err != nil {
			fmt.Fprintf(out, "%s: %v\n", nodes[i].Options().Addr, r.err)
			continue
		}
		if h != nil {
			for j := range r.res.ByCPU {
				r.res.ByCPU[j].Key = name(r.res.ByCPU[j].Key)
			}
			for j := range r.res.ByNet {
				r.res.ByNet[j].Key = name(r.res.ByNet[j].Key)
			}
		}
		all = append(all, r.res)
	}
	if *asJSON {
		return printJSON(out, all)
	}
	for _, r := range all {
		fmt.Fprintf(out, "\nnode %s: %s collected, sample 1/%d, cpu user %s sys %s, %s network\n", r.Node, r.Duration, r.SampleRatio,
			r.CPUUser, r.CPUSys, bytesHuman(r.TotalNetBytes))
		tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "RANK\tBY CPU (µs)\t\tBY NETWORK (bytes)\t")
		for i := 0; i < max(len(r.ByCPU), len(r.ByNet)); i++ {
			var a, b [2]string
			if i < len(r.ByCPU) {
				a = [2]string{r.ByCPU[i].Key, fmt.Sprint(r.ByCPU[i].Value)}
			}
			if i < len(r.ByNet) {
				b = [2]string{r.ByNet[i].Key, fmt.Sprint(r.ByNet[i].Value)}
			}
			fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\n", i+1, a[0], a[1], b[0], b[1])
		}
		tw.Flush()
	}
	fmt.Fprintln(out, "\nThese are Redis-side costs. Compare with SDK-side access frequencies from `keyshift top`.")
	return nil
}

func inspectKey(ctx context.Context, args []string, out io.Writer) error {
	fs := newFlags("inspect key", "[flags] KEY", out)
	var rf redisFlags
	rf.register(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		fs.Usage()
		return errors.New("exactly one key is required")
	}
	key := fs.Arg(0)
	var c redis.UniversalClient
	if rf.cluster {
		c = redis.NewClusterClient(&redis.ClusterOptions{Addrs: strings.Split(rf.addr, ","), Username: rf.username,
			Password: os.Getenv("KEYSHIFT_REDIS_PASSWORD"), TLSConfig: rf.tlsConfig()})
	} else {
		c = rf.standalone(2)
	}
	defer func() { _ = c.Close() }()
	typ, err := c.Type(ctx, key).Result()
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "key\t%s\n", key)
	fmt.Fprintf(tw, "type\t%s\n", typ)
	if typ != "none" {
		if ttl, err := c.PTTL(ctx, key).Result(); err == nil {
			fmt.Fprintf(tw, "ttl\t%s\n", ttl)
		}
		if mem, err := c.MemoryUsage(ctx, key).Result(); err == nil {
			fmt.Fprintf(tw, "memory usage\t%s\n", bytesHuman(mem))
		}
		if enc, err := c.ObjectEncoding(ctx, key).Result(); err == nil {
			fmt.Fprintf(tw, "encoding\t%s\n", enc)
		}
		if typ == "string" {
			if n, err := c.StrLen(ctx, key).Result(); err == nil {
				fmt.Fprintf(tw, "value bytes\t%d\n", n)
			}
			fmt.Fprintf(tw, "cacheable by keyshift\tyes (GET)\n")
		} else {
			fmt.Fprintf(tw, "cacheable by keyshift\tno (KeyShift caches GET on string keys)\n")
		}
	}
	if h, err := hasherFromEnv(); err == nil {
		fmt.Fprintf(tw, "fingerprint\t%s\n", h.Fingerprint(key))
	}
	return tw.Flush()
}
