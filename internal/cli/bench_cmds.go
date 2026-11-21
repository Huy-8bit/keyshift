package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/Huy-8bit/keyshift/internal/bench"
	"github.com/Huy-8bit/keyshift/internal/simulate"
)

// RunBench implements `keyshift bench` and the keyshift-bench binary.
func RunBench(ctx context.Context, args []string, out io.Writer) error {
	def := bench.DefaultConfig()
	fs := newFlags("bench", "[flags]", out)
	addr := fs.String("redis", envOr("KEYSHIFT_REDIS_ADDR", def.RedisAddr), "Redis address (benchmarks FLUSHALL this server!)")
	cluster := fs.String("cluster", "", "comma-separated Redis Cluster seeds for cluster scenarios")
	modes := fs.String("modes", strings.Join(def.Modes, ","), "modes: none,csc,keyshift,keyshift-rueidis")
	scenarios := fs.String("scenarios", "", "comma-separated scenarios (default: all non-cluster, non-restart)")
	list := fs.Bool("list", false, "list scenarios and exit")
	fs.DurationVar(&def.Duration, "duration", def.Duration, "measured duration per run")
	fs.DurationVar(&def.Warmup, "warmup", def.Warmup, "warmup before measuring (lets KeyShift admit keys)")
	fs.IntVar(&def.Concurrency, "concurrency", def.Concurrency, "concurrent workers")
	fs.IntVar(&def.Instances, "instances", def.Instances, "virtual SDK instances (scenarios may override)")
	fs.IntVar(&def.MaxEntries, "max-entries", def.MaxEntries, "local cache entry limit (csc and keyshift)")
	fs.Int64Var(&def.MaxBytes, "max-bytes", def.MaxBytes, "local cache memory limit (csc and keyshift)")
	fs.DurationVar(&def.MaxStaleness, "max-staleness", def.MaxStaleness, "client-side staleness backstop")
	fs.DurationVar(&def.Window, "window", def.Window, "KeyShift detection window")
	fs.Float64Var(&def.Rate, "rate", 0, "open-loop total ops/s (0: closed loop)")
	fs.IntVar(&def.TrackedPoolSize, "ks-tracked-pool", 0, "KeyShift go-redis tracked pool size (0: SDK default)")
	fs.DurationVar(&def.DrainInterval, "ks-drain", 0, "KeyShift go-redis invalidation drain interval (0: SDK default)")
	fs.StringVar(&def.RestartContainer, "restart-container", "", "Docker container restarted by the redis-restart scenario")
	jsonOut := fs.String("json", "", "write the JSON report to this file")
	mdOut := fs.String("markdown", "", "write a Markdown report to this file")
	yes := fs.Bool("yes", false, "confirm that the target Redis may be flushed")
	render := fs.String("render", "", "re-render the Markdown report of an existing JSON report and exit")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *render != "" {
		f, err := os.Open(*render)
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		var rep bench.Report
		if err := json.NewDecoder(f).Decode(&rep); err != nil {
			return fmt.Errorf("%s: %w", *render, err)
		}
		bench.WriteMarkdown(out, rep)
		return nil
	}
	if *list {
		tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
		for _, s := range bench.Scenarios() {
			tag := ""
			if s.Unfavorable {
				tag = "(unfavorable)"
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\n", s.Name, s.Description, tag)
		}
		return tw.Flush()
	}
	if !*yes {
		return fmt.Errorf("benchmarks run FLUSHALL on %s; re-run with -yes to confirm", *addr)
	}
	def.RedisAddr = *addr
	if *cluster != "" {
		def.ClusterAddrs = strings.Split(*cluster, ",")
	}
	def.Modes = strings.Split(*modes, ",")
	if *scenarios != "" {
		def.Scenarios = strings.Split(*scenarios, ",")
	}
	def.Log = os.Stderr
	env := bench.CollectEnvironment(ctx, def)
	results, err := bench.Run(ctx, def)
	rep := bench.Report{Environment: env, Results: results}
	if *jsonOut != "" {
		f, ferr := os.Create(*jsonOut)
		if ferr != nil {
			return ferr
		}
		werr := bench.WriteJSON(f, rep)
		if cerr := f.Close(); werr == nil {
			werr = cerr
		}
		if werr != nil {
			return werr
		}
	}
	if *mdOut != "" {
		f, ferr := os.Create(*mdOut)
		if ferr != nil {
			return ferr
		}
		bench.WriteMarkdown(f, rep)
		if cerr := f.Close(); cerr != nil {
			return cerr
		}
	}
	bench.WriteMarkdown(out, rep)
	return err
}

func runSimulate(ctx context.Context, args []string, out io.Writer) error {
	def := simulate.DefaultConfig()
	fs := newFlags("simulate", "[flags]", out)
	fs.IntVar(&def.Services, "services", def.Services, "number of services")
	fs.IntVar(&def.InstancesPerService, "instances-per-service", def.InstancesPerService, "virtual SDK instances per service")
	fs.IntVar(&def.CandidatesPerBatch, "candidates", def.CandidatesPerBatch, "candidate records per batch")
	fs.IntVar(&def.KeySpace, "keyspace", def.KeySpace, "global key space size")
	fs.IntVar(&def.Rounds, "rounds", def.Rounds, "upload rounds")
	fs.IntVar(&def.Concurrency, "concurrency", def.Concurrency, "concurrent uploaders")
	fs.DurationVar(&def.Paced, "paced", 0, "spread each round over this interval (0: as fast as possible)")
	fs.StringVar(&def.ControllerURL, "controller", "", "external controller URL (default: in-process controller)")
	fs.StringVar(&def.Token, "token", os.Getenv("KEYSHIFT_TOKEN"), "ingest token for an external controller")
	fs.StringVar(&def.InstancePrefix, "instance-prefix", "", "prefix for virtual instance IDs (unique per simulator)")
	asJSON := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "simulating %d virtual SDK instances (%d services x %d), %d candidates each, %d rounds...\n",
		def.Services*def.InstancesPerService, def.Services, def.InstancesPerService, def.CandidatesPerBatch, def.Rounds)
	res, err := simulate.Run(ctx, def)
	if err != nil {
		return err
	}
	if *asJSON {
		return printJSON(out, res)
	}
	fmt.Fprintln(out, "LOGICAL SIMULATION: virtual instances upload synthetic telemetry over HTTP; no Redis workload or real pods.")
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "ROUND\tBATCHES\tRECORDS\tSECONDS\tBATCHES/S\tRECORDS/S\tP50 ms\tP99 ms\tMAX ms\tERRORS\tREJECTED\tAVG BATCH")
	for _, r := range res.Rounds {
		fmt.Fprintf(tw, "%d\t%d\t%d\t%.2f\t%.0f\t%.0f\t%.2f\t%.2f\t%.2f\t%d\t%d\t%s\n", r.Round, r.Batches, r.Records, r.Seconds,
			r.BatchesPerSec, r.RecordsPerSec, r.P50ms, r.P99ms, r.MaxMs, r.Errors, r.Rejected, bytesHuman(int64(r.AvgBatchBytes)))
	}
	tw.Flush()
	for _, r := range res.Rounds {
		if r.FirstFailure != "" {
			fmt.Fprintf(out, "round %d first failure: %s\n", r.Round, r.FirstFailure)
		}
	}
	if res.InProcess {
		fmt.Fprintf(out, "aggregation of %d instances: %.1f ms; %d global keys; top key seen on %d instances in %d services; process heap in use %.1f MiB (controller + simulator)\n",
			res.ActiveInstances, res.AggregationMs, res.GlobalKeys, res.TopKeyInstances, res.TopKeyServices, res.HeapInuseMB)
	}
	fmt.Fprintf(out, "GOMAXPROCS=%d, telemetry encode time %.2fs total\n", res.GoMaxProcs, res.EncodeSecondsMean)
	_ = time.Second
	return nil
}
