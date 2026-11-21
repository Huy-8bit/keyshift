package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/Huy-8bit/keyshift/internal/controller"
	"github.com/Huy-8bit/keyshift/internal/policy"
)

func runTop(ctx context.Context, args []string, out io.Writer) error {
	fs := newFlags("top", "[flags]", out)
	var cf controllerFlags
	cf.register(fs)
	limit := fs.Int("limit", 20, "number of keys")
	service := fs.String("service", "", "only keys read by this service")
	class := fs.String("class", "", "filter by classification: global, local, single-instance")
	watch := fs.Duration("watch", 0, "refresh interval (0 prints once)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	q := url.Values{"limit": {strconv.Itoa(*limit)}}
	if *service != "" {
		q.Set("service", *service)
	}
	if *class != "" {
		q.Set("classification", *class)
	}
	for {
		var keys []controller.KeyView
		if err := cf.do(ctx, "GET", "/api/v1/hotkeys?"+q.Encode(), nil, &keys); err != nil {
			return err
		}
		if cf.json {
			if err := printJSON(out, keys); err != nil {
				return err
			}
		} else {
			if *watch > 0 {
				fmt.Fprint(out, "\033[H\033[2J")
				fmt.Fprintf(out, "keyshift top  %s  (refresh %s, Ctrl-C to quit)\n\n", time.Now().Format(time.TimeOnly), *watch)
			}
			tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
			fmt.Fprintln(tw, "#\tKEY\tREADS/S\tWRITES/S\tSAVED/S\tINSTANCES\tADMITTED\tMAX LOCAL/S\tCLASS\tTOP SERVICE\tRECOMMENDATION")
			for i, k := range keys {
				name := k.Display
				if name == "" {
					name = "fp:" + k.FP[:12]
				}
				top := ""
				if len(k.Services) > 0 {
					top = k.Services[0].Service
				}
				fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\t%d\t%d\t%s\t%s\t%s\t%s\n", i+1, name, human(k.ReadRate), human(k.WriteRate),
					human(k.SavedRate), k.Instances, k.States["admitted"], human(k.MaxLocalRate), k.Classification, top, k.Recommendation)
			}
			tw.Flush()
			if len(keys) == 0 {
				fmt.Fprintln(out, "no hot keys reported yet")
			}
		}
		if *watch <= 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(*watch):
		}
	}
}

func runStats(ctx context.Context, args []string, out io.Writer) error {
	fs := newFlags("stats", "[flags]", out)
	var cf controllerFlags
	cf.register(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	var ov controller.Overview
	var svcs []controller.ServiceView
	if err := cf.do(ctx, "GET", "/api/v1/overview", nil, &ov); err != nil {
		return err
	}
	if err := cf.do(ctx, "GET", "/api/v1/services", nil, &svcs); err != nil {
		return err
	}
	if cf.json {
		return printJSON(out, map[string]any{"overview": ov, "services": svcs})
	}
	r := ov.Rates
	fmt.Fprintf(out, "Instances: %d active, %d stale across %d services\n", ov.ActiveInstances, ov.StaleInstances, ov.Services)
	fmt.Fprintf(out, "SDK reads: %s/s  local hits: %s/s (%.1f%%)  Redis reads: %s/s  writes: %s/s  invalidations: %s/s\n",
		human(r.Reads), human(r.LocalHits), 100*ov.LocalHitRatio, human(r.RedisReads), human(r.Writes), human(r.Invalidations))
	fmt.Fprintf(out, "Local cache: %d entries, %s of %s budget, %d admitted keys\n", ov.CacheEntries,
		bytesHuman(ov.CacheMemoryBytes), bytesHuman(ov.CacheMemoryLimit), ov.AdmittedKeys)
	fmt.Fprintf(out, "Tracking: %v  Policies: %v\n", ov.Tracking, ov.PolicyVersions)
	fmt.Fprintf(out, "Controller: v%s, up since %s, ingest accepted=%d duplicate=%d rejected=%d, aggregation %.1fms\n\n",
		ov.Controller.Version, ov.Controller.StartedAt.Format(time.RFC3339), ov.Controller.IngestAccepted,
		ov.Controller.IngestDuplicate, ov.Controller.IngestRejected, ov.Controller.AggregationMillis)
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "SERVICE\tINSTANCES\tREADS/S\tHIT RATIO\tREDIS READS/S\tADMITTED\tCACHE MEM\tUNHEALTHY\tPOLICY VERSIONS")
	for _, s := range svcs {
		fmt.Fprintf(tw, "%s\t%d\t%s\t%.1f%%\t%s\t%d\t%s\t%d\t%v\n", s.Name, s.Instances, human(s.Rates.Reads), 100*s.HitRatio,
			human(s.Rates.RedisReads), s.AdmittedKeys, bytesHuman(s.CacheMemoryBytes), s.TrackingUnhealthy, s.PolicyVersions)
	}
	tw.Flush()
	if len(ov.Warnings) > 0 {
		fmt.Fprintln(out, "\nWarnings:")
		for _, w := range ov.Warnings {
			fmt.Fprintf(out, "  [%s] %s\n", w.Severity, w.Message)
		}
	}
	return nil
}

func runInstances(ctx context.Context, args []string, out io.Writer) error {
	fs := newFlags("instances", "[flags]", out)
	var cf controllerFlags
	cf.register(fs)
	service := fs.String("service", "", "filter by service")
	limit := fs.Int("limit", 100, "maximum instances")
	if err := fs.Parse(args); err != nil {
		return err
	}
	q := url.Values{"limit": {strconv.Itoa(*limit)}}
	if *service != "" {
		q.Set("service", *service)
	}
	var res struct {
		Total int                       `json:"total"`
		Items []controller.InstanceView `json:"items"`
	}
	if err := cf.do(ctx, "GET", "/api/v1/instances?"+q.Encode(), nil, &res); err != nil {
		return err
	}
	if cf.json {
		return printJSON(out, res)
	}
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "SERVICE\tINSTANCE\tDRIVER\tTRACKING\tLAST SEEN\tREADS/S\tHIT RATIO\tENTRIES\tCACHE MEM\tPOLICY")
	for _, i := range res.Items {
		fresh := fmt.Sprintf("%.0fs ago", i.AgeSeconds)
		if !i.Fresh {
			fresh += " (stale)"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%.1f%%\t%d\t%s\tv%d (%s)\n", i.Service, i.Instance, i.Driver, i.Tracking.State, fresh,
			human(i.Rates.Reads), 100*i.HitRatio, i.Cache.Entries, bytesHuman(i.Cache.MemoryBytes), i.PolicyVersion, i.PolicySource)
	}
	tw.Flush()
	fmt.Fprintf(out, "%d of %d instances shown\n", len(res.Items), res.Total)
	return nil
}

func runPolicy(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 {
		fmt.Fprintln(out, `Usage: keyshift policy <get|history|validate|set|rollout|rollback|disable|delete> [flags]

  get       Show active policy envelopes
  history   Show policy change history
  validate  Validate a policy file locally (-f FILE)
  set       Upload a policy (-f FILE [-service S] [-rollout N])
  rollout   Change the rollout percentage (-service S -percent N)
  rollback  Make the previous policy current (-service S)
  disable   Disable local caching (-service S)
  delete    Remove a service-specific policy (-service S)

Policies are JSON documents; see docs/configuration.md#policies.`)
		return nil
	}
	sub, args := args[0], args[1:]
	fs := newFlags("policy "+sub, "[flags]", out)
	var cf controllerFlags
	cf.register(fs)
	file := fs.String("f", "", "policy JSON file ('-' for stdin)")
	service := fs.String("service", "", "service scope (empty: default policy)")
	rollout := fs.Int("rollout", 100, "rollout percentage for set")
	percent := fs.Int("percent", -1, "rollout percentage for rollout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	scope := controller.DefaultScope
	if *service != "" {
		scope = *service
	}
	readPolicy := func() (*policy.Policy, error) {
		if *file == "" {
			return nil, fmt.Errorf("-f is required")
		}
		var r io.Reader = os.Stdin
		if *file != "-" {
			f, err := os.Open(*file)
			if err != nil {
				return nil, err
			}
			defer func() { _ = f.Close() }()
			r = f
		}
		return policy.Parse(r)
	}
	switch sub {
	case "get":
		var all map[string]*policy.Envelope
		if err := cf.do(ctx, "GET", "/api/v1/policies", nil, &all); err != nil {
			return err
		}
		if *service != "" {
			env, ok := all[*service]
			if !ok {
				env = all[controller.DefaultScope]
			}
			return printJSON(out, env)
		}
		return printJSON(out, all)
	case "history":
		var h []controller.HistoryEntry
		if err := cf.do(ctx, "GET", "/api/v1/policies/history?service="+url.QueryEscape(*service), nil, &h); err != nil {
			return err
		}
		if cf.json {
			return printJSON(out, h)
		}
		tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "TIME\tSCOPE\tACTION\tVERSION\tROLLOUT\tACTOR")
		for _, e := range h {
			fmt.Fprintf(tw, "%s\t%s\t%s\tv%d\t%d%%\t%s\n", e.At.Format(time.RFC3339), e.Service, e.Action, e.Version, e.RolloutPercent, e.Actor)
		}
		return tw.Flush()
	case "validate":
		p, err := readPolicy()
		if err != nil {
			return err
		}
		eff := policy.Resolve(nil, p)
		fmt.Fprintf(out, "policy is valid: mode=%s pins=%d denies=%d deny_fingerprints=%d min_read_rate=%.2f min_hit_ratio=%.2f budget=%d entries/%s\n",
			eff.Mode, len(p.PinPatterns), len(p.DenyPatterns), len(p.DenyFingerprints), eff.Admission.MinReadRate,
			eff.Admission.MinHitRatio, eff.Budget.MaxEntries, bytesHuman(eff.Budget.MaxMemoryBytes))
		return nil
	case "set":
		p, err := readPolicy()
		if err != nil {
			return err
		}
		if p.Service != "" && *service == "" {
			scope = p.Service
		}
		var env policy.Envelope
		if err := cf.do(ctx, "PUT", "/api/v1/policies/"+url.PathEscape(scope), map[string]any{"policy": p, "rollout_percent": *rollout}, &env); err != nil {
			return err
		}
		fmt.Fprintf(out, "policy v%d installed for %s at %d%% rollout\n", env.Current.Version, scope, env.RolloutPercent)
		return nil
	case "rollout":
		if *percent < 0 {
			return fmt.Errorf("-percent is required")
		}
		var env policy.Envelope
		if err := cf.do(ctx, "POST", "/api/v1/policies/"+url.PathEscape(scope)+"/rollout", map[string]int{"rollout_percent": *percent}, &env); err != nil {
			return err
		}
		fmt.Fprintf(out, "policy v%d for %s now at %d%% rollout\n", env.Current.Version, scope, env.RolloutPercent)
		return nil
	case "rollback", "disable":
		var env policy.Envelope
		if err := cf.do(ctx, "POST", "/api/v1/policies/"+url.PathEscape(scope)+"/"+sub, nil, &env); err != nil {
			return err
		}
		fmt.Fprintf(out, "%s: %s now runs policy v%d (mode=%s)\n", sub, scope, env.Current.Version, env.Current.Mode)
		return nil
	case "delete":
		if err := cf.do(ctx, "DELETE", "/api/v1/policies/"+url.PathEscape(scope), nil, nil); err != nil {
			return err
		}
		fmt.Fprintf(out, "deleted policy scope %s\n", scope)
		return nil
	}
	return fmt.Errorf("unknown policy subcommand %q", sub)
}

// rawGet is used by doctor to probe the controller.
func rawGet(ctx context.Context, cf *controllerFlags, path string) (json.RawMessage, error) {
	var raw json.RawMessage
	err := cf.do(ctx, "GET", path, nil, &raw)
	return raw, err
}
