package bench

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Environment records where a benchmark ran.
type Environment struct {
	Date          time.Time `json:"date"`
	GoVersion     string    `json:"go_version"`
	OS            string    `json:"os"`
	Arch          string    `json:"arch"`
	CPUs          int       `json:"cpus"`
	Hostname      string    `json:"hostname,omitempty"`
	RedisVersion  string    `json:"redis_version"`
	RedisAddr     string    `json:"redis_addr"`
	Config        Config    `json:"config"`
	ClientVersion string    `json:"go_redis_version"`
}

// Report is the full benchmark output.
type Report struct {
	Environment Environment `json:"environment"`
	Results     []Result    `json:"results"`
}

// CollectEnvironment describes the local environment.
func CollectEnvironment(ctx context.Context, cfg Config) Environment {
	env := Environment{Date: time.Now().UTC(), GoVersion: runtime.Version(), OS: runtime.GOOS, Arch: runtime.GOARCH,
		CPUs: runtime.NumCPU(), RedisAddr: cfg.RedisAddr, Config: cfg, ClientVersion: redis.Version()}
	env.Config.Log = nil
	c := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer c.Close()
	if info, err := c.Info(ctx, "server").Result(); err == nil {
		env.RedisVersion = infoFields(info)["redis_version"]
	}
	return env
}

// WriteJSON writes the report as indented JSON.
func WriteJSON(w io.Writer, r Report) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}

// WriteMarkdown renders comparison tables per scenario.
func WriteMarkdown(w io.Writer, r Report) {
	e := r.Environment
	fmt.Fprintf(w, "Environment: %s/%s, %d CPUs, %s, Redis %s at %s, go-redis %s. ", e.OS, e.Arch, e.CPUs, e.GoVersion, e.RedisVersion, e.RedisAddr, e.ClientVersion)
	loop := "closed-loop (each worker issues the next operation as soon as the previous completes)"
	if e.Config.Rate > 0 {
		loop = fmt.Sprintf("open-loop at %.0f ops/s target (latency measured from scheduled start)", e.Config.Rate)
	}
	fmt.Fprintf(w, "Duration %s after %s warmup, concurrency %d, %s, cache limits %d entries / %d MiB, max_staleness %s, KeyShift window %s.\n\n",
		e.Config.Duration, e.Config.Warmup, e.Config.Concurrency, loop, e.Config.MaxEntries, e.Config.MaxBytes>>20, e.Config.MaxStaleness, e.Config.Window)
	byScenario := map[string][]Result{}
	var order []string
	for _, res := range r.Results {
		if _, ok := byScenario[res.Scenario]; !ok {
			order = append(order, res.Scenario)
		}
		byScenario[res.Scenario] = append(byScenario[res.Scenario], res)
	}
	descs := map[string]string{}
	for _, s := range Scenarios() {
		descs[s.Name] = s.Description
	}
	for _, name := range order {
		rs := byScenario[name]
		tag := ""
		if rs[0].Unfavorable {
			tag = " (unfavorable for caching)"
		}
		fmt.Fprintf(w, "#### %s%s\n\n%s.\n\n", name, tag, descs[name])
		fmt.Fprintln(w, "| mode | ops/s | p50 µs | p99 µs | hit ratio | Redis GET/s | Redis CPU s | tracking keys | invalidations | stale reads | max stale ms | cache MiB | alloc B/op | client CPU µs/op | errors |")
		fmt.Fprintln(w, "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
		for _, x := range rs {
			mem := "-"
			if x.CacheMemory >= 0 && x.Mode != ModeNone {
				mem = fmt.Sprintf("%.2f", float64(x.CacheMemory)/(1<<20))
			}
			fmt.Fprintf(w, "| %s | %.0f | %.0f | %.0f | %.1f%% | %.0f | %.2f | %d | %d | %d (%.3f%%) | %.1f | %s | %.0f | %.2f | %d |\n",
				x.Mode, x.Throughput, x.P50us, x.P99us, 100*x.HitRatio, x.RedisGetPerSec, x.RedisCPUSeconds,
				x.TrackingKeys, x.Invalidations, x.StaleReads, 100*x.StaleRatio, x.MaxStaleMs, mem, x.AllocBytesPerOp, x.ClientCPUusOp, x.Errors)
		}
		var notes []string
		for _, x := range rs {
			for _, n := range x.Notes {
				notes = append(notes, x.Mode+": "+n)
			}
			if x.StartupMsMax > 0 {
				notes = append(notes, fmt.Sprintf("%s: slowest instance startup %.1f ms", x.Mode, x.StartupMsMax))
			}
		}
		if len(notes) > 0 {
			fmt.Fprintf(w, "\nNotes: %s.\n", strings.Join(notes, "; "))
		}
		fmt.Fprintln(w)
	}
}

// Hostname returns the host name or empty.
func Hostname() string {
	h, _ := os.Hostname()
	return h
}
