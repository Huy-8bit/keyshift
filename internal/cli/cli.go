// Package cli implements the keyshift command-line tool.
package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"
)

// Version is the CLI version.
const Version = "0.1.0"

type command struct {
	name    string
	summary string
	run     func(ctx context.Context, args []string, out io.Writer) error
}

func commands() []command {
	return []command{
		{"doctor", "Check Redis and controller compatibility with KeyShift", runDoctor},
		{"inspect", "Inspect Redis-side hot keys (HOTKEYS) or a single key", runInspect},
		{"top", "Show the global hot-key ranking from the controller", runTop},
		{"stats", "Show the global overview and per-service statistics", runStats},
		{"instances", "List SDK instances known to the controller", runInstances},
		{"policy", "Inspect, validate, and roll out caching policies", runPolicy},
		{"bench", "Run reproducible benchmarks (none vs csc vs keyshift)", RunBench},
		{"simulate", "Run a large-scale logical controller simulation", runSimulate},
		{"version", "Print the version", func(_ context.Context, _ []string, out io.Writer) error {
			fmt.Fprintln(out, "keyshift", Version)
			return nil
		}},
	}
}

// Main runs the CLI and returns the process exit code.
func Main(args []string, stdout, stderr io.Writer) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" || args[0] == "help" {
		usage(stdout)
		return 0
	}
	for _, c := range commands() {
		if c.name == args[0] {
			if err := c.run(ctx, args[1:], stdout); err != nil {
				if errors.Is(err, flag.ErrHelp) {
					return 0
				}
				var ec exitCode
				if errors.As(err, &ec) {
					return int(ec)
				}
				fmt.Fprintln(stderr, "keyshift "+c.name+":", err)
				return 1
			}
			return 0
		}
	}
	fmt.Fprintf(stderr, "keyshift: unknown command %q\n\n", args[0])
	usage(stderr)
	return 2
}

type exitCode int

func (e exitCode) Error() string { return fmt.Sprintf("exit %d", int(e)) }

func usage(w io.Writer) {
	fmt.Fprintln(w, "keyshift - adaptive client-side caching for Redis\n\nUsage: keyshift <command> [flags]\n\nCommands:")
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	for _, c := range commands() {
		fmt.Fprintf(tw, "  %s\t%s\n", c.name, c.summary)
	}
	tw.Flush()
	fmt.Fprintln(w, "\nRun 'keyshift <command> -h' for command flags.")
	fmt.Fprintln(w, "Controller commands read KEYSHIFT_CONTROLLER and KEYSHIFT_TOKEN from the environment.")
	fmt.Fprintln(w, "Redis commands read KEYSHIFT_REDIS_PASSWORD from the environment.")
}

func newFlags(name, usageLine string, out io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet("keyshift "+name, flag.ContinueOnError)
	fs.SetOutput(out)
	fs.Usage = func() {
		fmt.Fprintf(out, "Usage: keyshift %s %s\n\nFlags:\n", name, usageLine)
		fs.PrintDefaults()
	}
	return fs
}

// controllerFlags are shared by controller-backed commands.
type controllerFlags struct {
	url, token string
	json       bool
	timeout    time.Duration
}

func (c *controllerFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&c.url, "controller", envOr("KEYSHIFT_CONTROLLER", "http://localhost:8080"), "controller base URL")
	fs.StringVar(&c.token, "token", "", "API token (default $KEYSHIFT_TOKEN)")
	fs.BoolVar(&c.json, "json", false, "print raw JSON")
	fs.DurationVar(&c.timeout, "timeout", 10*time.Second, "request timeout")
}

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

type apiError struct {
	status int
	msg    string
}

func (e *apiError) Error() string { return fmt.Sprintf("controller returned %d: %s", e.status, e.msg) }

func (c *controllerFlags) do(ctx context.Context, method, path string, body any, out any) error {
	tok := c.token
	if tok == "" {
		tok = os.Getenv("KEYSHIFT_TOKEN")
	}
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	u, err := url.Parse(strings.TrimRight(c.url, "/") + path)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), rd)
	if err != nil {
		return err
	}
	if tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-KeyShift-Actor", "cli")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode >= 300 {
		var e struct{ Error string }
		_ = json.Unmarshal(raw, &e)
		if e.Error == "" {
			e.Error = strings.TrimSpace(string(raw))
		}
		return &apiError{resp.StatusCode, e.Error}
	}
	if out == nil || resp.StatusCode == http.StatusNoContent {
		return nil
	}
	if p, ok := out.(*json.RawMessage); ok {
		*p = raw
		return nil
	}
	return json.Unmarshal(raw, out)
}

func printJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func human(v float64) string {
	switch {
	case v >= 1e9:
		return fmt.Sprintf("%.2fG", v/1e9)
	case v >= 1e6:
		return fmt.Sprintf("%.2fM", v/1e6)
	case v >= 1e3:
		return fmt.Sprintf("%.1fk", v/1e3)
	default:
		return fmt.Sprintf("%.1f", v)
	}
}

func bytesHuman(v int64) string {
	switch {
	case v < 0:
		return "n/a"
	case v >= 1<<30:
		return fmt.Sprintf("%.1fGiB", float64(v)/(1<<30))
	case v >= 1<<20:
		return fmt.Sprintf("%.1fMiB", float64(v)/(1<<20))
	case v >= 1<<10:
		return fmt.Sprintf("%.1fKiB", float64(v)/(1<<10))
	default:
		return fmt.Sprintf("%dB", v)
	}
}
