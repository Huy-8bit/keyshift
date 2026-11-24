package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Huy-8bit/keyshift/internal/controller"
	"github.com/Huy-8bit/keyshift/internal/telemetry"
)

const (
	viewerTok = "viewer-token-0123456789"
	adminTok  = "admin-token-0123456789"
)

// run executes the CLI and returns its exit code, stdout, and stderr.
func run(args ...string) (int, string, string) {
	var out, errOut bytes.Buffer
	code := Main(args, &out, &errOut)
	return code, out.String(), errOut.String()
}

// newController starts an in-process controller with telemetry from two
// instances of one service that share a hot key.
func newController(t *testing.T) string {
	t.Helper()
	cfg := controller.DefaultConfig()
	cfg.Auth = controller.NewAuth(nil, []string{viewerTok}, []string{adminTok})
	cfg.PerInstanceMinInterval = 0
	cfg.DataDir = t.TempDir()
	s, err := controller.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)

	now := time.Now()
	for i, inst := range []string{"pod-a", "pod-b"} {
		b := &telemetry.Batch{
			SchemaVersion: telemetry.SchemaVersion, Service: "catalog", Instance: inst, Seq: 1,
			WindowStart: now.Add(-10 * time.Second), WindowEnd: now,
			FingerprintScope: "shared", FingerprintKeyID: "k1", PolicySource: "local", CacheMode: "adaptive",
			Tracking: telemetry.Tracking{State: "healthy"},
			Totals:   telemetry.Totals{Reads: 1000, LocalHits: 600, RedisReads: 400},
			Cache:    telemetry.Cache{Entries: 1, MemoryBytes: 100, MaxMemoryBytes: 1 << 20, Admitted: 1},
			Candidates: []telemetry.Candidate{{
				FP: fmt.Sprintf("%032x", 7), Display: "product:7", Reads: uint64(500 + 100*i), State: "admitted", SizeBytes: 64,
			}},
		}
		if _, err := s.Registry().Ingest(b, now); err != nil {
			t.Fatal(err)
		}
	}
	s.Aggregate(now)
	return ts.URL
}

func TestHelpAndUnknownCommand(t *testing.T) {
	code, out, _ := run("help")
	if code != 0 || !strings.Contains(out, "doctor") || !strings.Contains(out, "policy") {
		t.Fatalf("help: %d %q", code, out)
	}
	if code, _, errOut := run("no-such-command"); code != 2 || !strings.Contains(errOut, "unknown command") {
		t.Fatalf("unknown command: %d %q", code, errOut)
	}
}

func TestControllerQueries(t *testing.T) {
	url := newController(t)
	common := []string{"-controller", url, "-token", viewerTok}

	code, out, errOut := run(append([]string{"top"}, common...)...)
	if code != 0 || !strings.Contains(out, "product:7") || !strings.Contains(out, "catalog") {
		t.Fatalf("top: %d %q %q", code, out, errOut)
	}

	code, out, errOut = run(append([]string{"instances", "-json"}, common...)...)
	if code != 0 {
		t.Fatalf("instances: %d %q", code, errOut)
	}
	var instances struct {
		Total int                       `json:"total"`
		Items []controller.InstanceView `json:"items"`
	}
	if err := json.Unmarshal([]byte(out), &instances); err != nil || instances.Total != 2 || len(instances.Items) != 2 {
		t.Fatalf("instances JSON: %v %q", err, out)
	}

	code, out, errOut = run(append([]string{"stats"}, common...)...)
	if code != 0 || !strings.Contains(out, "catalog") {
		t.Fatalf("stats: %d %q %q", code, out, errOut)
	}

	// Without a token the controller refuses the request.
	if code, _, errOut := run("top", "-controller", url); code != 1 || !strings.Contains(errOut, "401") {
		t.Fatalf("unauthenticated top: %d %q", code, errOut)
	}
}

func TestPolicyLifecycle(t *testing.T) {
	url := newController(t)
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	good := write("good.json", `{"mode":"adaptive","deny_patterns":["stock:*"]}`)
	bad := write("bad.json", `{"mode":"sometimes"}`)
	admin := []string{"-controller", url, "-token", adminTok}

	if code, _, errOut := run("policy", "validate", "-f", good); code != 0 {
		t.Fatalf("validate good: %q", errOut)
	}
	if code, _, _ := run("policy", "validate", "-f", bad); code == 0 {
		t.Fatal("validate must reject an invalid policy")
	}

	// A viewer cannot change policies.
	if code, _, errOut := run("policy", "set", "-f", good, "-service", "catalog", "-controller", url, "-token", viewerTok); code != 1 || !strings.Contains(errOut, "403") {
		t.Fatalf("viewer set: %d %q", code, errOut)
	}

	steps := [][]string{
		{"policy", "set", "-f", good, "-service", "catalog", "-rollout", "25"},
		{"policy", "rollout", "-service", "catalog", "-percent", "100"},
		{"policy", "set", "-f", good, "-service", "catalog"},
		{"policy", "rollback", "-service", "catalog"},
		{"policy", "get"},
		{"policy", "history", "-service", "catalog"},
		{"policy", "delete", "-service", "catalog"},
	}
	for _, s := range steps {
		if code, out, errOut := run(append(s, admin...)...); code != 0 {
			t.Fatalf("%v: %d %q %q", s, code, out, errOut)
		}
	}
	code, out, _ := run(append([]string{"policy", "get", "-json"}, admin...)...)
	if code != 0 || strings.Contains(out, `"catalog"`) {
		t.Fatalf("the deleted policy must be gone: %q", out)
	}
}

func TestFormatTTL(t *testing.T) {
	for ttl, want := range map[time.Duration]string{
		-1: "no expiry", -time.Millisecond: "no expiry", -2: "key does not exist",
		-2 * time.Millisecond: "key does not exist", 1500 * time.Millisecond: "1.5s",
	} {
		if got := formatTTL(ttl); got != want {
			t.Errorf("formatTTL(%v) = %q, want %q", ttl, got, want)
		}
	}
}
