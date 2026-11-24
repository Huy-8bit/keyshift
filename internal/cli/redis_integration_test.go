//go:build integration

package cli

import (
	"context"
	"strings"
	"testing"

	"github.com/Huy-8bit/keyshift/internal/testutil"
)

func TestDoctorAgainstRedis(t *testing.T) {
	addr := testutil.RedisAddr(t)
	code, out, errOut := run("doctor", "-redis", addr, "-skip-controller")
	if code != 0 {
		t.Fatalf("doctor: %d %q %q", code, out, errOut)
	}
	for _, want := range []string{"[PASS]  redis connectivity", "[PASS]  resp3", "[PASS]  client tracking", "local caching available"} {
		if !strings.Contains(out, want) {
			t.Errorf("doctor output lacks %q:\n%s", want, out)
		}
	}
	if code, out, _ := run("doctor", "-redis", "127.0.0.1:1", "-skip-controller"); code != 1 || !strings.Contains(out, "[FAIL]") {
		t.Fatalf("doctor against a closed port must fail: %d %q", code, out)
	}
}

func TestInspectKeyAgainstRedis(t *testing.T) {
	addr := testutil.RedisAddr(t)
	r := testutil.Plain(t, addr)
	ctx := context.Background()
	str, hash := testutil.Key(t, "s"), testutil.Key(t, "h")
	r.Set(ctx, str, "value", 0)
	r.HSet(ctx, hash, "f", "v")

	code, out, errOut := run("inspect", "key", "-redis", addr, str)
	if code != 0 {
		t.Fatalf("inspect string: %d %q", code, errOut)
	}
	for field, want := range map[string]string{"type": "string", "ttl": "no expiry", "value bytes": "5", "cacheable by keyshift": "yes (Get, MGet)"} {
		if got := field2(out, field); got != want {
			t.Errorf("inspect string %s = %q, want %q", field, got, want)
		}
	}
	code, out, _ = run("inspect", "key", "-redis", addr, hash)
	if code != 0 || field2(out, "type") != "hash" || !strings.HasPrefix(field2(out, "cacheable by keyshift"), "no") {
		t.Fatalf("inspect hash: %d %q", code, out)
	}
	if code, _, _ := run("inspect", "key", "-redis", addr); code != 1 {
		t.Fatal("inspect key without a key must fail")
	}
}

// field2 returns the value column of a "name<spaces>value" line.
func field2(out, name string) string {
	for _, line := range strings.Split(out, "\n") {
		if v, ok := strings.CutPrefix(line, name+"  "); ok {
			return strings.TrimSpace(v)
		}
	}
	return ""
}
