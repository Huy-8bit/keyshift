package controller

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Huy-8bit/keyshift/internal/policy"
	"github.com/Huy-8bit/keyshift/internal/telemetry"
)

func scrape(t *testing.T, s *Server) string {
	t.Helper()
	mfs, err := s.promReg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	for _, mf := range mfs {
		for _, m := range mf.Metric {
			b.WriteString(mf.GetName())
			b.WriteString("{")
			for _, lp := range m.Label {
				b.WriteString(lp.GetName() + "=" + strconv.Quote(lp.GetValue()) + ",")
			}
			b.WriteString("} ")
			if m.Gauge != nil {
				b.WriteString(strconv.FormatFloat(m.Gauge.GetValue(), 'f', -1, 64))
			}
			b.WriteString("\n")
		}
	}
	return b.String()
}

func TestFleetMetrics(t *testing.T) {
	s, ts := newTestServer(t, func(c *Config) { c.MetricsTopKeys = 3 })
	for i := range 4 {
		svc := "svc-a"
		if i%2 == 1 {
			svc = "svc-b"
		}
		var cands []telemetry.Candidate
		for j := range 6 {
			c := telemetry.Candidate{FP: fp(j), Reads: uint64(1000 * (6 - j)), State: "admitted"}
			if j == 0 {
				c.Display = "product:1"
			}
			cands = append(cands, c)
		}
		if code, _ := send(t, ts, batch(svc, "pod-"+strconv.Itoa(i), 1, cands...)); code != 200 {
			t.Fatal("ingest")
		}
	}
	s.policies.Put(&policy.Policy{Service: "svc-a", Mode: policy.ModeExplicit}, 25, "test")
	s.Aggregate(time.Now())
	out := scrape(t, s)

	for _, want := range []string{
		`keyshift_fleet_instances{state="active",} 4`,
		`keyshift_fleet_services{} 2`,
		`keyshift_fleet_local_hit_ratio{} 0.6`,
		`keyshift_fleet_service_instances{service="svc-a",} 2`,
		`keyshift_fleet_tracking_instances{state="healthy",} 4`,
		`keyshift_fleet_policy_instances{source="local",version="0",} 4`,
		`keyshift_controller_policy_rollout_percent{scope="svc-a",} 25`,
		`keyshift_controller_policy_mode{mode="explicit",scope="svc-a",} 1`,
		`keyshift_fleet_hotkey_instances{fp="` + fp(0) + `",key="product:1",state="admitted",} 4`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing series %s", want)
		}
	}
	// Top-N bound: 3 keys x 6 rate kinds.
	if n := strings.Count(out, "keyshift_fleet_hotkey_rate{"); n != 3*6 {
		t.Fatalf("hot-key series %d, want %d", n, 3*6)
	}
	// Keys without a display name are labelled by truncated fingerprint only.
	if !strings.Contains(out, `key="fp:`+fp(1)[:12]+`"`) {
		t.Fatal("fingerprint label missing")
	}
	// Per-instance series are opt-in.
	if strings.Contains(out, "keyshift_fleet_instance_rate") {
		t.Fatal("instance series must be disabled by default")
	}
	s.cfg.MetricsInstances = true
	if n := strings.Count(scrape(t, s), "keyshift_fleet_instance_hit_ratio{"); n != 4 {
		t.Fatalf("instance series %d, want 4", n)
	}
}
