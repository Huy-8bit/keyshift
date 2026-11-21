package controller

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Huy-8bit/keyshift/internal/policy"
	"github.com/Huy-8bit/keyshift/internal/telemetry"
)

const (
	ingestTok = "ingest-token-0123456789"
	viewerTok = "viewer-token-0123456789"
	adminTok  = "admin-token-0123456789"
)

func newTestServer(t *testing.T, mut ...func(*Config)) (*Server, *httptest.Server) {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Auth = NewAuth([]string{ingestTok}, []string{viewerTok}, []string{adminTok})
	cfg.PerInstanceMinInterval = 0
	cfg.DataDir = t.TempDir()
	for _, m := range mut {
		m(&cfg)
	}
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	return s, ts
}

func fp(i int) string { return fmt.Sprintf("%032x", i) }

func batch(service, instance string, seq uint64, cands ...telemetry.Candidate) *telemetry.Batch {
	now := time.Now()
	return &telemetry.Batch{SchemaVersion: 1, Service: service, Instance: instance, Seq: seq,
		WindowStart: now.Add(-10 * time.Second), WindowEnd: now, FingerprintScope: "shared", FingerprintKeyID: "k1",
		Tracking: telemetry.Tracking{State: "healthy"}, PolicySource: "local",
		Totals:     telemetry.Totals{Reads: 1000, LocalHits: 600, RedisReads: 400, Writes: 10},
		Cache:      telemetry.Cache{Entries: 5, MemoryBytes: 1000, MaxMemoryBytes: 1 << 20, Admitted: 2},
		Candidates: cands}
}

func do(t *testing.T, ts *httptest.Server, method, path, token string, body any, hdr ...string) (*http.Response, []byte) {
	t.Helper()
	var rd io.Reader
	switch b := body.(type) {
	case nil:
	case []byte:
		rd = bytes.NewReader(b)
	default:
		raw, _ := json.Marshal(b)
		rd = bytes.NewReader(raw)
	}
	req, _ := http.NewRequest(method, ts.URL+path, rd)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return resp, out
}

func sendWith(t *testing.T, ts *httptest.Server, tok string, b *telemetry.Batch) (int, telemetry.Response) {
	t.Helper()
	body, gz, _ := telemetry.Encode(b)
	var hdr []string
	if gz {
		hdr = append(hdr, "Content-Encoding", "gzip")
	}
	resp, out := do(t, ts, "POST", "/v1/telemetry", tok, body, hdr...)
	var r telemetry.Response
	json.Unmarshal(out, &r)
	return resp.StatusCode, r
}

func send(t *testing.T, ts *httptest.Server, b *telemetry.Batch, hdr ...string) (int, telemetry.Response) {
	t.Helper()
	body, gz, _ := telemetry.Encode(b)
	if gz {
		hdr = append(hdr, "Content-Encoding", "gzip")
	}
	resp, out := do(t, ts, "POST", "/v1/telemetry", ingestTok, body, hdr...)
	var r telemetry.Response
	json.Unmarshal(out, &r)
	return resp.StatusCode, r
}

func TestAuthRoles(t *testing.T) {
	_, ts := newTestServer(t)
	cases := []struct {
		method, path, token string
		want                int
	}{
		{"GET", "/api/v1/overview", "", 401},
		{"GET", "/api/v1/overview", "wrong", 401},
		{"GET", "/api/v1/overview", ingestTok, 403},
		{"GET", "/api/v1/overview", viewerTok, 200},
		{"GET", "/api/v1/overview", adminTok, 200},
		{"POST", "/api/v1/policies/_default/rollback", viewerTok, 403},
		{"POST", "/v1/telemetry", viewerTok, 403},
		{"GET", "/healthz", "", 200},
		{"GET", "/readyz", "", 200},
		{"GET", "/metrics", "", 200},
	}
	for _, c := range cases {
		resp, _ := do(t, ts, c.method, c.path, c.token, nil)
		if resp.StatusCode != c.want {
			t.Errorf("%s %s token=%q: got %d want %d", c.method, c.path, c.token, resp.StatusCode, c.want)
		}
	}
	// A token listed under several roles holds all of them.
	_, multi := newTestServer(t, func(c *Config) {
		c.Auth = NewAuth([]string{"multi-role-token-0123456789"}, []string{"multi-role-token-0123456789"}, nil)
	})
	if code, _ := sendWith(t, multi, "multi-role-token-0123456789", batch("svc", "p", 1)); code != 200 {
		t.Fatalf("ingest+viewer token could not ingest: %d", code)
	}
	if resp, _ := do(t, multi, "GET", "/api/v1/overview", "multi-role-token-0123456789", nil); resp.StatusCode != 200 {
		t.Fatalf("ingest+viewer token could not read: %d", resp.StatusCode)
	}
	if resp, _ := do(t, multi, "POST", "/api/v1/policies/_default/rollback", "multi-role-token-0123456789", nil); resp.StatusCode != 403 {
		t.Fatalf("ingest+viewer token must not administer: %d", resp.StatusCode)
	}
	_, body := do(t, ts, "GET", "/api/v1/whoami", adminTok, nil)
	if !strings.Contains(string(body), `"admin"`) {
		t.Fatalf("whoami: %s", body)
	}
}

func TestIngestValidationAndIdempotency(t *testing.T) {
	s, ts := newTestServer(t)
	b := batch("svc", "pod-1", 1, telemetry.Candidate{FP: fp(1), Reads: 100, State: "admitted"})
	if code, r := send(t, ts, b); code != 200 || !r.Accepted || r.Duplicate {
		t.Fatalf("first upload: %d %+v", code, r)
	}
	if code, r := send(t, ts, b); code != 200 || !r.Duplicate {
		t.Fatalf("retry must be acknowledged as duplicate: %d %+v", code, r)
	}
	bad := batch("svc", "pod-1", 2, telemetry.Candidate{FP: "user:123", State: "admitted"})
	if code, _ := send(t, ts, bad); code != 400 {
		t.Fatalf("raw key as fingerprint accepted: %d", code)
	}
	resp, _ := do(t, ts, "POST", "/v1/telemetry", ingestTok, []byte("{not json"))
	if resp.StatusCode != 400 {
		t.Fatalf("garbage: %d", resp.StatusCode)
	}
	big := bytes.Repeat([]byte("a"), telemetry.MaxCompressedBytes+10)
	resp, _ = do(t, ts, "POST", "/v1/telemetry", ingestTok, big)
	if resp.StatusCode != 413 && resp.StatusCode != 400 {
		t.Fatalf("oversized: %d", resp.StatusCode)
	}
	resp, _ = do(t, ts, "POST", "/v1/telemetry", ingestTok, []byte("{}"), "Content-Encoding", "br")
	if resp.StatusCode != 415 {
		t.Fatalf("unsupported encoding: %d", resp.StatusCode)
	}
	if s.accepted.Load() != 1 || s.duplicate.Load() != 1 || s.rejected.Load() != 4 {
		t.Fatalf("counters accepted=%d dup=%d rejected=%d", s.accepted.Load(), s.duplicate.Load(), s.rejected.Load())
	}
}

func TestIngestRateLimitsAndCapacity(t *testing.T) {
	_, ts := newTestServer(t, func(c *Config) {
		c.MaxInstances = 2
		c.PerInstanceMinInterval = time.Hour
	})
	send(t, ts, batch("svc", "a", 1))
	if code, _ := send(t, ts, batch("svc", "a", 2)); code != 429 {
		t.Fatalf("per-instance rate limit: %d", code)
	}
	send(t, ts, batch("svc", "b", 1))
	if code, _ := send(t, ts, batch("svc", "c", 1)); code != 503 {
		t.Fatalf("capacity: %d", code)
	}
	_, ts2 := newTestServer(t, func(c *Config) { c.IngestRatePerSecond, c.IngestBurst = 1, 1 })
	send(t, ts2, batch("svc", "a", 1))
	body, _, _ := telemetry.Encode(batch("svc", "a", 2))
	resp, _ := do(t, ts2, "POST", "/v1/telemetry", ingestTok, body)
	if resp.StatusCode != 429 || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("global rate limit: %d %q", resp.StatusCode, resp.Header.Get("Retry-After"))
	}
}

func TestAggregationGlobalVersusLocal(t *testing.T) {
	s, ts := newTestServer(t)
	// fp1: read evenly by 10 instances of two services -> global.
	// fp2: hot on one instance only -> local.
	for i := range 10 {
		svc := "svc-a"
		if i >= 5 {
			svc = "svc-b"
		}
		cands := []telemetry.Candidate{{FP: fp(1), Reads: 1000, State: "admitted", Fills: 10, SizeBytes: 100, SavedRate: 99}}
		if i == 0 {
			cands = append(cands, telemetry.Candidate{FP: fp(2), Reads: 5000, State: "admitted", SizeBytes: 50, SavedRate: 499, Display: "config:flags"})
		}
		cands = append(cands, telemetry.Candidate{FP: fp(3), Reads: 100, Writes: 90, State: "cold"})
		if code, _ := send(t, ts, batch(svc, "pod-"+strconv.Itoa(i), 1, cands...)); code != 200 {
			t.Fatal("ingest failed")
		}
	}
	v := s.Aggregate(time.Now())
	ov := v.Overview
	if ov.ActiveInstances != 10 || ov.Services != 2 || ov.Rates.Reads != 1000 || ov.LocalHitRatio != 0.6 {
		t.Fatalf("overview %+v", ov)
	}
	keys := map[string]KeyView{}
	for _, k := range v.Keys {
		keys[k.FP] = k
	}
	k1, k2, k3 := keys[fp(1)], keys[fp(2)], keys[fp(3)]
	if k1.Instances != 10 || k1.Classification != "global" || len(k1.Services) != 2 || k1.ReadRate != 1000 {
		t.Fatalf("k1 %+v", k1)
	}
	if k1.CachedHitRatio < 0.98 || k1.Recommendation != "cached on every reporting instance" {
		t.Fatalf("k1 cache %+v", k1)
	}
	if k2.Classification != "single-instance" || k2.Display != "config:flags" || k2.LocalShare != 1 {
		t.Fatalf("k2 %+v", k2)
	}
	if !strings.Contains(k3.Recommendation, "write-heavy") {
		t.Fatalf("k3 %+v", k3)
	}
	// Ranking is by global read rate.
	if v.Keys[0].FP != fp(1) {
		t.Fatalf("ranking: %s first", v.Keys[0].FP)
	}
	// API surfaces.
	_, body := do(t, ts, "GET", "/api/v1/hotkeys?classification=global&limit=5", viewerTok, nil)
	var hk []KeyView
	json.Unmarshal(body, &hk)
	if len(hk) != 2 || hk[0].FP != fp(1) || hk[1].FP != fp(3) {
		t.Fatalf("hotkeys filter: %s", body)
	}
	_, body = do(t, ts, "GET", "/api/v1/hotkeys/"+fp(2), viewerTok, nil)
	if !strings.Contains(string(body), "config:flags") {
		t.Fatalf("hotkey detail: %s", body)
	}
	_, body = do(t, ts, "GET", "/api/v1/instances?service=svc-b&limit=2", viewerTok, nil)
	var inst struct {
		Total int            `json:"total"`
		Items []InstanceView `json:"items"`
	}
	json.Unmarshal(body, &inst)
	if inst.Total != 5 || len(inst.Items) != 2 {
		t.Fatalf("instances: %s", body)
	}
	for _, p := range []string{"/api/v1/services", "/api/v1/cache", "/api/v1/timeseries", "/api/v1/overview"} {
		if resp, _ := do(t, ts, "GET", p, viewerTok, nil); resp.StatusCode != 200 {
			t.Fatalf("%s: %d", p, resp.StatusCode)
		}
	}
}

func TestStaleInstancesAndWarnings(t *testing.T) {
	s, ts := newTestServer(t, func(c *Config) { c.FreshFor = time.Minute; c.InstanceTTL = 2 * time.Minute })
	b := batch("svc", "pod-1", 1)
	b.Tracking.State = "unhealthy"
	send(t, ts, b)
	b2 := batch("svc", "pod-2", 1)
	b2.FingerprintScope = "instance"
	send(t, ts, b2)
	v := s.Aggregate(time.Now())
	codes := map[string]bool{}
	for _, w := range v.Overview.Warnings {
		codes[w.Code] = true
	}
	if !codes["tracking_unhealthy"] || !codes["instance_fingerprints"] {
		t.Fatalf("warnings %+v", v.Overview.Warnings)
	}
	v = s.Aggregate(time.Now().Add(90 * time.Second))
	if v.Overview.StaleInstances != 2 || v.Overview.ActiveInstances != 0 {
		t.Fatalf("staleness %+v", v.Overview)
	}
	if n := s.registry.Sweep(time.Now().Add(3 * time.Minute)); n != 2 || s.registry.Len() != 0 {
		t.Fatalf("sweep removed %d", n)
	}
}

func TestPolicyLifecycleAndDistribution(t *testing.T) {
	s, ts := newTestServer(t)
	put := func(scope string, p policy.Policy, rollout int) (int, policy.Envelope) {
		resp, body := do(t, ts, "PUT", "/api/v1/policies/"+scope, adminTok, map[string]any{"policy": p, "rollout_percent": rollout})
		var env policy.Envelope
		json.Unmarshal(body, &env)
		return resp.StatusCode, env
	}
	if code, _ := put("_default", policy.Policy{Mode: "bogus"}, 100); code != 400 {
		t.Fatal("invalid policy accepted")
	}
	code, env := put("_default", policy.Policy{Mode: policy.ModeAdaptive, DenyPatterns: []string{"lock:*"}}, 100)
	if code != 200 || env.Current.Version != 1 {
		t.Fatalf("put default: %d %+v", code, env)
	}
	code, env = put("svc", policy.Policy{Mode: policy.ModeExplicit, PinPatterns: []string{"cfg:*"}}, 100)
	if code != 200 || env.Current.Version != 2 || env.Current.Service != "svc" {
		t.Fatalf("put svc: %+v", env)
	}
	// Telemetry from svc receives its service policy; other services get the default.
	_, r := send(t, ts, batch("svc", "pod-1", 1))
	if r.Policy == nil || r.Policy.Current.Version != 2 || r.PolicyVersion != 2 {
		t.Fatalf("svc did not receive policy: %+v", r)
	}
	_, r = send(t, ts, batch("svc", "pod-1", 2), "X-KeyShift-Policy-Version", "2")
	if r.Policy != nil {
		t.Fatal("unchanged policy must not be resent")
	}
	_, r = send(t, ts, batch("other", "pod-9", 1))
	if r.Policy == nil || r.Policy.Current.Version != 1 || r.NoPolicy {
		t.Fatalf("default policy not distributed: %+v", r)
	}
	// Staged rollout keeps the previous policy as baseline.
	code, env = put("svc", policy.Policy{Mode: policy.ModeOff}, 10)
	if code != 200 || env.Previous == nil || env.Previous.Version != 2 || env.RolloutPercent != 10 {
		t.Fatalf("staged put: %+v", env)
	}
	resp, body := do(t, ts, "POST", "/api/v1/policies/svc/rollout", adminTok, map[string]int{"rollout_percent": 50})
	if resp.StatusCode != 200 || !strings.Contains(string(body), `"rollout_percent":50`) {
		t.Fatalf("rollout: %d %s", resp.StatusCode, body)
	}
	resp, body = do(t, ts, "POST", "/api/v1/policies/svc/rollback", adminTok, nil)
	if resp.StatusCode != 200 || !strings.Contains(string(body), `"version":2`) {
		t.Fatalf("rollback: %d %s", resp.StatusCode, body)
	}
	resp, _ = do(t, ts, "POST", "/api/v1/policies/svc/disable", adminTok, nil)
	if resp.StatusCode != 200 || s.policies.For("svc").Current.Mode != policy.ModeOff {
		t.Fatal("disable failed")
	}
	resp, _ = do(t, ts, "GET", "/v1/policy?service=svc", ingestTok, nil)
	if resp.StatusCode != 200 {
		t.Fatalf("policy fetch: %d", resp.StatusCode)
	}
	resp, _ = do(t, ts, "DELETE", "/api/v1/policies/svc", adminTok, nil)
	if resp.StatusCode != 204 || s.policies.For("svc").Current.Version != 1 {
		t.Fatal("delete should fall back to default")
	}
	resp, _ = do(t, ts, "DELETE", "/api/v1/policies/_default", adminTok, nil)
	if resp.StatusCode != 204 {
		t.Fatal("delete default")
	}
	if _, r := send(t, ts, batch("svc", "pod-1", 10)); !r.NoPolicy || r.Policy != nil {
		t.Fatalf("with no policy left the response must say so: %+v", r)
	}
	_, body = do(t, ts, "GET", "/api/v1/policies/history", viewerTok, nil)
	var hist []HistoryEntry
	json.Unmarshal(body, &hist)
	if len(hist) < 7 || hist[0].Action != "delete" {
		t.Fatalf("history: %s", body)
	}
	// Persistence across restarts.
	s2, err := New(Config{Auth: s.cfg.Auth, DataDir: s.cfg.DataDir, MaxInstances: 10, MaxServices: 10,
		MaxCandidatesPerBatch: 10, MaxGlobalKeys: 10, MaxPolicyScopes: 10, AggregateInterval: time.Second,
		HistoryPoints: 10, FreshFor: time.Minute, InstanceTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	// Every policy was deleted above: the restart must keep that state, the
	// history, and the version counter.
	if s2.policies.For("svc") != nil || len(s2.policies.History("")) < 7 {
		t.Fatal("policy state or history not persisted")
	}
	if p, _ := s2.policies.Put(&policy.Policy{Mode: policy.ModeAdaptive}, 100, "t"); p.Current.Version <= 4 {
		t.Fatalf("version counter not persisted: %d", p.Current.Version)
	}
}

func TestPolicyPersistFailureIsServerErrorAndRolledBack(t *testing.T) {
	s, ts := newTestServer(t)
	dir := s.cfg.DataDir
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(dir, 0o700)
	resp, _ := do(t, ts, "PUT", "/api/v1/policies/_default", adminTok, map[string]any{"policy": policy.Policy{Mode: policy.ModeOff}})
	if resp.StatusCode != 500 {
		t.Fatalf("persist failure returned %d, want 500", resp.StatusCode)
	}
	if s.policies.For("svc") != nil {
		t.Fatal("failed change must be rolled back")
	}
}

func TestPolicyScopeLimit(t *testing.T) {
	_, ts := newTestServer(t, func(c *Config) { c.MaxPolicyScopes = 2 })
	for i, want := range []int{200, 200, 400} {
		resp, _ := do(t, ts, "PUT", "/api/v1/policies/svc"+strconv.Itoa(i), adminTok,
			map[string]any{"policy": policy.Policy{Mode: policy.ModeAdaptive}})
		if resp.StatusCode != want {
			t.Fatalf("scope %d: %d", i, resp.StatusCode)
		}
	}
}

func TestConcurrentIngestAndAggregation(t *testing.T) {
	s, ts := newTestServer(t)
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for i := range 25 {
				b := batch("svc-"+strconv.Itoa(g%3), "pod-"+strconv.Itoa(g*100+i), 1,
					telemetry.Candidate{FP: fp(i % 7), Reads: 10, State: "candidate"})
				body, _, _ := telemetry.Encode(b)
				req, _ := http.NewRequest("POST", ts.URL+"/v1/telemetry", bytes.NewReader(body))
				req.Header.Set("Authorization", "Bearer "+ingestTok)
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Error(err)
					return
				}
				resp.Body.Close()
			}
		})
	}
	wg.Go(func() {
		for range 50 {
			s.Aggregate(time.Now())
		}
	})
	wg.Wait()
	v := s.Aggregate(time.Now())
	if v.Overview.ActiveInstances != 200 || len(v.Keys) != 7 {
		t.Fatalf("instances=%d keys=%d", v.Overview.ActiveInstances, len(v.Keys))
	}
}

func TestGlobalKeysBounded(t *testing.T) {
	s, ts := newTestServer(t, func(c *Config) { c.MaxGlobalKeys = 25 })
	for i := range 20 {
		var cands []telemetry.Candidate
		for j := range 10 {
			cands = append(cands, telemetry.Candidate{FP: fp(i*10 + j), Reads: uint64(j + 1), State: "cold"})
		}
		send(t, ts, batch("svc", "pod-"+strconv.Itoa(i), 1, cands...))
	}
	if v := s.Aggregate(time.Now()); len(v.Keys) != 25 {
		t.Fatalf("global keys %d", len(v.Keys))
	}
}

func TestTokenBucket(t *testing.T) {
	b := newTokenBucket(10, 2)
	now := time.Now()
	ok1, _ := b.allow(now)
	ok2, _ := b.allow(now)
	ok3, wait := b.allow(now)
	if !ok1 || !ok2 || ok3 || wait <= 0 {
		t.Fatal("burst handling")
	}
	if ok, _ := b.allow(now.Add(200 * time.Millisecond)); !ok {
		t.Fatal("refill")
	}
}
