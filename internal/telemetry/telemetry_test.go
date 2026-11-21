package telemetry

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Huy-8bit/keyshift/internal/policy"
)

func validBatch(n int) *Batch {
	now := time.Now()
	b := &Batch{SchemaVersion: SchemaVersion, Service: "svc", Instance: "pod-1", Seq: 1,
		WindowStart: now.Add(-30 * time.Second), WindowEnd: now, FingerprintScope: "shared",
		Tracking: Tracking{State: "healthy"}}
	for range n {
		b.Candidates = append(b.Candidates, Candidate{FP: strings.Repeat("ab", 16), State: "admitted", Reads: 10, HitRatio: 0.5})
	}
	return b
}

func TestValidate(t *testing.T) {
	if err := validBatch(3).Validate(time.Now()); err != nil {
		t.Fatal(err)
	}
	mut := map[string]func(*Batch){
		"schema":      func(b *Batch) { b.SchemaVersion = 99 },
		"service":     func(b *Batch) { b.Service = "bad name" },
		"instance":    func(b *Batch) { b.Instance = "" },
		"window":      func(b *Batch) { b.WindowEnd = b.WindowStart.Add(-time.Second) },
		"future":      func(b *Batch) { b.WindowStart, b.WindowEnd = time.Now().Add(time.Hour), time.Now().Add(2*time.Hour) },
		"scope":       func(b *Batch) { b.FingerprintScope = "global" },
		"fp":          func(b *Batch) { b.Candidates[0].FP = "raw:key" },
		"state":       func(b *Batch) { b.Candidates[0].State = "weird" },
		"hit":         func(b *Batch) { b.Candidates[0].HitRatio = 2 },
		"display":     func(b *Batch) { b.Candidates[0].Display = strings.Repeat("x", MaxDisplayLen+1) },
		"candidates":  func(b *Batch) { b.Candidates = validBatch(MaxCandidates + 1).Candidates },
		"long reason": func(b *Batch) { b.Tracking.Reason = strings.Repeat("x", MaxReasonLen+1) },
	}
	for name, f := range mut {
		b := validBatch(1)
		f(b)
		if err := b.Validate(time.Now()); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	for _, n := range []int{0, 150} {
		b := validBatch(n)
		body, gz, err := Encode(b)
		if err != nil {
			t.Fatal(err)
		}
		if n > 0 && !gz {
			t.Fatal("large batch should compress")
		}
		got, err := Decode(bytes.NewReader(body), gz)
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Candidates) != n || got.Instance != "pod-1" {
			t.Fatalf("round trip mismatch")
		}
	}
}

func TestDecodeZipBomb(t *testing.T) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	zw.Write([]byte(`{"service":"`))
	zw.Write(bytes.Repeat([]byte("a"), MaxDecompressedBytes+10))
	zw.Write([]byte(`"}`))
	zw.Close()
	if buf.Len() > MaxCompressedBytes {
		t.Skip("bomb not small enough for this test")
	}
	if _, err := Decode(&buf, true); err == nil || !strings.Contains(err.Error(), "too large") {
		t.Fatalf("expected size error, got %v", err)
	}
}

func FuzzDecode(f *testing.F) {
	body, _, _ := Encode(validBatch(2))
	f.Add(body, false)
	f.Add([]byte(`{"schema_version":1}`), false)
	f.Add([]byte{0x1f, 0x8b}, true)
	f.Fuzz(func(t *testing.T, data []byte, gz bool) {
		b, err := Decode(bytes.NewReader(data), gz)
		if err != nil {
			return
		}
		_ = b.Validate(time.Now()) // must not panic
	})
}

func TestReporterDeliversAndAppliesPolicy(t *testing.T) {
	var got atomic.Int32
	var auth, enc string
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		auth, enc = r.Header.Get("Authorization"), r.Header.Get("Content-Encoding")
		mu.Unlock()
		b, err := Decode(io.LimitReader(r.Body, MaxCompressedBytes), r.Header.Get("Content-Encoding") == "gzip")
		if err != nil || b.Validate(time.Now()) != nil {
			http.Error(w, "bad", 400)
			return
		}
		got.Add(1)
		json.NewEncoder(w).Encode(Response{Accepted: true, Policy: &policy.Envelope{
			Current: &policy.Policy{Version: 7, Mode: policy.ModeAdaptive}, RolloutPercent: 100}})
	}))
	defer srv.Close()
	var policyVersion atomic.Int64
	var contacts atomic.Int32
	r, err := NewReporter(ReporterConfig{Endpoint: srv.URL, Token: "secret", Interval: 10 * time.Millisecond,
		Build:     func() *Batch { return validBatch(100) },
		OnPolicy:  func(e *policy.Envelope) { policyVersion.Store(e.Current.Version) },
		OnContact: func() { contacts.Add(1) }})
	if err != nil {
		t.Fatal(err)
	}
	r.Start()
	deadline := time.Now().Add(3 * time.Second)
	// Wait on the client-side count: the server counts an upload before the
	// reporter has processed its response.
	for contacts.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	r.Stop()
	if got.Load() < 3 || policyVersion.Load() != 7 || contacts.Load() < 3 {
		t.Fatalf("delivered=%d policy=%d contacts=%d", got.Load(), policyVersion.Load(), contacts.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	if auth != "Bearer secret" || enc != "gzip" {
		t.Fatalf("headers: auth=%q enc=%q", auth, enc)
	}
	if s := r.Stats(); s.Sent < 3 || s.LastSuccess.IsZero() || s.BytesSent == 0 {
		t.Fatalf("stats %+v", s)
	}
}

func TestReporterControllerDownDropsBounded(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "down", http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	var builds atomic.Int32
	r, _ := NewReporter(ReporterConfig{Endpoint: srv.URL, Interval: 2 * time.Millisecond, QueueSize: 2,
		MaxAttempts: 2, MaxBackoff: 5 * time.Millisecond,
		Build: func() *Batch { builds.Add(1); return validBatch(1) }})
	r.Start()
	time.Sleep(300 * time.Millisecond)
	start := time.Now()
	r.Stop()
	if time.Since(start) > time.Second {
		t.Fatal("Stop blocked")
	}
	s := r.Stats()
	if s.Sent != 0 || (s.Failed+s.Dropped) == 0 {
		t.Fatalf("stats %+v", s)
	}
	if uint64(builds.Load()) > s.Failed+s.Dropped+uint64(r.cfg.QueueSize)+2 {
		t.Fatalf("batches unaccounted: builds=%d stats=%+v", builds.Load(), s)
	}
}

func TestReporterPermanentRejectNoRetry(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "nope", http.StatusUnauthorized)
	}))
	defer srv.Close()
	r, _ := NewReporter(ReporterConfig{Endpoint: srv.URL, Build: func() *Batch { return nil }})
	r.deliver(validBatch(1))
	if calls.Load() != 1 || r.Stats().Rejected != 1 {
		t.Fatalf("calls=%d stats=%+v", calls.Load(), r.Stats())
	}
}

func TestNewReporterValidation(t *testing.T) {
	for _, ep := range []string{"", "ftp://x", "http://"} {
		if _, err := NewReporter(ReporterConfig{Endpoint: ep, Build: func() *Batch { return nil }}); err == nil {
			t.Errorf("%q accepted", ep)
		}
	}
}

func TestFetchPolicy(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("service") != "svc" {
			http.NotFound(w, r)
			return
		}
		json.NewEncoder(w).Encode(policy.Envelope{Current: &policy.Policy{Version: 3, Mode: policy.ModeOff}, RolloutPercent: 100})
	}))
	defer srv.Close()
	r, _ := NewReporter(ReporterConfig{Endpoint: srv.URL, Service: "svc", Instance: "i", Build: func() *Batch { return nil }})
	env, err := r.FetchPolicy(t.Context())
	if err != nil || env.Current.Version != 3 {
		t.Fatalf("%v %+v", err, env)
	}
	r.cfg.Service = "other"
	if env, err := r.FetchPolicy(t.Context()); err != nil || env != nil {
		t.Fatalf("expected no policy, got %v %+v", err, env)
	}
}
