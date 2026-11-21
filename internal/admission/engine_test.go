package admission

import (
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Huy-8bit/keyshift/internal/detector"
	"github.com/Huy-8bit/keyshift/internal/policy"
)

type harness struct {
	t        *testing.T
	e        *Engine
	now      time.Time
	demoted  []string
	suspends int
	mu       sync.Mutex
}

func newHarness(t *testing.T, local *policy.Policy) *harness {
	t.Helper()
	h := &harness{t: t}
	e, err := New(Config{
		Detector:     detector.Config{Capacity: 64, TopK: 16, SampleRate: 1},
		MaxStaleness: 10 * time.Second,
		Fingerprint:  func(k string) string { return strings.Repeat("0", 32-len(k)%32) + strings.Repeat("a", len(k)%32) },
		OnDemote: func(k string) {
			h.mu.Lock()
			h.demoted = append(h.demoted, k)
			h.mu.Unlock()
		},
		OnSuspend: func() { h.suspends++ },
	}, policy.Resolve(local, nil))
	if err != nil {
		t.Fatal(err)
	}
	e.SetHealthy(true)
	h.e = e
	h.now = time.Now()
	return h
}

// read simulates n reads of key, routed like the SDK does.
func (h *harness) read(key string, n int, size int64) {
	for range n {
		if ks := h.e.Lookup(key); ks != nil {
			ks.AddRead(size)
		} else {
			h.e.ObserveRead(key, size)
		}
	}
}

func (h *harness) rotate() WindowReport {
	h.now = h.now.Add(10 * time.Second)
	return h.e.Rotate(h.now)
}

func (h *harness) state(key string) string { return h.e.Explain(key).State }

func TestHotReadOnlyKeyIsAdmittedAfterObservationWindows(t *testing.T) {
	h := newHarness(t, nil)
	h.read("hot", 1000, 100) // 100 reads/s
	h.rotate()
	if got := h.state("hot"); got != "candidate" {
		t.Fatalf("after 1 window: %s (%s)", got, h.e.Explain("hot").Reason)
	}
	if h.e.Lookup("hot") != nil {
		t.Fatal("candidate must not be served from cache")
	}
	h.read("hot", 1000, 100)
	rep := h.rotate()
	if got := h.state("hot"); got != "admitted" {
		t.Fatalf("after 2 windows: %s (%s)", got, h.e.Explain("hot").Reason)
	}
	if h.e.Lookup("hot") == nil || rep.Admitted != 1 {
		t.Fatalf("expected admitted set to contain hot; report=%+v", rep)
	}
	d := h.e.Explain("hot")
	// The decayed estimate converges to the true rate from below.
	if d.HitRatioKind != "predicted" || d.HitRatio < 0.99 || d.ReadRate < 50 {
		t.Fatalf("unexpected decision %+v", d)
	}
}

func TestColdKeyIsNotAdmitted(t *testing.T) {
	h := newHarness(t, nil)
	for range 5 {
		h.read("cold", 5, 10) // 0.5 reads/s < 2
		h.rotate()
	}
	d := h.e.Explain("cold")
	if d.State != "cold" || !strings.Contains(d.Reason, "min_read_rate") {
		t.Fatalf("unexpected %+v", d)
	}
}

func TestWriteHeavyKeyIsRejected(t *testing.T) {
	h := newHarness(t, nil)
	for range 3 {
		h.read("counter", 1000, 8)
		for range 800 {
			h.e.ObserveWrite("counter")
		}
		h.rotate()
	}
	d := h.e.Explain("counter")
	if d.State != "cold" || !strings.Contains(d.Reason, "predicted hit ratio") {
		t.Fatalf("write-heavy key should be rejected: %+v", d)
	}
}

func admit(h *harness, key string, size int64) *KeyStats {
	h.t.Helper()
	for range 2 {
		h.read(key, 1000, size)
		h.rotate()
	}
	ks := h.e.Lookup(key)
	if ks == nil {
		h.t.Fatalf("%s not admitted: %+v", key, h.e.Explain(key))
	}
	return ks
}

func TestInvalidationChurnTriggersCooldownWithBackoff(t *testing.T) {
	h := newHarness(t, nil)
	ks := admit(h, "churn", 10)
	// 1000 reads, 900 fills: observed hit ratio 0.1 < 0.3.
	for range 1000 {
		ks.AddRead(10)
	}
	for range 900 {
		h.e.RecordFill("churn")
		h.e.RecordInvalidation("churn")
	}
	h.rotate()
	d := h.e.Explain("churn")
	if d.State != "cooldown" || d.Strikes != 1 || h.e.Lookup("churn") != nil {
		t.Fatalf("expected cooldown: %+v", d)
	}
	if len(h.demoted) != 1 || h.demoted[0] != "churn" {
		t.Fatalf("OnDemote not called: %v", h.demoted)
	}
	// Still hot, but barred during the 30s cooldown (3 windows of 10s).
	for range 2 {
		h.read("churn", 1000, 10)
		h.rotate()
		if h.state("churn") != "cooldown" {
			t.Fatalf("left cooldown early: %+v", h.e.Explain("churn"))
		}
	}
	// After cooldown it must re-qualify through observation windows.
	h.read("churn", 1000, 10)
	h.rotate()
	h.read("churn", 1000, 10)
	h.rotate()
	h.read("churn", 1000, 10)
	h.rotate()
	ks = h.e.Lookup("churn")
	if ks == nil {
		t.Fatalf("expected re-admission: %+v", h.e.Explain("churn"))
	}
	// Second strike doubles the cooldown.
	for range 1000 {
		ks.AddRead(10)
	}
	for range 900 {
		h.e.RecordFill("churn")
	}
	h.rotate()
	d = h.e.Explain("churn")
	until, _ := time.Parse(time.RFC3339, d.CooldownUntil)
	if d.Strikes != 2 || until.Sub(h.now) < 59*time.Second {
		t.Fatalf("expected doubled cooldown, got %+v (now %s)", d, h.now)
	}
}

func TestEffectiveCachedKeyStaysAdmitted(t *testing.T) {
	h := newHarness(t, nil)
	ks := admit(h, "good", 10)
	for range 5 {
		for range 1000 {
			ks.AddRead(10)
		}
		h.e.RecordFill("good")
		h.rotate()
		if h.e.Lookup("good") == nil {
			t.Fatalf("effective key demoted: %+v", h.e.Explain("good"))
		}
	}
	d := h.e.Explain("good")
	if d.HitRatioKind != "observed" || d.HitRatio < 0.99 {
		t.Fatalf("unexpected %+v", d)
	}
}

func TestHysteresisAndExitWindows(t *testing.T) {
	h := newHarness(t, nil)
	ks := admit(h, "k", 10)
	// 1.5 reads/s: below min_read_rate (2) but above exit (1) -> stays.
	for range 3 {
		for range 15 {
			ks.AddRead(10)
		}
		h.rotate()
		if h.e.Lookup("k") == nil {
			t.Fatalf("hysteresis violated: %+v", h.e.Explain("k"))
		}
	}
	// 0.5 reads/s: the smoothed rate decays below the exit threshold and
	// then must stay there for exit_windows (3) before demotion.
	demotedAt := -1
	for i := range 12 {
		ks := h.e.Lookup("k")
		if ks == nil {
			demotedAt = i
			break
		}
		for range 5 {
			ks.AddRead(10)
		}
		h.rotate()
	}
	if demotedAt < 3 || h.state("k") != "cold" {
		t.Fatalf("expected demotion after at least 3 cold windows, got window %d: %+v", demotedAt, h.e.Explain("k"))
	}
}

func TestEntryBudget(t *testing.T) {
	h := newHarness(t, &policy.Policy{Mode: policy.ModeAdaptive, Budget: policy.Budget{MaxEntries: 2}})
	for range 2 {
		for i := range 5 {
			h.read("k"+strconv.Itoa(i), 100*(i+1), 10)
		}
		h.rotate()
	}
	if n := h.e.AdmittedCount(); n != 2 {
		t.Fatalf("admitted %d, want 2", n)
	}
	// Highest benefit keys win.
	if h.e.Lookup("k4") == nil || h.e.Lookup("k3") == nil {
		t.Fatal("budget should favour the hottest keys")
	}
	if d := h.e.Explain("k0"); d.State != "candidate" || !strings.Contains(d.Reason, "budget") {
		t.Fatalf("unexpected %+v", d)
	}
	if h.e.Counters().BudgetRejected == 0 {
		t.Fatal("budget rejections not counted")
	}
}

// TestAdmittedSetGrowsBeyondTopK verifies that incumbents do not crowd new
// candidates out of the per-window top-K evaluation.
func TestAdmittedSetGrowsBeyondTopK(t *testing.T) {
	h := newHarness(t, nil) // TopK 16, capacity 64, budget 1000 entries
	for range 8 {
		for i := range 40 {
			h.read("k"+strconv.Itoa(i), 1000-i*10, 10)
		}
		h.rotate()
	}
	if n := h.e.AdmittedCount(); n < 40 {
		t.Fatalf("admitted %d keys, want all 40 qualifying keys despite TopK=16", n)
	}
}

func TestMemoryBudgetAndMaxEntryBytes(t *testing.T) {
	h := newHarness(t, &policy.Policy{Mode: policy.ModeAdaptive,
		Budget:    policy.Budget{MaxMemoryBytes: 3000},
		Admission: policy.Admission{MaxEntryBytes: 2000}})
	for range 2 {
		h.read("big", 1000, 5000)
		h.read("a", 900, 1000)
		h.read("b", 800, 1000)
		h.read("c", 700, 1000)
		h.rotate()
	}
	if d := h.e.Explain("big"); d.State != "bypassed" || !strings.Contains(d.Reason, "max_entry_bytes") {
		t.Fatalf("oversized key: %+v", d)
	}
	if n := h.e.AdmittedCount(); n != 2 {
		t.Fatalf("memory budget admitted %d keys, want 2", n)
	}
}

func TestPinsAndExplicitMode(t *testing.T) {
	h := newHarness(t, &policy.Policy{Mode: policy.ModeExplicit, PinPatterns: []string{"cfg:*"}})
	h.read("cfg:1", 1, 10) // a single read suffices for pinned keys
	h.read("hot", 1000, 10)
	h.rotate()
	if h.e.Lookup("cfg:1") == nil {
		t.Fatalf("pinned key not admitted: %+v", h.e.Explain("cfg:1"))
	}
	h.read("hot", 1000, 10)
	h.rotate()
	if d := h.e.Explain("hot"); d.State != "cold" || !strings.Contains(d.Reason, "explicit") {
		t.Fatalf("explicit mode admitted unpinned key: %+v", d)
	}
}

func TestPolicyChangeDemotesImmediately(t *testing.T) {
	h := newHarness(t, nil)
	admit(h, "balance:1", 10)
	admit(h, "product:1", 10)
	h.e.SetPolicy(policy.Resolve(nil, &policy.Policy{Version: 2, Mode: policy.ModeAdaptive, DenyPatterns: []string{"balance:*"}}), true)
	if h.e.Lookup("balance:1") != nil {
		t.Fatal("denied key still admitted")
	}
	if h.e.Lookup("product:1") == nil {
		t.Fatal("unrelated key demoted")
	}
	if d := h.e.Explain("balance:1"); d.State != "bypassed" || d.PolicyVersion != 2 {
		t.Fatalf("unexpected %+v", d)
	}
	// Deny by fingerprint.
	fp := h.e.cfg.Fingerprint("product:1")
	h.e.SetPolicy(policy.Resolve(nil, &policy.Policy{Version: 3, Mode: policy.ModeAdaptive, DenyFingerprints: []string{fp}}), true)
	if h.e.Lookup("product:1") != nil {
		t.Fatal("fingerprint deny not applied")
	}
	// Mode off and rollout exclusion.
	admit(h, "x", 10)
	h.e.SetPolicy(policy.Resolve(nil, &policy.Policy{Version: 4, Mode: policy.ModeOff}), true)
	if h.e.AdmittedCount() != 0 || h.state("x") != "disabled" {
		t.Fatalf("mode=off left keys admitted: %+v", h.e.Explain("x"))
	}
	h.e.SetPolicy(policy.Resolve(nil, nil), false)
	h.read("x", 1000, 10)
	h.rotate()
	h.read("x", 1000, 10)
	h.rotate()
	if h.e.AdmittedCount() != 0 {
		t.Fatal("instance outside rollout must not cache")
	}
	if len(h.demoted) < 3 {
		t.Fatalf("OnDemote calls missing: %v", h.demoted)
	}
}

func TestUnhealthySuspendsAndRecovers(t *testing.T) {
	h := newHarness(t, nil)
	admit(h, "k", 10)
	h.e.SetHealthy(false)
	if h.e.Lookup("k") != nil || h.suspends != 1 {
		t.Fatal("unhealthy tracking must stop cache reads and flush")
	}
	h.read("k", 1000, 10)
	h.rotate()
	if h.state("k") != "unhealthy" || h.e.AdmittedCount() != 0 {
		t.Fatalf("expected unhealthy: %+v", h.e.Explain("k"))
	}
	h.e.SetHealthy(true)
	h.read("k", 1000, 10)
	h.rotate()
	if h.e.Lookup("k") == nil {
		t.Fatalf("key should be re-admitted promptly after recovery: %+v", h.e.Explain("k"))
	}
}

func TestTableIsBounded(t *testing.T) {
	h := newHarness(t, nil)
	r := rand.New(rand.NewPCG(1, 2))
	for range 20 {
		for range 5000 {
			h.read("k"+strconv.Itoa(r.IntN(100000)), 1, 10)
		}
		h.read("hot", 500, 10)
		h.rotate()
		if n := h.e.TableSize(); n > 64 {
			t.Fatalf("table grew to %d", n)
		}
	}
	if h.e.Lookup("hot") == nil {
		t.Fatal("hot key lost in high-cardinality noise")
	}
}

func TestReportContents(t *testing.T) {
	h := newHarness(t, nil)
	ks := admit(h, "hot", 10)
	for range 100 {
		ks.AddRead(10)
	}
	h.e.RecordFill("hot")
	h.read("other", 50, 10)
	h.e.ObserveWrite("w")
	rep := h.rotate()
	if rep.CachedReads != 100 || rep.Fills != 1 || rep.Writes != 1 || rep.Reads < 150 {
		t.Fatalf("unexpected report %+v", rep)
	}
	if len(rep.Keys) == 0 || rep.Keys[0].Key != "hot" || rep.Keys[0].State != Admitted {
		t.Fatalf("unexpected keys %+v", rep.Keys)
	}
}

func TestConcurrentRequestPathRotationAndPolicyUpdates(t *testing.T) {
	h := newHarness(t, nil)
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for g := range 8 {
		wg.Go(func() {
			r := rand.New(rand.NewPCG(uint64(g), 9))
			for {
				select {
				case <-stop:
					return
				default:
				}
				key := "k" + strconv.Itoa(int(r.ExpFloat64()*3))
				if ks := h.e.Lookup(key); ks != nil {
					ks.AddRead(10)
					if r.IntN(50) == 0 {
						h.e.RecordFill(key)
					}
				} else {
					h.e.ObserveRead(key, 10)
				}
				if r.IntN(100) == 0 {
					h.e.ObserveWrite(key)
				}
			}
		})
	}
	wg.Go(func() {
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			mode := policy.ModeAdaptive
			if i%7 == 0 {
				mode = policy.ModeOff
			}
			h.e.SetPolicy(policy.Resolve(nil, &policy.Policy{Version: int64(i), Mode: mode}), i%5 != 0)
			h.e.SetHealthy(i%11 != 0)
			_ = h.e.Explain("k0")
			time.Sleep(100 * time.Microsecond)
		}
	})
	for range 200 {
		h.mu.Lock()
		h.now = h.now.Add(10 * time.Second)
		now := h.now
		h.mu.Unlock()
		h.e.Rotate(now)
	}
	close(stop)
	wg.Wait()
}

func TestStateNames(t *testing.T) {
	for s := Cold; s <= Unhealthy; s++ {
		got, ok := ParseState(s.String())
		if !ok || got != s {
			t.Fatalf("round trip failed for %v", s)
		}
	}
	if State(99).String() != "unknown" {
		t.Fatal("unknown state name")
	}
}

func BenchmarkLookupAdmitted(b *testing.B) {
	e, _ := New(Config{Detector: detector.Config{Capacity: 1000, TopK: 100, SampleRate: 1}, MaxStaleness: time.Second}, nil)
	e.SetHealthy(true)
	now := time.Now()
	for range 2 {
		for i := range 100 {
			for range 100 {
				e.ObserveRead("key:"+strconv.Itoa(i), 100)
			}
		}
		now = now.Add(time.Second)
		e.Rotate(now)
	}
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if ks := e.Lookup("key:" + strconv.Itoa(i%100)); ks != nil {
				ks.AddRead(100)
			}
			i++
		}
	})
}
