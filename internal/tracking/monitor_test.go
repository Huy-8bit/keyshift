package tracking

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestFailFastRecoverSlow(t *testing.T) {
	var fail atomic.Bool
	var mu sync.Mutex
	var changes []State
	m := NewMonitor(Config{
		Probe: func(context.Context) error {
			if fail.Load() {
				return errors.New("boom")
			}
			return nil
		},
		RecoverAfter: 3,
		OnChange: func(s Status) {
			mu.Lock()
			changes = append(changes, s.State)
			mu.Unlock()
		},
	})
	ctx := context.Background()
	m.CheckNow(ctx)
	if !m.Healthy() {
		t.Fatal("first success should be healthy")
	}
	fail.Store(true)
	m.CheckNow(ctx)
	if m.Healthy() || m.Status().ConsecutiveFailures != 1 || m.Status().Reason != "boom" {
		t.Fatalf("single failure must mark unhealthy: %+v", m.Status())
	}
	fail.Store(false)
	m.CheckNow(ctx)
	m.CheckNow(ctx)
	if m.Healthy() {
		t.Fatal("recovered too early")
	}
	m.CheckNow(ctx)
	if !m.Healthy() {
		t.Fatal("should recover after 3 successes")
	}
	m.ReportError(errors.New("conn reset"))
	if m.Healthy() {
		t.Fatal("reported error must mark unhealthy")
	}
	mu.Lock()
	defer mu.Unlock()
	want := []State{StateHealthy, StateUnhealthy, StateHealthy, StateUnhealthy}
	if len(changes) != len(want) {
		t.Fatalf("changes %v", changes)
	}
	for i := range want {
		if changes[i] != want[i] {
			t.Fatalf("changes %v", changes)
		}
	}
	if m.Status().Transitions != 4 || m.Status().Errors != 2 {
		t.Fatalf("counters %+v", m.Status())
	}
}

func TestProbeTimeout(t *testing.T) {
	m := NewMonitor(Config{Timeout: 20 * time.Millisecond, Probe: func(ctx context.Context) error {
		<-ctx.Done() // silent partition: the probe never answers
		return ctx.Err()
	}})
	start := time.Now()
	m.CheckNow(context.Background())
	if m.Healthy() || time.Since(start) > time.Second {
		t.Fatal("hung probe must time out and mark unhealthy")
	}
}

func TestProbeIgnoringContextStillTimesOut(t *testing.T) {
	release := make(chan struct{})
	m := NewMonitor(Config{Timeout: 20 * time.Millisecond, Probe: func(context.Context) error {
		<-release // ignores ctx, like a client without context-aware I/O
		return nil
	}})
	start := time.Now()
	m.CheckNow(context.Background())
	if m.Healthy() || time.Since(start) > 500*time.Millisecond {
		t.Fatal("probe ignoring ctx must still time out")
	}
	// While the first probe is stuck, further checks fail fast without
	// spawning more goroutines.
	m.CheckNow(context.Background())
	if s := m.Status(); s.ConsecutiveFailures != 2 || s.Reason != errProbePending.Error() {
		t.Fatalf("status %+v", s)
	}
	close(release)
	time.Sleep(10 * time.Millisecond)
	m.CheckNow(context.Background())
	m.CheckNow(context.Background())
	if !m.Healthy() {
		t.Fatalf("should recover once the probe answers: %+v", m.Status())
	}
}

func TestLoopStartStop(t *testing.T) {
	var n atomic.Int32
	m := NewMonitor(Config{Interval: 5 * time.Millisecond, Probe: func(context.Context) error { n.Add(1); return nil }})
	m.Start()
	time.Sleep(60 * time.Millisecond)
	m.Stop()
	got := n.Load()
	if got < 3 {
		t.Fatalf("probe ran %d times", got)
	}
	time.Sleep(20 * time.Millisecond)
	if n.Load() != got {
		t.Fatal("probe ran after Stop")
	}
	m.Stop() // idempotent
}

func TestFixed(t *testing.T) {
	m := Fixed(StateUnsupported, "no RESP3")
	m.Start()
	m.ReportError(errors.New("x"))
	if s := m.Status(); s.State != StateUnsupported || s.Reason != "no RESP3" {
		t.Fatalf("%+v", s)
	}
	m.Stop()
}
