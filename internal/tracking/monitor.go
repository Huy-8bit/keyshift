// Package tracking monitors the health of Redis client tracking on the
// cache path and decides when local caching must be suspended.
package tracking

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

// State is the tracking health state.
type State string

const (
	StateUnknown     State = "unknown"
	StateHealthy     State = "healthy"
	StateUnhealthy   State = "unhealthy"
	StateUnsupported State = "unsupported"
	StateDisabled    State = "disabled"
)

// Status is a point-in-time health report.
type Status struct {
	State               State     `json:"state"`
	Reason              string    `json:"reason,omitempty"`
	Since               time.Time `json:"since"`
	LastCheck           time.Time `json:"last_check"`
	ConsecutiveFailures int       `json:"consecutive_failures"`
	Transitions         uint64    `json:"transitions"`
	Errors              uint64    `json:"errors"`
}

// Config configures a Monitor.
type Config struct {
	// Probe verifies tracking. It must honor ctx.
	Probe func(ctx context.Context) error
	// Interval between probes. Default 1s.
	Interval time.Duration
	// Timeout per probe. Default 500ms.
	Timeout time.Duration
	// RecoverAfter is the number of consecutive successful probes required
	// to leave the unhealthy state. Default 2.
	RecoverAfter int
	// OnChange is called on every state transition, from the monitor
	// goroutine or from ReportError's caller.
	OnChange func(Status)
}

// Monitor runs periodic probes. A single failed probe (or a reported
// cache-path transport error) marks tracking unhealthy immediately; recovery
// requires RecoverAfter consecutive successes. Failing fast is deliberate:
// serving from the cache while tracking is uncertain risks stale reads,
// while bypassing the cache only costs extra Redis reads.
type Monitor struct {
	cfg Config

	mu        sync.Mutex
	st        Status
	successes int
	errors    atomic.Uint64
	inflight  atomic.Bool

	stop chan struct{}
	done chan struct{}
	once sync.Once
}

// NewMonitor creates a monitor in the unknown state.
func NewMonitor(cfg Config) *Monitor {
	if cfg.Interval <= 0 {
		cfg.Interval = time.Second
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 500 * time.Millisecond
	}
	if cfg.RecoverAfter <= 0 {
		cfg.RecoverAfter = 2
	}
	return &Monitor{cfg: cfg, st: Status{State: StateUnknown, Since: time.Now()},
		stop: make(chan struct{}), done: make(chan struct{})}
}

// Fixed creates a monitor permanently in a terminal state (unsupported or
// disabled). Start is a no-op for it.
func Fixed(state State, reason string) *Monitor {
	m := NewMonitor(Config{})
	m.st = Status{State: state, Reason: reason, Since: time.Now()}
	close(m.done)
	m.once.Do(func() {})
	return m
}

// Status returns the current status.
func (m *Monitor) Status() Status {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.st
	s.Errors = m.errors.Load()
	return s
}

// Healthy reports whether caching may use tracked entries.
func (m *Monitor) Healthy() bool { return m.Status().State == StateHealthy }

// errProbePending is reported when the previous probe has not returned.
var errProbePending = errors.New("previous tracking probe is still pending")

// CheckNow runs one probe and waits at most Timeout for it. The timeout is
// enforced here rather than trusted to the probe: some clients do not apply
// context deadlines to socket I/O by default (go-redis without
// ContextTimeoutEnabled), and a silently partitioned connection would
// otherwise stall the monitor while stale entries keep being served. At most
// one probe is in flight; a probe that is still pending counts as a failure.
func (m *Monitor) CheckNow(ctx context.Context) {
	if !m.inflight.CompareAndSwap(false, true) {
		m.record(errProbePending)
		return
	}
	ctx, cancel := context.WithTimeout(ctx, m.cfg.Timeout)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		err := m.cfg.Probe(ctx)
		// Clear the flag before publishing the result: once CheckNow has
		// received it, the next CheckNow must be able to start a probe.
		m.inflight.Store(false)
		done <- err
	}()
	select {
	case err := <-done:
		m.record(err)
	case <-ctx.Done():
		m.record(fmt.Errorf("tracking probe did not answer within %s", m.cfg.Timeout))
	}
}

// Start runs the probe loop until Stop.
func (m *Monitor) Start() {
	m.once.Do(func() {
		go m.loop()
	})
}

func (m *Monitor) loop() {
	defer close(m.done)
	for {
		// ±10% jitter avoids synchronized probes across thousands of pods.
		d := time.Duration(float64(m.cfg.Interval) * (0.9 + 0.2*rand.Float64()))
		t := time.NewTimer(d)
		select {
		case <-m.stop:
			t.Stop()
			return
		case <-t.C:
		}
		m.CheckNow(context.Background())
	}
}

// Stop terminates the probe loop and waits for it to exit.
func (m *Monitor) Stop() {
	select {
	case <-m.stop:
	default:
		close(m.stop)
	}
	m.once.Do(func() { close(m.done) })
	<-m.done
}

// ReportError records a transport error observed on the cache path. It
// marks tracking unhealthy immediately.
func (m *Monitor) ReportError(err error) {
	if err == nil {
		return
	}
	m.record(err)
}

func (m *Monitor) record(err error) {
	m.mu.Lock()
	prev := m.st.State
	if prev == StateUnsupported || prev == StateDisabled {
		m.mu.Unlock()
		return
	}
	m.st.LastCheck = time.Now()
	if err != nil {
		m.errors.Add(1)
		m.successes = 0
		m.st.ConsecutiveFailures++
		m.st.Reason = err.Error()
		m.st.State = StateUnhealthy
	} else {
		m.st.ConsecutiveFailures = 0
		m.successes++
		if prev == StateUnknown || (prev == StateUnhealthy && m.successes >= m.cfg.RecoverAfter) {
			m.st.State = StateHealthy
			m.st.Reason = ""
		}
	}
	changed := m.st.State != prev
	if changed {
		m.st.Since = m.st.LastCheck
		m.st.Transitions++
	}
	st := m.st
	st.Errors = m.errors.Load()
	m.mu.Unlock()
	if changed && m.cfg.OnChange != nil {
		m.cfg.OnChange(st)
	}
}
