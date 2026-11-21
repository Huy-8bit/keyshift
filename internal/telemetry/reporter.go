package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Huy-8bit/keyshift/internal/policy"
)

// ReporterConfig configures a Reporter.
type ReporterConfig struct {
	// Endpoint is the controller base URL, e.g. https://keyshift:8080.
	Endpoint string
	// Token is sent as a bearer token.
	Token string
	// Interval between batches. Default 30s. Each wait is jittered ±20%.
	Interval time.Duration
	// QueueSize bounds batches waiting to be sent. Default 4. When full the
	// oldest batch is dropped.
	QueueSize int
	// MaxAttempts per batch. Default 3.
	MaxAttempts int
	// MaxBackoff caps retry backoff. Default 30s.
	MaxBackoff time.Duration
	// HTTPClient is used for requests. Default has a 10s timeout.
	HTTPClient *http.Client
	// Build produces the next batch, or nil to skip.
	Build func() *Batch
	// OnPolicy receives a policy envelope from the controller.
	OnPolicy func(env *policy.Envelope)
	// OnContact is called after every successful exchange with the
	// controller (used for policy staleness tracking).
	OnContact func()
	// PolicyVersion returns the policy version the SDK currently enforces.
	PolicyVersion func() int64
	Logger        *slog.Logger
	// Service and Instance identify the SDK for policy fetches.
	Service, Instance string
}

// ReporterStats are cumulative reporter counters.
type ReporterStats struct {
	Sent, Failed, Dropped, Rejected uint64
	LastSuccess                     time.Time
	BytesSent                       uint64
}

// Reporter periodically sends batches. It never blocks the caller of Build
// and never retries forever.
type Reporter struct {
	cfg   ReporterConfig
	queue chan *Batch

	sent, failed, dropped, rejected, bytes atomic.Uint64
	lastSuccess                            atomic.Int64

	stop chan struct{}
	wg   sync.WaitGroup
	once sync.Once
}

// NewReporter validates cfg and returns a reporter.
func NewReporter(cfg ReporterConfig) (*Reporter, error) {
	u, err := url.Parse(cfg.Endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("telemetry: invalid endpoint %q", cfg.Endpoint)
	}
	if cfg.Build == nil {
		return nil, errors.New("telemetry: Build is required")
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 30 * time.Second
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 4
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 3
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 30 * time.Second
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 10 * time.Second}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	return &Reporter{cfg: cfg, queue: make(chan *Batch, cfg.QueueSize), stop: make(chan struct{})}, nil
}

// Start launches the producer and sender goroutines.
func (r *Reporter) Start() {
	r.wg.Add(2)
	go r.produce()
	go r.send()
}

// Stop terminates both goroutines and waits for them. Pending batches are
// discarded (counted as dropped).
func (r *Reporter) Stop() {
	r.once.Do(func() { close(r.stop) })
	r.wg.Wait()
	for {
		select {
		case <-r.queue:
			r.dropped.Add(1)
		default:
			return
		}
	}
}

// Stats returns cumulative counters.
func (r *Reporter) Stats() ReporterStats {
	s := ReporterStats{Sent: r.sent.Load(), Failed: r.failed.Load(), Dropped: r.dropped.Load(),
		Rejected: r.rejected.Load(), BytesSent: r.bytes.Load()}
	if ns := r.lastSuccess.Load(); ns > 0 {
		s.LastSuccess = time.Unix(0, ns)
	}
	return s
}

func (r *Reporter) jittered(d time.Duration) time.Duration {
	return time.Duration(float64(d) * (0.8 + 0.4*rand.Float64()))
}

func (r *Reporter) sleep(d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-r.stop:
		return false
	case <-t.C:
		return true
	}
}

func (r *Reporter) produce() {
	defer r.wg.Done()
	// Spread the first report over a full interval so that a fleet-wide
	// restart does not synchronize uploads.
	if !r.sleep(time.Duration(rand.Int64N(int64(r.cfg.Interval)) + 1)) {
		return
	}
	for {
		if b := r.cfg.Build(); b != nil {
			r.enqueue(b)
		}
		if !r.sleep(r.jittered(r.cfg.Interval)) {
			return
		}
	}
}

// enqueue adds b, dropping the oldest queued batch when full.
func (r *Reporter) enqueue(b *Batch) {
	for {
		select {
		case r.queue <- b:
			return
		default:
		}
		select {
		case <-r.queue:
			r.dropped.Add(1)
		default:
		}
	}
}

func (r *Reporter) send() {
	defer r.wg.Done()
	for {
		select {
		case <-r.stop:
			return
		case b := <-r.queue:
			r.deliver(b)
		}
	}
}

type permanentError struct{ error }

func (r *Reporter) deliver(b *Batch) {
	body, gz, err := Encode(b)
	if err != nil || len(body) > MaxCompressedBytes {
		r.rejected.Add(1)
		r.cfg.Logger.Warn("keyshift telemetry batch dropped: encoding failed or too large", "error", err, "bytes", len(body))
		return
	}
	backoff := time.Second
	for attempt := 1; ; attempt++ {
		retryAfter, err := r.post(body, gz)
		if err == nil {
			r.sent.Add(1)
			r.bytes.Add(uint64(len(body)))
			return
		}
		var perm permanentError
		if errors.As(err, &perm) {
			r.rejected.Add(1)
			r.cfg.Logger.Warn("keyshift telemetry batch rejected by controller", "error", err)
			return
		}
		if attempt >= r.cfg.MaxAttempts {
			r.failed.Add(1)
			r.cfg.Logger.Debug("keyshift telemetry batch dropped after retries", "error", err, "attempts", attempt)
			return
		}
		wait := r.jittered(backoff)
		if retryAfter > wait {
			wait = min(retryAfter, r.cfg.MaxBackoff)
		}
		if !r.sleep(wait) {
			r.dropped.Add(1)
			return
		}
		backoff = min(backoff*2, r.cfg.MaxBackoff)
	}
}

func (r *Reporter) post(body []byte, gz bool) (time.Duration, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-r.stop:
			cancel()
		case <-ctx.Done():
		}
	}()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.cfg.Endpoint+"/v1/telemetry", bytes.NewReader(body))
	if err != nil {
		return 0, permanentError{err}
	}
	req.Header.Set("Content-Type", "application/json")
	if gz {
		req.Header.Set("Content-Encoding", "gzip")
	}
	r.setAuth(req)
	resp, err := r.cfg.HTTPClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusOK:
		var out Response
		if err := json.NewDecoder(io.LimitReader(resp.Body, policy.MaxPolicyBytes*3)).Decode(&out); err != nil {
			return 0, fmt.Errorf("decode response: %w", err)
		}
		r.contact()
		if out.Policy != nil && r.cfg.OnPolicy != nil {
			r.cfg.OnPolicy(out.Policy)
		}
		return 0, nil
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable:
		ra, _ := strconv.Atoi(resp.Header.Get("Retry-After"))
		return time.Duration(ra) * time.Second, fmt.Errorf("controller busy: %s", resp.Status)
	case resp.StatusCode >= 500:
		return 0, fmt.Errorf("controller error: %s", resp.Status)
	default:
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return 0, permanentError{fmt.Errorf("%s: %s", resp.Status, bytes.TrimSpace(msg))}
	}
}

func (r *Reporter) setAuth(req *http.Request) {
	if r.cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+r.cfg.Token)
	}
	if r.cfg.PolicyVersion != nil {
		req.Header.Set("X-KeyShift-Policy-Version", strconv.FormatInt(r.cfg.PolicyVersion(), 10))
	}
}

func (r *Reporter) contact() {
	r.lastSuccess.Store(time.Now().UnixNano())
	if r.cfg.OnContact != nil {
		r.cfg.OnContact()
	}
}

// FetchPolicy retrieves the policy envelope for this instance's service.
// It returns (nil, nil) when the controller has no policy.
func (r *Reporter) FetchPolicy(ctx context.Context) (*policy.Envelope, error) {
	q := url.Values{"service": {r.cfg.Service}, "instance": {r.cfg.Instance}}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.cfg.Endpoint+"/v1/policy?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	r.setAuth(req)
	resp, err := r.cfg.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound:
		r.contact()
		return nil, nil
	default:
		return nil, fmt.Errorf("policy fetch: %s", resp.Status)
	}
	var env policy.Envelope
	if err := json.NewDecoder(io.LimitReader(resp.Body, policy.MaxPolicyBytes*3)).Decode(&env); err != nil {
		return nil, fmt.Errorf("policy fetch: decode: %w", err)
	}
	r.contact()
	return &env, nil
}
