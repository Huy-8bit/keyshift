package controller

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Huy-8bit/keyshift/internal/policy"
	"github.com/Huy-8bit/keyshift/internal/telemetry"
)

// Version is the controller version.
const Version = "0.1.0"

// Config configures a Server.
type Config struct {
	Auth *Auth
	// DataDir persists policies. Empty keeps policies in memory only.
	DataDir string

	MaxInstances           int
	MaxServices            int
	MaxCandidatesPerBatch  int
	MaxGlobalKeys          int
	MaxPolicyScopes        int
	InstanceTTL            time.Duration
	FreshFor               time.Duration
	AggregateInterval      time.Duration
	HistoryPoints          int
	ReportIntervalSeconds  int
	IngestRatePerSecond    float64
	IngestBurst            int
	PerInstanceMinInterval time.Duration
	// MetricsPublic serves /metrics without authentication.
	MetricsPublic bool
	// MetricsTopKeys bounds the hot keys exported as Prometheus series.
	MetricsTopKeys int
	// MetricsInstances exports per-instance series (one set per SDK
	// instance; consider cardinality before enabling at large scale).
	MetricsInstances bool
	Logger           *slog.Logger
}

// DefaultConfig returns production-oriented defaults sized for ~5,000 SDK
// instances reporting every 30 seconds.
func DefaultConfig() Config {
	return Config{
		MaxInstances:           20000,
		MaxServices:            1000,
		MaxCandidatesPerBatch:  50,
		MaxGlobalKeys:          5000,
		MaxPolicyScopes:        1001,
		InstanceTTL:            10 * time.Minute,
		FreshFor:               2 * time.Minute,
		AggregateInterval:      10 * time.Second,
		HistoryPoints:          360,
		ReportIntervalSeconds:  30,
		IngestRatePerSecond:    2000,
		IngestBurst:            4000,
		PerInstanceMinInterval: 5 * time.Second,
		MetricsPublic:          true,
		MetricsTopKeys:         20,
	}
}

// Server is the controller.
type Server struct {
	cfg      Config
	registry *Registry
	agg      *Aggregator
	policies *PolicyStore
	limiter  *tokenBucket
	started  time.Time
	ready    atomic.Bool

	accepted, duplicate, rejected, bytesIn atomic.Uint64

	promReg  *prometheus.Registry
	reqTotal *prometheus.CounterVec
	ingest   *prometheus.CounterVec
	aggDur   prometheus.Histogram
	mux      *http.ServeMux
}

// New creates a controller server.
func New(cfg Config) (*Server, error) {
	if cfg.Auth == nil {
		return nil, errors.New("controller: Auth is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	ps, err := NewPolicyStore(cfg.DataDir, 200, cfg.MaxPolicyScopes)
	if err != nil {
		return nil, err
	}
	s := &Server{
		cfg:      cfg,
		registry: NewRegistry(cfg.MaxInstances, cfg.MaxServices, cfg.MaxCandidatesPerBatch, cfg.PerInstanceMinInterval, cfg.InstanceTTL),
		agg: NewAggregator(AggregatorConfig{FreshFor: cfg.FreshFor, MaxGlobalKeys: cfg.MaxGlobalKeys,
			HistoryPoints: cfg.HistoryPoints, TrendKeys: 20, TrendPoints: 60, MinLocalRate: policy.DefaultAdmission.MinReadRate}),
		policies: ps,
		limiter:  newTokenBucket(cfg.IngestRatePerSecond, cfg.IngestBurst),
		started:  time.Now(),
		promReg:  prometheus.NewRegistry(),
	}
	s.reqTotal = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "keyshift_controller_requests_total",
		Help: "HTTP requests by route and status code."}, []string{"route", "code"})
	s.ingest = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "keyshift_controller_ingest_total",
		Help: "Telemetry uploads by result."}, []string{"result"})
	s.aggDur = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "keyshift_controller_aggregation_duration_seconds",
		Help: "Time to build the global view.", Buckets: prometheus.ExponentialBuckets(0.0005, 2, 14)})
	s.promReg.MustRegister(s.reqTotal, s.ingest, s.aggDur,
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "keyshift_controller_instances", Help: "Registered SDK instances."},
			func() float64 { return float64(s.registry.Len()) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "keyshift_controller_active_instances", Help: "SDK instances that reported recently."},
			func() float64 { return float64(s.agg.Current().Overview.ActiveInstances) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "keyshift_controller_global_keys", Help: "Key fingerprints in the global ranking."},
			func() float64 { return float64(len(s.agg.Current().Keys)) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "keyshift_controller_ingest_bytes_total", Help: "Telemetry bytes received (as sent, possibly compressed)."},
			func() float64 { return float64(s.bytesIn.Load()) }),
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		newFleetCollector(s),
	)
	s.routes()
	s.Aggregate(time.Now())
	s.ready.Store(true)
	return s, nil
}

// Run aggregates and sweeps periodically until ctx is done.
func (s *Server) Run(ctx context.Context) {
	t := time.NewTicker(s.cfg.AggregateInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			s.registry.Sweep(now)
			s.Aggregate(now)
		}
	}
}

// Aggregate rebuilds the global view now.
func (s *Server) Aggregate(now time.Time) *View {
	start := time.Now()
	v := s.agg.Build(s.registry.snapshot(), now, ControllerInfo{
		StartedAt: s.started, Version: Version, IngestAccepted: s.accepted.Load(), IngestDuplicate: s.duplicate.Load(),
		IngestRejected: s.rejected.Load(), IngestBytes: s.bytesIn.Load(), RegistryCapacity: s.cfg.MaxInstances,
	})
	s.aggDur.Observe(time.Since(start).Seconds())
	return v
}

// Handler returns the HTTP handler.
func (s *Server) Handler() http.Handler { return s.mux }

// Registry exposes the instance registry (for tests and simulations).
func (s *Server) Registry() *Registry { return s.registry }

// Policies exposes the policy store.
func (s *Server) Policies() *PolicyStore { return s.policies }

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (r *statusRecorder) WriteHeader(c int) { r.code = c; r.ResponseWriter.WriteHeader(c) }

func (s *Server) route(pattern, name string, need Role, h http.HandlerFunc) {
	s.mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
		rec := &statusRecorder{ResponseWriter: w, code: 200}
		w = rec
		defer func() { s.reqTotal.WithLabelValues(name, strconv.Itoa(rec.code)).Inc() }()
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Cache-Control", "no-store")
		if need != RoleNone {
			roles := s.cfg.Auth.RolesFor(r)
			if roles == 0 {
				w.Header().Set("WWW-Authenticate", `Bearer realm="keyshift"`)
				writeError(w, http.StatusUnauthorized, "missing or invalid bearer token")
				return
			}
			if !allows(roles, need) {
				writeError(w, http.StatusForbidden, "token does not grant "+need.String()+" access")
				return
			}
			r = r.WithContext(context.WithValue(r.Context(), roleKey{}, roles.Highest()))
		}
		h(w, r)
	})
}

type roleKey struct{}

func (s *Server) routes() {
	s.mux = http.NewServeMux()
	s.route("GET /healthz", "healthz", RoleNone, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{"status": "ok"})
	})
	s.route("GET /readyz", "readyz", RoleNone, func(w http.ResponseWriter, r *http.Request) {
		if !s.ready.Load() {
			writeError(w, 503, "not ready")
			return
		}
		writeJSON(w, 200, map[string]any{"status": "ready"})
	})
	metricsRole := RoleViewer
	if s.cfg.MetricsPublic {
		metricsRole = RoleNone
	}
	ph := promhttp.HandlerFor(s.promReg, promhttp.HandlerOpts{})
	s.route("GET /metrics", "metrics", metricsRole, ph.ServeHTTP)

	// SDK endpoints.
	s.route("POST /v1/telemetry", "telemetry", RoleIngest, s.handleTelemetry)
	s.route("GET /v1/policy", "policy_fetch", RoleIngest, s.handlePolicyFetch)

	// Read API.
	s.route("GET /api/v1/whoami", "whoami", RoleViewer, func(w http.ResponseWriter, r *http.Request) {
		role, _ := r.Context().Value(roleKey{}).(Role)
		writeJSON(w, 200, map[string]any{"role": role.String(), "version": Version})
	})
	s.route("GET /api/v1/overview", "overview", RoleViewer, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, s.agg.Current().Overview)
	})
	s.route("GET /api/v1/timeseries", "timeseries", RoleViewer, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, s.agg.History())
	})
	s.route("GET /api/v1/services", "services", RoleViewer, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, s.agg.Current().Services)
	})
	s.route("GET /api/v1/instances", "instances", RoleViewer, s.handleInstances)
	s.route("GET /api/v1/hotkeys", "hotkeys", RoleViewer, s.handleHotKeys)
	s.route("GET /api/v1/hotkeys/{fp}", "hotkey", RoleViewer, func(w http.ResponseWriter, r *http.Request) {
		fp := r.PathValue("fp")
		for _, k := range s.agg.Current().Keys {
			if k.FP == fp {
				writeJSON(w, 200, k)
				return
			}
		}
		writeError(w, 404, "fingerprint not in the global ranking")
	})
	s.route("GET /api/v1/cache", "cache", RoleViewer, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, s.agg.Current().Cache)
	})
	s.route("GET /api/v1/policies", "policies", RoleViewer, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, s.policies.All())
	})
	s.route("GET /api/v1/policies/history", "policy_history", RoleViewer, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, s.policies.History(r.URL.Query().Get("service")))
	})

	// Admin API.
	s.route("PUT /api/v1/policies/{scope}", "policy_put", RoleAdmin, s.handlePolicyPut)
	s.route("POST /api/v1/policies/{scope}/rollout", "policy_rollout", RoleAdmin, s.handlePolicyRollout)
	s.route("POST /api/v1/policies/{scope}/rollback", "policy_rollback", RoleAdmin, func(w http.ResponseWriter, r *http.Request) {
		env, err := s.policies.Rollback(serviceOf(r.PathValue("scope")), actor(r))
		s.respondEnvelope(w, env, err)
	})
	s.route("POST /api/v1/policies/{scope}/disable", "policy_disable", RoleAdmin, func(w http.ResponseWriter, r *http.Request) {
		env, err := s.policies.Put(&policy.Policy{Service: serviceOf(r.PathValue("scope")), Mode: policy.ModeOff,
			Comment: "disabled via API"}, 100, actor(r))
		s.respondEnvelope(w, env, err)
	})
	s.route("DELETE /api/v1/policies/{scope}", "policy_delete", RoleAdmin, func(w http.ResponseWriter, r *http.Request) {
		if err := s.policies.Delete(serviceOf(r.PathValue("scope")), actor(r)); err != nil {
			s.respondEnvelope(w, nil, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	s.route("GET /{$}", "root", RoleNone, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{"service": "keyshift-controller", "version": Version,
			"api": "/api/v1", "metrics": "/metrics", "health": "/healthz"})
	})
}

func serviceOf(scope string) string {
	if scope == DefaultScope {
		return ""
	}
	return scope
}

func actor(r *http.Request) string {
	role, _ := r.Context().Value(roleKey{}).(Role)
	a := role.String()
	if h := r.Header.Get("X-KeyShift-Actor"); h != "" && len(h) <= 64 && policy.ValidName(h) {
		a += ":" + h
	}
	return a
}

func (s *Server) reject(w http.ResponseWriter, code int, result, msg string) {
	s.rejected.Add(1)
	s.ingest.WithLabelValues(result).Inc()
	writeError(w, code, msg)
}

func (s *Server) handleTelemetry(w http.ResponseWriter, r *http.Request) {
	now := time.Now()
	if ok, wait := s.limiter.allow(now); !ok {
		w.Header().Set("Retry-After", strconv.Itoa(max(1, int(wait.Seconds()+0.999))))
		s.reject(w, http.StatusTooManyRequests, "rate_limited", "controller ingest rate limit exceeded")
		return
	}
	body := http.MaxBytesReader(w, r.Body, telemetry.MaxCompressedBytes)
	counted := &countingReader{r: body}
	enc := r.Header.Get("Content-Encoding")
	if enc != "" && enc != "gzip" {
		s.reject(w, http.StatusUnsupportedMediaType, "invalid", "unsupported Content-Encoding")
		return
	}
	b, err := telemetry.Decode(counted, enc == "gzip")
	s.bytesIn.Add(uint64(counted.n))
	if err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) || strings.Contains(err.Error(), "too large") {
			s.reject(w, http.StatusRequestEntityTooLarge, "too_large", "payload too large")
			return
		}
		s.reject(w, http.StatusBadRequest, "invalid", err.Error())
		return
	}
	if err := b.Validate(now); err != nil {
		s.reject(w, http.StatusBadRequest, "invalid", err.Error())
		return
	}
	res, err := s.registry.Ingest(b, now)
	switch res {
	case IngestCapacity:
		w.Header().Set("Retry-After", "60")
		s.reject(w, http.StatusServiceUnavailable, "capacity", err.Error())
		return
	case IngestRateLimited:
		w.Header().Set("Retry-After", strconv.Itoa(int(s.cfg.PerInstanceMinInterval.Seconds())))
		s.reject(w, http.StatusTooManyRequests, "instance_rate_limited", err.Error())
		return
	case IngestDuplicate:
		s.duplicate.Add(1)
		s.ingest.WithLabelValues("duplicate").Inc()
	default:
		s.accepted.Add(1)
		s.ingest.WithLabelValues("accepted").Inc()
	}
	resp := telemetry.Response{Accepted: true, Duplicate: res == IngestDuplicate, NextReportSeconds: s.cfg.ReportIntervalSeconds}
	env := s.policies.For(b.Service)
	resp.NoPolicy = env == nil
	if env != nil {
		sel := env.Select(b.Instance)
		var selVersion int64
		if sel != nil {
			selVersion = sel.Version
		}
		resp.PolicyVersion = selVersion
		have, _ := strconv.ParseInt(r.Header.Get("X-KeyShift-Policy-Version"), 10, 64)
		if have != selVersion {
			resp.Policy = env
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

type countingReader struct {
	r io.Reader
	n int
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += n
	return n, err
}

func (s *Server) handlePolicyFetch(w http.ResponseWriter, r *http.Request) {
	svc := r.URL.Query().Get("service")
	if !policy.ValidName(svc) {
		writeError(w, 400, "service is required")
		return
	}
	env := s.policies.For(svc)
	if env == nil {
		writeError(w, 404, "no policy")
		return
	}
	writeJSON(w, 200, env)
}

func (s *Server) handleInstances(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	svc := q.Get("service")
	limit := clampInt(q.Get("limit"), 500, 1, 5000)
	offset := clampInt(q.Get("offset"), 0, 0, 1<<30)
	all := s.agg.Current().Instances
	var out []InstanceView
	for _, iv := range all {
		if svc == "" || iv.Service == svc {
			out = append(out, iv)
		}
	}
	total := len(out)
	out = out[min(offset, total):min(offset+limit, total)]
	writeJSON(w, 200, map[string]any{"total": total, "items": out})
}

func (s *Server) handleHotKeys(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	svc := q.Get("service")
	class := q.Get("classification")
	limit := clampInt(q.Get("limit"), 100, 1, 5000)
	var out []KeyView
	for _, k := range s.agg.Current().Keys {
		if class != "" && k.Classification != class {
			continue
		}
		if svc != "" && !slices.ContainsFunc(k.Services, func(ss ServiceShare) bool { return ss.Service == svc }) {
			continue
		}
		out = append(out, k)
		if len(out) >= limit {
			break
		}
	}
	if out == nil {
		out = []KeyView{}
	}
	writeJSON(w, 200, out)
}

type policyPut struct {
	Policy         *policy.Policy `json:"policy"`
	RolloutPercent *int           `json:"rollout_percent"`
}

func (s *Server) handlePolicyPut(w http.ResponseWriter, r *http.Request) {
	scope := r.PathValue("scope")
	var req policyPut
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, policy.MaxPolicyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil || req.Policy == nil {
		writeError(w, 400, "body must be {\"policy\": {...}, \"rollout_percent\": N}")
		return
	}
	if scope != DefaultScope && !policy.ValidName(scope) {
		writeError(w, 400, "invalid scope")
		return
	}
	req.Policy.Service = serviceOf(scope)
	rollout := 100
	if req.RolloutPercent != nil {
		rollout = *req.RolloutPercent
	}
	env, err := s.policies.Put(req.Policy, rollout, actor(r))
	s.respondEnvelope(w, env, err)
}

func (s *Server) handlePolicyRollout(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Percent *int `json:"rollout_percent"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024)).Decode(&req); err != nil || req.Percent == nil {
		writeError(w, 400, "body must be {\"rollout_percent\": N}")
		return
	}
	env, err := s.policies.SetRollout(serviceOf(r.PathValue("scope")), *req.Percent, actor(r))
	s.respondEnvelope(w, env, err)
}

func (s *Server) respondEnvelope(w http.ResponseWriter, env *policy.Envelope, err error) {
	switch {
	case errors.Is(err, ErrNotFound):
		writeError(w, 404, "no policy for this scope")
	case errors.Is(err, ErrPersist):
		s.cfg.Logger.Error("policy change could not be persisted; it was rolled back", "error", err)
		writeError(w, http.StatusInternalServerError, "policy could not be persisted; change rolled back (check the controller data directory)")
	case err != nil:
		writeError(w, 400, err.Error())
	default:
		writeJSON(w, 200, env)
	}
}

func clampInt(s string, def, lo, hi int) int {
	v, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return min(max(v, lo), hi)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
