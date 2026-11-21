// Package telemetry defines the SDK-to-controller wire schema and the SDK
// reporter that sends batched, bounded telemetry.
//
// Telemetry never contains Redis values. Keys are represented by keyed
// fingerprints; raw key names are only sent for keys that match explicitly
// configured display patterns.
package telemetry

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/Huy-8bit/keyshift/internal/fingerprint"
	"github.com/Huy-8bit/keyshift/internal/policy"
)

// SchemaVersion is the current telemetry schema version.
const SchemaVersion = 1

// Limits enforced by both the SDK and the controller.
const (
	MaxCandidates         = 200
	MaxDisplayLen         = 256
	MaxReasonLen          = 256
	MaxCompressedBytes    = 1 << 20
	MaxDecompressedBytes  = 4 << 20
	MaxIdentifierLen      = 256
	MaxWindowDuration     = 24 * time.Hour
	MaxClockSkewTolerance = 10 * time.Minute
)

// Batch is one telemetry upload from one SDK instance.
type Batch struct {
	SchemaVersion int    `json:"schema_version"`
	Service       string `json:"service"`
	Instance      string `json:"instance"`
	// Seq increases by one per batch produced by this instance. Together
	// with Instance it makes uploads idempotent.
	Seq        uint64 `json:"seq"`
	SDKVersion string `json:"sdk_version"`
	Driver     string `json:"driver"`
	Topology   string `json:"topology"`

	WindowStart time.Time `json:"window_start"`
	WindowEnd   time.Time `json:"window_end"`

	FingerprintScope string `json:"fingerprint_scope"`
	FingerprintKeyID string `json:"fingerprint_key_id"`

	PolicyVersion int64  `json:"policy_version"`
	PolicySource  string `json:"policy_source"`
	CacheMode     string `json:"cache_mode"`

	Tracking   Tracking    `json:"tracking"`
	Totals     Totals      `json:"totals"`
	Cache      Cache       `json:"cache"`
	Detector   Detector    `json:"detector"`
	Runtime    Runtime     `json:"runtime"`
	Candidates []Candidate `json:"candidates"`
}

// Tracking reports client tracking health.
type Tracking struct {
	State  string `json:"state"`
	Reason string `json:"reason,omitempty"`
	Mode   string `json:"mode,omitempty"`
}

// Totals are counts for the window. They are deliberately separate
// quantities: logical reads observed by the SDK, reads that reached Redis,
// and reads served locally.
type Totals struct {
	Reads         uint64 `json:"reads"`
	CachedReads   uint64 `json:"cached_reads"`
	LocalHits     uint64 `json:"local_hits"`
	RedisReads    uint64 `json:"redis_reads"`
	Bypasses      uint64 `json:"bypasses"`
	Writes        uint64 `json:"writes"`
	Invalidations uint64 `json:"invalidations"`
	Evictions     uint64 `json:"evictions"`
	Errors        uint64 `json:"errors"`
	Admissions    uint64 `json:"admissions"`
	Demotions     uint64 `json:"demotions"`
}

// Cache reports local cache residency at the end of the window.
type Cache struct {
	Entries        int64 `json:"entries"`
	MemoryBytes    int64 `json:"memory_bytes"`
	MaxEntries     int64 `json:"max_entries"`
	MaxMemoryBytes int64 `json:"max_memory_bytes"`
	Admitted       int   `json:"admitted"`
}

// Detector reports detector coverage for the window.
type Detector struct {
	Observed uint64  `json:"observed"`
	Recorded uint64  `json:"recorded"`
	Dropped  uint64  `json:"dropped"`
	Tracked  int     `json:"tracked"`
	Coverage float64 `json:"coverage"`
}

// Runtime reports process resource usage.
type Runtime struct {
	HeapAllocBytes   uint64  `json:"heap_alloc_bytes"`
	Goroutines       int     `json:"goroutines"`
	GCCPUFraction    float64 `json:"gc_cpu_fraction"`
	TelemetryDropped uint64  `json:"telemetry_dropped"`
}

// Candidate is a per-key observation. FP is always present; Display only
// when the key matched a configured display pattern.
type Candidate struct {
	FP            string  `json:"fp"`
	Display       string  `json:"display,omitempty"`
	Reads         uint64  `json:"reads"`
	Writes        uint64  `json:"writes"`
	Fills         uint64  `json:"fills"`
	Invalidations uint64  `json:"invalidations"`
	SizeBytes     int64   `json:"size_bytes"`
	State         string  `json:"state"`
	Reason        string  `json:"reason,omitempty"`
	SavedRate     float64 `json:"saved_rate"`
	HitRatio      float64 `json:"hit_ratio"`
}

// Response is the controller's reply to an upload.
type Response struct {
	Accepted  bool             `json:"accepted"`
	Duplicate bool             `json:"duplicate,omitempty"`
	Policy    *policy.Envelope `json:"policy,omitempty"`
	// PolicyVersion is the version of the envelope's current policy for the
	// instance's service, so SDKs can skip unchanged envelopes.
	PolicyVersion int64 `json:"policy_version"`
	// NextReportSeconds lets the controller slow reporters down.
	NextReportSeconds int `json:"next_report_seconds,omitempty"`
	// NoPolicy is true when no controller policy applies to the instance's
	// service (for example after it was deleted). SDKs then return to their
	// local policy instead of waiting for the remote one to expire.
	NoPolicy bool `json:"no_policy,omitempty"`
}

var validStates = map[string]bool{"cold": true, "candidate": true, "admitted": true, "cooldown": true,
	"bypassed": true, "disabled": true, "unhealthy": true}

// Validate checks a batch received from an untrusted SDK.
func (b *Batch) Validate(now time.Time) error {
	var errs []error
	if b.SchemaVersion != SchemaVersion {
		errs = append(errs, fmt.Errorf("unsupported schema_version %d", b.SchemaVersion))
	}
	if !policy.ValidName(b.Service) || len(b.Service) > policy.MaxServiceNameLen {
		errs = append(errs, fmt.Errorf("invalid service %q", truncate(b.Service, 64)))
	}
	if !policy.ValidName(b.Instance) || len(b.Instance) > MaxIdentifierLen {
		errs = append(errs, fmt.Errorf("invalid instance %q", truncate(b.Instance, 64)))
	}
	if len(b.SDKVersion) > 64 || len(b.Driver) > 32 || len(b.Topology) > 32 || len(b.PolicySource) > 16 || len(b.CacheMode) > 16 {
		errs = append(errs, errors.New("identifier field too long"))
	}
	if b.WindowEnd.Before(b.WindowStart) || b.WindowEnd.Sub(b.WindowStart) > MaxWindowDuration {
		errs = append(errs, errors.New("invalid window"))
	}
	if b.WindowEnd.After(now.Add(MaxClockSkewTolerance)) {
		errs = append(errs, errors.New("window_end is in the future"))
	}
	if b.FingerprintScope != string(fingerprint.ScopeShared) && b.FingerprintScope != string(fingerprint.ScopeInstance) {
		errs = append(errs, errors.New("invalid fingerprint_scope"))
	}
	if len(b.FingerprintKeyID) > 16 || len(b.Tracking.State) > 32 || len(b.Tracking.Reason) > MaxReasonLen || len(b.Tracking.Mode) > 128 {
		errs = append(errs, errors.New("tracking or key id field too long"))
	}
	if len(b.Candidates) > MaxCandidates {
		errs = append(errs, fmt.Errorf("too many candidates (%d > %d)", len(b.Candidates), MaxCandidates))
	}
	for i := range b.Candidates {
		c := &b.Candidates[i]
		if !fingerprint.Valid(c.FP) {
			errs = append(errs, fmt.Errorf("candidate %d: invalid fingerprint", i))
			break
		}
		if len(c.Display) > MaxDisplayLen || len(c.Reason) > MaxReasonLen || !validStates[c.State] {
			errs = append(errs, fmt.Errorf("candidate %d: invalid display, reason or state", i))
			break
		}
		if c.HitRatio < 0 || c.HitRatio > 1 || c.SavedRate < 0 || c.SavedRate > 1e12 {
			errs = append(errs, fmt.Errorf("candidate %d: out-of-range rates", i))
			break
		}
	}
	return errors.Join(errs...)
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

// Encode serializes a batch, gzip-compressing it when that helps. It returns
// the body and whether it is gzip-encoded.
func Encode(b *Batch) ([]byte, bool, error) {
	raw, err := json.Marshal(b)
	if err != nil {
		return nil, false, err
	}
	if len(raw) < 1024 {
		return raw, false, nil
	}
	var buf bytes.Buffer
	zw, _ := gzip.NewWriterLevel(&buf, gzip.BestSpeed)
	if _, err := zw.Write(raw); err != nil {
		return nil, false, err
	}
	if err := zw.Close(); err != nil {
		return nil, false, err
	}
	if buf.Len() >= len(raw) {
		return raw, false, nil
	}
	return buf.Bytes(), true, nil
}

// Decode reads a (possibly gzip-compressed) batch with size limits. The
// caller should already have limited r to MaxCompressedBytes.
func Decode(r io.Reader, gzipped bool) (*Batch, error) {
	if gzipped {
		zr, err := gzip.NewReader(r)
		if err != nil {
			return nil, fmt.Errorf("telemetry: gzip: %w", err)
		}
		defer func() { _ = zr.Close() }()
		r = zr
	}
	lr := &io.LimitedReader{R: r, N: MaxDecompressedBytes + 1}
	dec := json.NewDecoder(lr)
	var b Batch
	if err := dec.Decode(&b); err != nil {
		if lr.N <= 0 {
			return nil, errors.New("telemetry: decompressed payload too large")
		}
		return nil, fmt.Errorf("telemetry: decode: %w", err)
	}
	if lr.N <= 0 {
		return nil, errors.New("telemetry: decompressed payload too large")
	}
	return &b, nil
}
