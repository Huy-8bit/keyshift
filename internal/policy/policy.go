// Package policy defines KeyShift caching policies, their validation, and
// how remote policies are merged with local hard limits.
//
// A Policy is an immutable value. SDK instances hold the active policy in an
// atomic pointer so policy reads on the request path never block and never
// observe a partially applied update.
package policy

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"strings"
	"time"
)

// Mode selects how keys become eligible for local caching.
type Mode string

const (
	// ModeAdaptive admits keys automatically based on observed workload, plus
	// any pinned patterns.
	ModeAdaptive Mode = "adaptive"
	// ModeExplicit admits only keys matching pinned patterns.
	ModeExplicit Mode = "explicit"
	// ModeOff disables local caching. Detection and telemetry keep running.
	ModeOff Mode = "off"
)

// Limits on policy contents to keep evaluation cheap and bounded.
const (
	MaxPatterns       = 256
	MaxPatternLen     = 256
	MaxFingerprints   = 1024
	MaxServiceNameLen = 128
	MaxCommentLen     = 1024
	MaxPolicyBytes    = 256 << 10
)

// Admission holds the tunable thresholds of the admission engine. Zero values
// mean "use the local default".
type Admission struct {
	// MinReadRate is the minimum local read rate (reads/second) for a key to
	// become a candidate.
	MinReadRate float64 `json:"min_read_rate,omitempty"`
	// MinHitRatio is the minimum predicted fraction of reads served locally
	// for a key to be admitted.
	MinHitRatio float64 `json:"min_hit_ratio,omitempty"`
	// ExitHitRatio is the observed hit ratio below which an admitted key is
	// demoted into cooldown. Must be <= MinHitRatio (hysteresis).
	ExitHitRatio float64 `json:"exit_hit_ratio,omitempty"`
	// MinObservationWindows is the number of consecutive qualifying windows
	// required before admission.
	MinObservationWindows int `json:"min_observation_windows,omitempty"`
	// ExitWindows is the number of consecutive cold windows before an
	// admitted key is demoted.
	ExitWindows int `json:"exit_windows,omitempty"`
	// CooldownSeconds is how long a demoted key is barred from re-admission.
	CooldownSeconds int `json:"cooldown_seconds,omitempty"`
	// MaxEntryBytes is the largest value admitted.
	MaxEntryBytes int64 `json:"max_entry_bytes,omitempty"`
}

// Budget bounds local cache resources. Remote budgets can only lower local
// hard limits, never raise them.
type Budget struct {
	MaxEntries     int   `json:"max_entries,omitempty"`
	MaxMemoryBytes int64 `json:"max_memory_bytes,omitempty"`
}

// Policy is a versioned caching policy.
type Policy struct {
	// Version is assigned by the controller and increases monotonically.
	Version int64 `json:"version"`
	// Service scopes the policy. Empty means the default for all services.
	Service string `json:"service,omitempty"`
	Mode    Mode   `json:"mode"`

	Admission Admission `json:"admission,omitempty"`
	Budget    Budget    `json:"budget,omitempty"`

	// PinPatterns are glob patterns of keys that are admitted without
	// waiting for adaptive scoring (still subject to size and health checks).
	// Only pin read-mostly keys.
	PinPatterns []string `json:"pin_patterns,omitempty"`
	// DenyPatterns are glob patterns of keys that are never cached.
	DenyPatterns []string `json:"deny_patterns,omitempty"`
	// DenyFingerprints are key fingerprints that are never cached. They let
	// operators exclude a key seen in the dashboard without knowing its name.
	DenyFingerprints []string `json:"deny_fingerprints,omitempty"`

	// ValidForSeconds bounds how long an SDK keeps using this policy without
	// hearing from the controller before reverting to its local policy.
	ValidForSeconds int `json:"valid_for_seconds,omitempty"`

	Comment   string    `json:"comment,omitempty"`
	CreatedAt time.Time `json:"created_at,omitempty"`
	CreatedBy string    `json:"created_by,omitempty"`
}

// Envelope is what the controller distributes to SDK instances. Instances
// whose rollout bucket is below RolloutPercent use Current; the rest use
// Previous (or their local policy when Previous is nil).
type Envelope struct {
	Current        *Policy `json:"current"`
	Previous       *Policy `json:"previous,omitempty"`
	RolloutPercent int     `json:"rollout_percent"`
}

// Validate checks a policy for structural and semantic errors.
func (p *Policy) Validate() error {
	if p == nil {
		return errors.New("policy: nil")
	}
	var errs []error
	switch p.Mode {
	case ModeAdaptive, ModeExplicit, ModeOff:
	default:
		errs = append(errs, fmt.Errorf("mode %q is not one of adaptive, explicit, off", p.Mode))
	}
	if p.Version < 0 {
		errs = append(errs, errors.New("version must be >= 0"))
	}
	if len(p.Service) > MaxServiceNameLen || !validName(p.Service) {
		errs = append(errs, fmt.Errorf("service %q is invalid", p.Service))
	}
	a := p.Admission
	if a.MinReadRate < 0 || a.MinReadRate > 1e9 {
		errs = append(errs, errors.New("admission.min_read_rate out of range"))
	}
	if a.MinHitRatio < 0 || a.MinHitRatio > 1 || a.ExitHitRatio < 0 || a.ExitHitRatio > 1 {
		errs = append(errs, errors.New("admission hit ratios must be within [0, 1]"))
	}
	if a.MinHitRatio > 0 && a.ExitHitRatio > a.MinHitRatio {
		errs = append(errs, errors.New("admission.exit_hit_ratio must be <= min_hit_ratio"))
	}
	if a.MinObservationWindows < 0 || a.MinObservationWindows > 100 || a.ExitWindows < 0 || a.ExitWindows > 100 {
		errs = append(errs, errors.New("admission window counts must be within [0, 100]"))
	}
	if a.CooldownSeconds < 0 || a.CooldownSeconds > 86400 {
		errs = append(errs, errors.New("admission.cooldown_seconds must be within [0, 86400]"))
	}
	if a.MaxEntryBytes < 0 || a.MaxEntryBytes > 512<<20 {
		errs = append(errs, errors.New("admission.max_entry_bytes out of range"))
	}
	if p.Budget.MaxEntries < 0 || p.Budget.MaxMemoryBytes < 0 {
		errs = append(errs, errors.New("budget values must be >= 0"))
	}
	if p.ValidForSeconds < 0 || p.ValidForSeconds > 7*86400 {
		errs = append(errs, errors.New("valid_for_seconds must be within [0, 604800]"))
	}
	if len(p.Comment) > MaxCommentLen || len(p.CreatedBy) > MaxServiceNameLen {
		errs = append(errs, errors.New("comment or created_by too long"))
	}
	for _, set := range []struct {
		name string
		v    []string
	}{{"pin_patterns", p.PinPatterns}, {"deny_patterns", p.DenyPatterns}} {
		if len(set.v) > MaxPatterns {
			errs = append(errs, fmt.Errorf("%s: at most %d patterns", set.name, MaxPatterns))
		}
		for _, pat := range set.v {
			if err := ValidatePattern(pat); err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", set.name, err))
			}
		}
	}
	if len(p.DenyFingerprints) > MaxFingerprints {
		errs = append(errs, fmt.Errorf("deny_fingerprints: at most %d entries", MaxFingerprints))
	}
	for _, fp := range p.DenyFingerprints {
		if !validFingerprint(fp) {
			errs = append(errs, fmt.Errorf("deny_fingerprints: %q is not a 32-character lowercase hex fingerprint", fp))
		}
	}
	return errors.Join(errs...)
}

// Validate checks an envelope.
func (e *Envelope) Validate() error {
	if e == nil || e.Current == nil {
		return errors.New("policy envelope: current policy is required")
	}
	if e.RolloutPercent < 0 || e.RolloutPercent > 100 {
		return errors.New("policy envelope: rollout_percent must be within [0, 100]")
	}
	if err := e.Current.Validate(); err != nil {
		return fmt.Errorf("current: %w", err)
	}
	if e.Previous != nil {
		if err := e.Previous.Validate(); err != nil {
			return fmt.Errorf("previous: %w", err)
		}
	}
	return nil
}

// Select returns the policy that applies to instanceID under this envelope,
// or nil when the instance should use its local policy.
func (e *Envelope) Select(instanceID string) *Policy {
	if e == nil || e.Current == nil {
		return nil
	}
	if RolloutBucket(instanceID) < e.RolloutPercent {
		return e.Current
	}
	return e.Previous
}

// RolloutBucket maps an instance identity to a stable bucket in [0, 100).
func RolloutBucket(instanceID string) int {
	h := fnv.New32a()
	h.Write([]byte(instanceID))
	return int(h.Sum32() % 100)
}

// Parse decodes and validates a JSON policy, rejecting unknown fields and
// oversized documents.
func Parse(r io.Reader) (*Policy, error) {
	dec := json.NewDecoder(io.LimitReader(r, MaxPolicyBytes+1))
	dec.DisallowUnknownFields()
	var p Policy
	if err := dec.Decode(&p); err != nil {
		return nil, fmt.Errorf("policy: decode: %w", err)
	}
	if dec.More() {
		return nil, errors.New("policy: trailing data after JSON document")
	}
	if p.Mode == "" {
		p.Mode = ModeAdaptive
	}
	if err := p.Validate(); err != nil {
		return nil, err
	}
	return &p, nil
}

// Clone returns a deep copy.
func (p *Policy) Clone() *Policy {
	if p == nil {
		return nil
	}
	c := *p
	c.PinPatterns = append([]string(nil), p.PinPatterns...)
	c.DenyPatterns = append([]string(nil), p.DenyPatterns...)
	c.DenyFingerprints = append([]string(nil), p.DenyFingerprints...)
	return &c
}

func validName(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.') {
			return false
		}
	}
	return true
}

// ValidName reports whether s is an acceptable service or instance name.
func ValidName(s string) bool { return s != "" && len(s) <= 256 && validName(s) }

func validFingerprint(s string) bool {
	if len(s) != 32 {
		return false
	}
	return strings.Trim(s, "0123456789abcdef") == ""
}
