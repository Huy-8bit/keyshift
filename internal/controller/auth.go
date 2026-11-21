// Package controller implements the optional KeyShift control plane:
// telemetry ingestion, bounded global aggregation, versioned policy
// distribution, and the API consumed by the CLI and dashboard.
//
// The controller is never on the Redis request path. SDK instances keep
// working with their last valid (or local) policy when it is unavailable.
package controller

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Role is an API permission level.
type Role int

const (
	RoleNone Role = iota
	// RoleIngest may upload telemetry and fetch policies (SDKs).
	RoleIngest
	// RoleViewer may read aggregated data (dashboard, CLI).
	RoleViewer
	// RoleAdmin may also change policies.
	RoleAdmin
)

func (r Role) String() string {
	switch r {
	case RoleIngest:
		return "ingest"
	case RoleViewer:
		return "viewer"
	case RoleAdmin:
		return "admin"
	}
	return "none"
}

// Auth maps bearer tokens to roles. Tokens are compared in constant time
// against SHA-256 digests so token length does not leak.
type Auth struct {
	entries []tokenEntry
	// Disabled grants admin to every request. Only for local development;
	// the controller refuses to start with it on a non-loopback address.
	Disabled bool
}

type tokenEntry struct {
	digest [32]byte
	role   Role
}

// NewAuth builds an Auth from token lists. Admin tokens also grant viewer
// access; ingest tokens grant only ingestion.
func NewAuth(ingest, viewer, admin []string) *Auth {
	a := &Auth{}
	add := func(tokens []string, r Role) {
		for _, t := range tokens {
			if t = strings.TrimSpace(t); t != "" {
				a.entries = append(a.entries, tokenEntry{sha256.Sum256([]byte(t)), r})
			}
		}
	}
	add(ingest, RoleIngest)
	add(viewer, RoleViewer)
	add(admin, RoleAdmin)
	return a
}

// Empty reports whether no tokens are configured.
func (a *Auth) Empty() bool { return len(a.entries) == 0 }

// Roles is the set of roles granted to a token. A token listed under
// several roles holds all of them.
type Roles uint8

func (rs Roles) has(r Role) bool { return rs&(1<<r) != 0 }

// Highest returns the most privileged role in the set (for display).
func (rs Roles) Highest() Role {
	for r := RoleAdmin; r > RoleNone; r-- {
		if rs.has(r) {
			return r
		}
	}
	return RoleNone
}

// RolesFor returns the roles of a request.
func (a *Auth) RolesFor(r *http.Request) Roles {
	if a.Disabled {
		return 1<<RoleIngest | 1<<RoleViewer | 1<<RoleAdmin
	}
	h := r.Header.Get("Authorization")
	tok, ok := strings.CutPrefix(h, "Bearer ")
	if !ok || tok == "" {
		return 0
	}
	d := sha256.Sum256([]byte(tok))
	var rs Roles
	for _, e := range a.entries {
		if subtle.ConstantTimeCompare(d[:], e.digest[:]) == 1 {
			rs |= 1 << e.role
		}
	}
	return rs
}

// allows reports whether a token's roles satisfy need. Ingest is a separate
// capability: viewer tokens cannot upload telemetry and ingest-only tokens
// cannot read the API. Admin implies viewer and ingest.
func allows(have Roles, need Role) bool {
	switch need {
	case RoleIngest:
		return have.has(RoleIngest) || have.has(RoleAdmin)
	case RoleViewer:
		return have.has(RoleViewer) || have.has(RoleAdmin)
	default:
		return have.has(need)
	}
}

// tokenBucket is a simple thread-safe token-bucket rate limiter.
type tokenBucket struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
}

func newTokenBucket(rate float64, burst int) *tokenBucket {
	return &tokenBucket{rate: rate, burst: float64(burst), tokens: float64(burst), last: time.Now()}
}

// allow takes one token, returning false and a suggested retry delay when
// none are available.
func (b *tokenBucket) allow(now time.Time) (bool, time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.rate <= 0 {
		return true, 0
	}
	b.tokens = min(b.burst, b.tokens+now.Sub(b.last).Seconds()*b.rate)
	b.last = now
	if b.tokens >= 1 {
		b.tokens--
		return true, 0
	}
	return false, time.Duration((1 - b.tokens) / b.rate * float64(time.Second))
}
