// Package admission decides, per process, which Redis keys are worth caching
// locally. Decisions are made once per observation window from local
// measurements only; the request path performs a single lock-free map lookup
// against an immutable admitted-key snapshot.
package admission

// State is the lifecycle state of a key in the admission engine.
type State uint8

const (
	// Cold keys are not cached and not currently qualifying.
	Cold State = iota
	// Candidate keys qualified in at least one window and are under
	// observation, or qualified but did not fit in the cache budget.
	Candidate
	// Admitted keys are read through the tracked client-side cache.
	Admitted
	// Cooldown keys were demoted because caching them was not effective
	// (for example invalidation churn) and are barred from re-admission
	// until the cooldown expires.
	Cooldown
	// Bypassed keys are excluded by policy (deny pattern, deny fingerprint)
	// or by size limits.
	Bypassed
	// Disabled means caching is turned off by policy or rollout.
	Disabled
	// Unhealthy means caching is suspended because client tracking is not
	// healthy.
	Unhealthy
)

var stateNames = [...]string{"cold", "candidate", "admitted", "cooldown", "bypassed", "disabled", "unhealthy"}

func (s State) String() string {
	if int(s) < len(stateNames) {
		return stateNames[s]
	}
	return "unknown"
}

// ParseState converts a state name back to a State.
func ParseState(s string) (State, bool) {
	for i, n := range stateNames {
		if n == s {
			return State(i), true
		}
	}
	return 0, false
}

// Decision explains the current admission state of a key.
type Decision struct {
	Key    string `json:"key"`
	State  string `json:"state"`
	Reason string `json:"reason"`
	// Rates are per second, measured over the last closed window.
	ReadRate         float64 `json:"read_rate"`
	WriteRate        float64 `json:"write_rate"`
	FillRate         float64 `json:"fill_rate"`
	InvalidationRate float64 `json:"invalidation_rate"`
	// HitRatio is observed for admitted keys and predicted otherwise.
	HitRatio      float64 `json:"hit_ratio"`
	HitRatioKind  string  `json:"hit_ratio_kind"`
	SavedRate     float64 `json:"saved_rate"`
	SizeBytes     int64   `json:"size_bytes"`
	Qualifying    int     `json:"qualifying_windows"`
	Strikes       int     `json:"cooldown_strikes"`
	CooldownUntil string  `json:"cooldown_until,omitempty"`
	Pinned        bool    `json:"pinned"`
	PolicyVersion int64   `json:"policy_version"`
	PolicySource  string  `json:"policy_source"`
}
