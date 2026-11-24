package policy

import "time"

// Default admission thresholds. They are starting points validated by the
// benchmark suite (docs/benchmarks.md), not universally optimal values.
var DefaultAdmission = Admission{
	MinReadRate:           2,
	MinHitRatio:           0.6,
	ExitHitRatio:          0.3,
	MinObservationWindows: 2,
	ExitWindows:           3,
	CooldownSeconds:       30,
	MaxEntryBytes:         64 << 10,
}

// DefaultBudget is the default per-instance cache budget.
var DefaultBudget = Budget{MaxEntries: 1000, MaxMemoryBytes: 16 << 20}

// DefaultValidFor is how long a remote policy stays valid without refresh.
const DefaultValidFor = 10 * time.Minute

// Policy sources reported in Effective.Source.
const (
	// SourceLocal is the SDK's own configuration.
	SourceLocal = "local"
	// SourceRemote is a policy merged over the local configuration: one
	// received from the controller or applied programmatically.
	SourceRemote = "remote"
)

// Effective is the fully resolved policy an SDK instance enforces.
type Effective struct {
	// Source is SourceLocal or SourceRemote.
	Source    string
	Version   int64
	Mode      Mode
	Admission Admission
	Budget    Budget
	ValidFor  time.Duration

	Pin      *Matcher
	Deny     *Matcher
	DenyFP   map[string]struct{}
	HasPins  bool
	HasDenyF bool
}

// Resolve merges a remote policy (may be nil) into the local policy. Local
// values are hard limits: a remote policy can tighten budgets and add deny
// rules but cannot raise budgets above local limits or re-enable caching
// that is locally switched off.
func Resolve(local, remote *Policy) *Effective {
	if local == nil {
		local = &Policy{Mode: ModeAdaptive}
	}
	e := &Effective{
		Source:    SourceLocal,
		Version:   local.Version,
		Mode:      local.Mode,
		Admission: fill(local.Admission, DefaultAdmission),
		Budget:    fillBudget(local.Budget, DefaultBudget),
		ValidFor:  DefaultValidFor,
	}
	pins := append([]string(nil), local.PinPatterns...)
	deny := append([]string(nil), local.DenyPatterns...)
	denyFP := append([]string(nil), local.DenyFingerprints...)

	if remote != nil {
		e.Source = SourceRemote
		e.Version = remote.Version
		if local.Mode != ModeOff && remote.Mode != "" {
			e.Mode = remote.Mode
		}
		e.Admission = fill(remote.Admission, e.Admission)
		if remote.Budget.MaxEntries > 0 && remote.Budget.MaxEntries < e.Budget.MaxEntries {
			e.Budget.MaxEntries = remote.Budget.MaxEntries
		}
		if remote.Budget.MaxMemoryBytes > 0 && remote.Budget.MaxMemoryBytes < e.Budget.MaxMemoryBytes {
			e.Budget.MaxMemoryBytes = remote.Budget.MaxMemoryBytes
		}
		// Entries larger than the local limit are never admitted.
		if local.Admission.MaxEntryBytes > 0 && e.Admission.MaxEntryBytes > local.Admission.MaxEntryBytes {
			e.Admission.MaxEntryBytes = local.Admission.MaxEntryBytes
		}
		if remote.ValidForSeconds > 0 {
			e.ValidFor = time.Duration(remote.ValidForSeconds) * time.Second
		}
		pins = append(pins, remote.PinPatterns...)
		deny = append(deny, remote.DenyPatterns...)
		denyFP = append(denyFP, remote.DenyFingerprints...)
	}
	if e.Admission.ExitHitRatio > e.Admission.MinHitRatio {
		e.Admission.ExitHitRatio = e.Admission.MinHitRatio
	}
	e.Pin = NewMatcher(pins)
	e.Deny = NewMatcher(deny)
	e.HasPins = len(pins) > 0
	if len(denyFP) > 0 {
		e.DenyFP = make(map[string]struct{}, len(denyFP))
		for _, fp := range denyFP {
			e.DenyFP[fp] = struct{}{}
		}
		e.HasDenyF = true
	}
	return e
}

func fill(a, def Admission) Admission {
	if a.MinReadRate == 0 {
		a.MinReadRate = def.MinReadRate
	}
	if a.MinHitRatio == 0 {
		a.MinHitRatio = def.MinHitRatio
	}
	if a.ExitHitRatio == 0 {
		a.ExitHitRatio = def.ExitHitRatio
	}
	if a.MinObservationWindows == 0 {
		a.MinObservationWindows = def.MinObservationWindows
	}
	if a.ExitWindows == 0 {
		a.ExitWindows = def.ExitWindows
	}
	if a.CooldownSeconds == 0 {
		a.CooldownSeconds = def.CooldownSeconds
	}
	if a.MaxEntryBytes == 0 {
		a.MaxEntryBytes = def.MaxEntryBytes
	}
	return a
}

func fillBudget(b, def Budget) Budget {
	if b.MaxEntries == 0 {
		b.MaxEntries = def.MaxEntries
	}
	if b.MaxMemoryBytes == 0 {
		b.MaxMemoryBytes = def.MaxMemoryBytes
	}
	return b
}
