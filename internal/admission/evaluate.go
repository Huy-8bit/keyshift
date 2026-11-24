package admission

import (
	"fmt"
	"math"
	"time"

	"github.com/Huy-8bit/keyshift/internal/policy"
)

// blocked reports whether policy or limits prevent caching key at all.
func (e *Engine) blocked(key string, r *record, eff *policy.Effective) (State, string, bool) {
	if why := e.unavailable.Load(); why != nil {
		return Disabled, "local caching unavailable: " + *why, true
	}
	if !e.healthy.Load() {
		return Unhealthy, "client tracking is unhealthy; caching suspended", true
	}
	if eff.Mode == policy.ModeOff {
		return Disabled, "caching disabled by policy (mode=off)", true
	}
	if !e.inRoll.Load() {
		return Disabled, "instance outside policy rollout", true
	}
	if eff.Deny.Match(key) {
		return Bypassed, "denied by policy pattern", true
	}
	if eff.HasDenyF && e.cfg.Fingerprint != nil {
		if _, ok := eff.DenyFP[e.cfg.Fingerprint(key)]; ok {
			return Bypassed, "denied by policy fingerprint", true
		}
	}
	if r != nil && r.size > eff.Admission.MaxEntryBytes {
		return Bypassed, fmt.Sprintf("value size %d exceeds max_entry_bytes %d", r.size, eff.Admission.MaxEntryBytes), true
	}
	return 0, "", false
}

// maxCooldownDoublings caps the exponential cooldown at 2^4 = 16 times the
// base cooldown.
const maxCooldownDoublings = 4

// evaluate applies the state machine to one record: policy and health gates
// first, then cooldown, pins, and explicit mode, then the adaptive rules.
func (e *Engine) evaluate(r *record, eff *policy.Effective, now time.Time, stalenessFloor float64) {
	a := eff.Admission
	r.pinned = eff.Pin.Match(r.key)
	r.estimateBenefit(stalenessFloor)

	if st, reason, blocked := e.blocked(r.key, r, eff); blocked {
		if st == Unhealthy && r.state == Admitted {
			r.qualifying = a.MinObservationWindows // allow prompt recovery
		}
		r.state, r.reason = st, reason
		return
	}

	if r.state == Cooldown {
		if now.Before(r.cooldownUntil) {
			r.reason = fmt.Sprintf("cooling down after ineffective caching (strike %d)", r.strikes)
			return
		}
		r.state, r.qualifying = Cold, 0
	}

	switch {
	case r.pinned:
		r.state, r.reason = Admitted, "pinned by policy"
	case eff.Mode == policy.ModeExplicit:
		r.state, r.reason, r.qualifying = Cold, "explicit mode: key does not match a pin pattern", 0
	case r.stats != nil:
		e.evaluateAdmitted(r, a, now)
	default:
		r.evaluateUnadmitted(a)
	}
}

// estimateBenefit sets the hit ratio and saved Redis reads per second:
// observed for admitted keys that were read this window, otherwise
// predicted from the read and write rates. Every cached entry is refetched
// at least once per max_staleness, which bounds the predicted hit ratio.
func (r *record) estimateBenefit(stalenessFloor float64) {
	if r.stats != nil && r.reads > 0 {
		r.hitObserved = true
		r.hitRatio = 1 - math.Min(1, float64(r.fills)/float64(r.reads))
		r.savedRate = r.readRate * r.hitRatio
		return
	}
	r.hitObserved = false
	predictedFill := math.Min(r.readRate, r.writeRate+stalenessFloor)
	r.hitRatio = 0
	if r.readRate > 0 {
		r.hitRatio = 1 - predictedFill/r.readRate
	}
	r.savedRate = math.Max(0, r.readRate-predictedFill)
}

// evaluateAdmitted decides whether an admitted key stays admitted, goes into
// cooldown (caching proved ineffective), or turns cold (no longer hot).
func (e *Engine) evaluateAdmitted(r *record, a policy.Admission, now time.Time) {
	switch {
	case r.reads >= minSampleReads && r.hitRatio < a.ExitHitRatio:
		r.strikes++
		cool := time.Duration(a.CooldownSeconds) * time.Second << min(r.strikes-1, maxCooldownDoublings)
		r.cooldownUntil = now.Add(cool)
		r.state, r.qualifying, r.coldWindows = Cooldown, 0, 0
		r.reason = fmt.Sprintf("observed hit ratio %.2f below exit threshold %.2f (fills %d / reads %d, invalidations %d); cooldown %s",
			r.hitRatio, a.ExitHitRatio, r.fills, r.reads, r.invalidations, cool)
		e.counters.Cooldowns++
	case r.readRate < a.MinReadRate/2:
		r.coldWindows++
		if r.coldWindows >= a.ExitWindows {
			r.state, r.qualifying, r.coldWindows = Cold, 0, 0
			r.reason = fmt.Sprintf("read rate %.2f/s stayed below exit threshold %.2f/s for %d windows", r.readRate, a.MinReadRate/2, a.ExitWindows)
			return
		}
		r.state = Admitted
		r.reason = fmt.Sprintf("admitted; read rate low for %d/%d windows", r.coldWindows, a.ExitWindows)
	default:
		r.coldWindows = 0
		r.state = Admitted
		r.reason = fmt.Sprintf("admitted; observed hit ratio %.2f saves %.1f Redis reads/s", r.hitRatio, r.savedRate)
	}
}

// evaluateUnadmitted moves a key that is not admitted (cold, candidate,
// recovering from unhealthy, or previously blocked by a now relaxed policy)
// towards admission after MinObservationWindows qualifying windows.
func (r *record) evaluateUnadmitted(a policy.Admission) {
	switch {
	case r.readRate < a.MinReadRate:
		r.state, r.qualifying = Cold, 0
		r.reason = fmt.Sprintf("read rate %.2f/s below min_read_rate %.2f/s", r.readRate, a.MinReadRate)
	case r.hitRatio < a.MinHitRatio:
		r.state, r.qualifying = Cold, 0
		r.reason = fmt.Sprintf("predicted hit ratio %.2f below min_hit_ratio %.2f (write rate %.2f/s)", r.hitRatio, a.MinHitRatio, r.writeRate)
	default:
		r.qualifying++
		if r.qualifying >= a.MinObservationWindows {
			r.state = Admitted
			r.reason = fmt.Sprintf("admitted; predicted hit ratio %.2f saves %.1f Redis reads/s", r.hitRatio, r.savedRate)
		} else {
			r.state = Candidate
			r.reason = fmt.Sprintf("qualifying window %d/%d", r.qualifying, a.MinObservationWindows)
		}
	}
}
