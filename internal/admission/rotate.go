package admission

import (
	"cmp"
	"slices"
	"strings"
	"time"

	"github.com/Huy-8bit/keyshift/internal/detector"
	"github.com/Huy-8bit/keyshift/internal/policy"
)

// Rotate closes the current window, re-evaluates every tracked key, and
// publishes a new admitted set. It must not be called concurrently with
// itself; it is safe to call concurrently with the request path.
//
// A rotation has six phases: harvest the cache-path counters of admitted
// keys, merge the detector's candidates into the record table, evaluate
// every record, enforce the budget, publish the new admitted set, and build
// the window report.
func (e *Engine) Rotate(now time.Time) WindowReport {
	e.mu.Lock()
	eff := e.pol.Load()
	cur := e.admitted.Load()

	cached := e.harvestAdmitted(cur)
	// Ask for TopK candidates beyond the keys that are already admitted, so
	// the admitted set can grow to the cache budget instead of being capped
	// at TopK by its own incumbents.
	rs := e.reads.RotateK(now, e.cfg.Detector.TopK+len(cur.m))
	ws := e.writes.Rotate(now)
	secs := now.Sub(rs.Start).Seconds()
	if secs <= 0 {
		secs = 1e-3
	}

	e.mergeCandidates(rs.Top, secs)
	eligible := e.evaluateAll(eff, now, secs, windowCounts(ws.Top))
	next := e.applyBudget(eligible, eff.Budget)
	demoted := e.detach(cur, next)
	// If health flipped to unhealthy during evaluation, SetHealthy is
	// waiting on e.mu and will clear this set as soon as we release it;
	// Lookup already refuses to serve while unhealthy.
	e.admitted.Store(&admittedSet{m: next})

	rep := WindowReport{
		Start: rs.Start, End: now,
		Reads: rs.Observed, Recorded: rs.Recorded, Dropped: rs.Dropped,
		Writes:      ws.Recorded,
		CachedReads: cached.reads, Fills: cached.fills, Invalidations: cached.invalidations,
		Admitted: len(next), Tracked: rs.Tracked, Coverage: rs.Coverage(),
		PolicyVersion: eff.Version, PolicySource: eff.Source,
	}
	e.finishWindowLocked(&rep)
	e.last = rep
	e.mu.Unlock()

	e.demote(demoted)
	return rep
}

// cacheTotals sums cache-path counters over the admitted keys of a window.
type cacheTotals struct {
	reads, fills, invalidations uint64
}

// harvestAdmitted moves the cache-path counters of admitted keys into their
// records and folds their reads into the detector, so admitted keys are
// ranked with everything else.
func (e *Engine) harvestAdmitted(cur *admittedSet) cacheTotals {
	var t cacheTotals
	for key, ks := range cur.m {
		r := e.table[key]
		if r == nil { // should not happen; keep the engine consistent anyway
			r = &record{key: key, state: Admitted, size: -1}
			e.table[key] = r
		}
		r.reads = ks.reads.Swap(0)
		r.fills = ks.fills.Swap(0)
		r.invalidations = ks.invalidations.Swap(0)
		if sz := ks.size.Load(); sz >= 0 {
			r.size = sz
		}
		t.reads += r.reads
		t.fills += r.fills
		t.invalidations += r.invalidations
		r.seen = true
		e.reads.Add(key, r.reads, r.size)
	}
	return t
}

// mergeCandidates adds the detector's top keys to the table (bounded by
// maxTable) and records their window counts.
func (e *Engine) mergeCandidates(top []detector.Candidate, secs float64) {
	decay := float64(uint64(1) << e.reads.Config().DecayShift)
	for _, c := range top {
		r := e.table[c.Key]
		if r == nil {
			if len(e.table) >= e.maxTable {
				continue
			}
			r = &record{key: c.Key, state: Cold, size: -1}
			e.table[c.Key] = r
		}
		if r.stats == nil { // not admitted: detector count is the read count
			r.reads = c.Window
			r.fills, r.invalidations = 0, 0
			// The detector's decayed count is a long-memory estimate (with
			// halving per window it converges to twice the per-window
			// count). Its lower bound (Score-Err) is used as the read rate
			// of non-admitted keys so admission is not driven by a few
			// lucky windows.
			r.scoreRate = float64(c.Score-min(c.Err, c.Score)) / decay / secs
		}
		if c.SizeBytes >= 0 {
			r.size = c.SizeBytes
		}
		r.seen = true
	}
}

// windowCounts indexes detector candidates by key.
func windowCounts(top []detector.Candidate) map[string]uint64 {
	m := make(map[string]uint64, len(top))
	for _, c := range top {
		m[c.Key] = c.Window
	}
	return m
}

// evaluateAll updates the rates of every record, applies the state machine,
// and returns the records that qualify for admission.
func (e *Engine) evaluateAll(eff *policy.Effective, now time.Time, secs float64, writes map[string]uint64) []*record {
	stalenessFloor := 1 / e.cfg.MaxStaleness.Seconds()
	var eligible []*record
	for key, r := range e.table {
		if !r.seen {
			r.reads, r.fills, r.invalidations = 0, 0, 0
		}
		r.writes = writes[key]
		r.updateRates(secs)
		e.evaluate(r, eff, now, stalenessFloor)
		if r.state == Admitted {
			eligible = append(eligible, r)
		}
	}
	return eligible
}

// updateRates smooths read and write rates across windows (EWMA). Per-window
// counts of moderately hot keys are noisy; deciding on raw counts makes keys
// near a threshold flip in and out.
func (r *record) updateRates(secs float64) {
	rr, wr := float64(r.reads)/secs, float64(r.writes)/secs
	if r.stats == nil {
		if !r.seen {
			r.scoreRate = 0
		}
		// Conservative: the smaller of the window rate and the decayed
		// estimate must clear the thresholds.
		rr = min(rr, r.scoreRate)
	}
	if r.hasRate {
		r.readRate = rateAlpha*rr + (1-rateAlpha)*r.readRate
		r.writeRate = rateAlpha*wr + (1-rateAlpha)*r.writeRate
	} else {
		r.readRate, r.writeRate, r.hasRate = rr, wr, true
	}
	r.fillRate = float64(r.fills) / secs
	r.invRate = float64(r.invalidations) / secs
}

// cost is the estimated local cache footprint of r in bytes.
func (r *record) cost() int64 {
	return max(r.size, 0) + int64(len(r.key)) + EntryOverheadBytes
}

// density is the benefit per cached byte, with a stability bonus for keys
// that are already admitted.
func (r *record) density() float64 {
	d := r.savedRate / float64(r.cost())
	if r.stats != nil {
		d *= incumbentBonus
	}
	return d
}

// incumbentBonus favors keys that are already admitted when the budget is
// contended, so equally beneficial keys do not swap places every window.
const incumbentBonus = 1.25

// compareBudgetOrder orders records for the budget: pinned first, then by
// benefit density, then by key for determinism.
func compareBudgetOrder(a, b *record) int {
	if a.pinned != b.pinned {
		if a.pinned {
			return -1
		}
		return 1
	}
	if c := cmp.Compare(b.density(), a.density()); c != 0 {
		return c
	}
	return strings.Compare(a.key, b.key)
}

// applyBudget admits eligible records in budget order until the entry or
// memory budget is exhausted and returns the new admitted set.
func (e *Engine) applyBudget(eligible []*record, budget policy.Budget) map[string]*KeyStats {
	slices.SortFunc(eligible, compareBudgetOrder)
	next := make(map[string]*KeyStats, len(eligible))
	var usedBytes int64
	for _, r := range eligible {
		cost := r.cost()
		if len(next) >= budget.MaxEntries || usedBytes+cost > budget.MaxMemoryBytes {
			r.state = Candidate
			r.reason = "qualifies but the cache budget is exhausted"
			e.counters.BudgetRejected++
			continue
		}
		usedBytes += cost
		if r.stats == nil {
			r.stats = &KeyStats{}
			r.stats.size.Store(r.size)
			e.counters.Admissions++
		}
		next[r.key] = r.stats
	}
	return next
}

// detach clears the stats of records that left the admitted set and returns
// their keys.
func (e *Engine) detach(cur *admittedSet, next map[string]*KeyStats) []string {
	var demoted []string
	for key := range cur.m {
		if _, ok := next[key]; ok {
			continue
		}
		demoted = append(demoted, key)
		if r := e.table[key]; r != nil {
			r.stats = nil
		}
		e.counters.Demotions++
	}
	return demoted
}

// finishWindowLocked completes rep with per-state counts and the busiest
// keys, prunes cold records that were not seen, and resets per-window flags.
func (e *Engine) finishWindowLocked(rep *WindowReport) {
	keys := make([]KeyReport, 0, len(e.table))
	for key, r := range e.table {
		switch r.state {
		case Candidate:
			rep.Candidates++
		case Cooldown:
			rep.Cooldown++
		}
		if r.seen || r.state != Cold {
			keys = append(keys, r.report())
		}
		if !r.seen && r.state == Cold {
			delete(e.table, key)
		}
		r.seen = false
	}
	slices.SortFunc(keys, func(a, b KeyReport) int {
		if c := cmp.Compare(b.Reads, a.Reads); c != 0 {
			return c
		}
		return strings.Compare(a.Key, b.Key)
	})
	rep.Keys = keys[:min(len(keys), e.cfg.MaxReportKeys)]
	e.pruneLocked()
	e.counters.Rotations++
}

func (r *record) report() KeyReport {
	return KeyReport{
		Key: r.key, State: r.state, Reason: r.reason,
		Reads: r.reads, Writes: r.writes, Fills: r.fills, Invalidations: r.invalidations,
		SizeBytes: r.size, ReadRate: r.readRate, SavedRate: r.savedRate, HitRatio: r.hitRatio,
	}
}

// pruneLocked bounds the table when cooldown/bypassed records accumulate.
func (e *Engine) pruneLocked() {
	if len(e.table) <= e.maxTable {
		return
	}
	var victims []*record
	for _, r := range e.table {
		if r.stats == nil {
			victims = append(victims, r)
		}
	}
	slices.SortFunc(victims, func(a, b *record) int {
		switch {
		case a.reads < b.reads:
			return -1
		case a.reads > b.reads:
			return 1
		}
		return 0
	})
	for _, r := range victims {
		if len(e.table) <= e.maxTable {
			return
		}
		delete(e.table, r.key)
	}
}
