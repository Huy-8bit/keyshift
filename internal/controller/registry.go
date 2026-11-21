package controller

import (
	"errors"
	"sync"
	"time"

	"github.com/Huy-8bit/keyshift/internal/telemetry"
)

// instanceRecord holds the latest accepted telemetry for one SDK instance.
// Only the latest batch is retained, so memory per instance is bounded by
// the per-instance candidate limit.
type instanceRecord struct {
	service      string
	instance     string
	firstSeen    time.Time
	lastSeen     time.Time
	lastAccepted time.Time
	lastSeq      uint64
	batches      uint64
	duplicates   uint64
	latest       *telemetry.Batch
}

// IngestResult describes the outcome of an ingestion attempt.
type IngestResult int

const (
	IngestAccepted IngestResult = iota
	IngestDuplicate
	IngestRateLimited
	IngestCapacity
)

var (
	errCapacity    = errors.New("instance capacity reached")
	errRateLimited = errors.New("instance reporting too frequently")
)

// Registry tracks SDK instances. It is bounded by maxInstances; expired
// instances are removed by Sweep.
type Registry struct {
	mu               sync.RWMutex
	instances        map[string]*instanceRecord
	maxInstances     int
	maxCandidates    int
	minInterval      time.Duration
	ttl              time.Duration
	services         map[string]int // active instance count per service
	maxServices      int
	evicted, expired uint64
}

// NewRegistry creates a registry.
func NewRegistry(maxInstances, maxServices, maxCandidates int, minInterval, ttl time.Duration) *Registry {
	return &Registry{instances: map[string]*instanceRecord{}, services: map[string]int{},
		maxInstances: maxInstances, maxServices: maxServices, maxCandidates: maxCandidates,
		minInterval: minInterval, ttl: ttl}
}

func instanceKey(service, instance string) string { return service + "\x00" + instance }

// Ingest stores b. Duplicate or stale sequence numbers are acknowledged but
// not aggregated, which makes retries idempotent.
func (r *Registry) Ingest(b *telemetry.Batch, now time.Time) (IngestResult, error) {
	if len(b.Candidates) > r.maxCandidates {
		b.Candidates = b.Candidates[:r.maxCandidates]
	}
	key := instanceKey(b.Service, b.Instance)
	r.mu.Lock()
	defer r.mu.Unlock()
	rec := r.instances[key]
	if rec == nil {
		if len(r.instances) >= r.maxInstances {
			r.evicted++
			return IngestCapacity, errCapacity
		}
		if _, ok := r.services[b.Service]; !ok && len(r.services) >= r.maxServices {
			r.evicted++
			return IngestCapacity, errors.New("service capacity reached")
		}
		rec = &instanceRecord{service: b.Service, instance: b.Instance, firstSeen: now}
		r.instances[key] = rec
		r.services[b.Service]++
	}
	rec.lastSeen = now
	if rec.latest != nil && b.Seq <= rec.lastSeq {
		rec.duplicates++
		return IngestDuplicate, nil
	}
	if r.minInterval > 0 && !rec.lastAccepted.IsZero() && now.Sub(rec.lastAccepted) < r.minInterval {
		return IngestRateLimited, errRateLimited
	}
	rec.lastSeq = b.Seq
	rec.lastAccepted = now
	rec.batches++
	rec.latest = b
	return IngestAccepted, nil
}

// Sweep removes instances not seen for longer than the TTL.
func (r *Registry) Sweep(now time.Time) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for k, rec := range r.instances {
		if now.Sub(rec.lastSeen) > r.ttl {
			delete(r.instances, k)
			if r.services[rec.service]--; r.services[rec.service] <= 0 {
				delete(r.services, rec.service)
			}
			n++
		}
	}
	r.expired += uint64(n)
	return n
}

// Len returns the number of registered instances.
func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.instances)
}

// snapshot copies the instance records for aggregation without holding the
// lock while aggregating.
func (r *Registry) snapshot() []instanceRecord {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]instanceRecord, 0, len(r.instances))
	for _, rec := range r.instances {
		out = append(out, *rec)
	}
	return out
}
