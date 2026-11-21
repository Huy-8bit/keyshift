package bench

import (
	"math/rand/v2"
	"strconv"
	"time"
)

// Distribution selects how keys are chosen.
type Distribution string

const (
	Uniform   Distribution = "uniform"
	Zipf      Distribution = "zipf"
	SingleHot Distribution = "single-hot"
	LocalHot  Distribution = "local-hot"
	Shifting  Distribution = "shifting"
)

// Fault is a fault injected halfway through the measured phase.
type Fault string

const (
	NoFault              Fault = ""
	FaultTrackingKill    Fault = "tracking-interruption"
	FaultRedisRestart    Fault = "redis-restart"
	FaultControllerDown  Fault = "controller-outage"
	FaultStartupSurgeRun Fault = "startup-surge"
)

// Scenario is a reproducible workload definition.
type Scenario struct {
	Name        string       `json:"name"`
	Description string       `json:"description"`
	Dist        Distribution `json:"distribution"`
	Keys        int          `json:"keys"`
	ZipfS       float64      `json:"zipf_s,omitempty"`
	// ReadRatio is the fraction of operations that are reads.
	ReadRatio float64 `json:"read_ratio"`
	// HotWrites directs writes at the hottest keys (worst case for caching).
	HotWrites bool `json:"hot_writes,omitempty"`
	ValueSize int  `json:"value_size"`
	// TTL sets an expiry on written keys; a refresher rewrites them.
	TTL time.Duration `json:"ttl,omitempty"`
	// ShiftEvery rotates the hot set (Shifting distribution).
	ShiftEvery time.Duration `json:"shift_every,omitempty"`
	// Burst alternates 1s of full load with 1s of 10% load.
	Burst bool `json:"burst,omitempty"`
	// Instances overrides the number of virtual SDK instances.
	Instances int   `json:"instances,omitempty"`
	Fault     Fault `json:"fault,omitempty"`
	// Cluster runs against Redis Cluster.
	Cluster bool `json:"cluster,omitempty"`
	// Unfavorable marks workloads where local caching is expected to help
	// little or not at all.
	Unfavorable bool `json:"unfavorable,omitempty"`
}

// Scenarios returns the built-in scenario catalogue.
func Scenarios() []Scenario {
	return []Scenario{
		{Name: "uniform", Description: "Uniform random reads over 100k keys (no locality)", Dist: Uniform, Keys: 100_000, ReadRatio: 0.95, ValueSize: 128, Unfavorable: true},
		{Name: "zipf", Description: "Zipfian (s=1.1) reads over 100k keys, 5% writes", Dist: Zipf, Keys: 100_000, ZipfS: 1.1, ReadRatio: 0.95, ValueSize: 128},
		{Name: "single-hot", Description: "One key receives 50% of reads; the rest are uniform", Dist: SingleHot, Keys: 100_000, ReadRatio: 0.99, ValueSize: 128},
		{Name: "local-hot", Description: "Each of 4 instances has its own hot set (80% of its reads); globally moderate", Dist: LocalHot, Keys: 100_000, ReadRatio: 0.98, ValueSize: 128, Instances: 4},
		{Name: "read-only-zipf", Description: "Zipfian read-only traffic on immutable keys", Dist: Zipf, Keys: 100_000, ZipfS: 1.1, ReadRatio: 1, ValueSize: 128},
		{Name: "write-heavy", Description: "Zipfian with 50% writes aimed at the hottest keys", Dist: Zipf, Keys: 10_000, ZipfS: 1.1, ReadRatio: 0.5, HotWrites: true, ValueSize: 128, Unfavorable: true},
		{Name: "expiring", Description: "Zipfian reads of keys with a 2s TTL that are rewritten as they expire", Dist: Zipf, Keys: 10_000, ZipfS: 1.1, ReadRatio: 0.98, ValueSize: 128, TTL: 2 * time.Second},
		{Name: "small-values", Description: "Zipfian reads, 16-byte values", Dist: Zipf, Keys: 100_000, ZipfS: 1.1, ReadRatio: 0.98, ValueSize: 16},
		{Name: "large-values", Description: "Zipfian reads, 32 KiB values", Dist: Zipf, Keys: 2_000, ZipfS: 1.1, ReadRatio: 0.98, ValueSize: 32 << 10},
		{Name: "oversized-values", Description: "Zipfian reads, 128 KiB values (above max_entry_bytes)", Dist: Zipf, Keys: 500, ZipfS: 1.1, ReadRatio: 0.98, ValueSize: 128 << 10, Unfavorable: true},
		{Name: "hot-shift", Description: "Zipfian whose hot set moves every 5s", Dist: Shifting, Keys: 100_000, ZipfS: 1.1, ReadRatio: 0.98, ValueSize: 128, ShiftEvery: 5 * time.Second},
		{Name: "burst", Description: "Zipfian with alternating 1s bursts and 1s lulls", Dist: Zipf, Keys: 100_000, ZipfS: 1.1, ReadRatio: 0.98, ValueSize: 128, Burst: true},
		{Name: "redis-restart", Description: "Zipfian; Redis is restarted halfway (requires --restart-container)", Dist: Zipf, Keys: 10_000, ZipfS: 1.1, ReadRatio: 0.98, ValueSize: 128, Fault: FaultRedisRestart},
		{Name: "tracking-interruption", Description: "Zipfian; client connections are killed halfway", Dist: Zipf, Keys: 100_000, ZipfS: 1.1, ReadRatio: 0.98, ValueSize: 128, Fault: FaultTrackingKill},
		{Name: "startup-surge", Description: "32 instances start simultaneously with no warmup", Dist: Zipf, Keys: 100_000, ZipfS: 1.1, ReadRatio: 0.98, ValueSize: 128, Instances: 32, Fault: FaultStartupSurgeRun},
		{Name: "controller-outage", Description: "Zipfian; KeyShift telemetry endpoint is unreachable", Dist: Zipf, Keys: 100_000, ZipfS: 1.1, ReadRatio: 0.98, ValueSize: 128, Fault: FaultControllerDown},
		{Name: "cluster-zipf", Description: "Zipfian on Redis Cluster (requires --cluster)", Dist: Zipf, Keys: 100_000, ZipfS: 1.1, ReadRatio: 0.98, ValueSize: 128, Cluster: true},
	}
}

// keyName returns the Redis key for index i.
func keyName(i int) string { return "ksbench:" + strconv.Itoa(i) }

// picker chooses key indexes for one worker.
type picker struct {
	s        Scenario
	r        *rand.Rand
	z        *rand.Zipf
	instance int
	start    time.Time
	// perm scrambles zipf ranks so hot keys are spread across slots.
	perm []int32
}

func newPicker(s Scenario, seed uint64, instance int, perm []int32, start time.Time) *picker {
	r := rand.New(rand.NewPCG(seed, seed*0x9e3779b97f4a7c15+1))
	p := &picker{s: s, r: r, instance: instance, perm: perm, start: start}
	if s.ZipfS > 1 {
		p.z = rand.NewZipf(r, s.ZipfS, 1, uint64(s.Keys-1))
	}
	return p
}

func (p *picker) zipf() int {
	if p.z == nil {
		return p.r.IntN(p.s.Keys)
	}
	return int(p.perm[p.z.Uint64()])
}

// next returns the key index for a read.
func (p *picker) next(now time.Time) int {
	switch p.s.Dist {
	case Zipf:
		return p.zipf()
	case SingleHot:
		if p.r.IntN(2) == 0 {
			return 0
		}
		return p.r.IntN(p.s.Keys)
	case LocalHot:
		// 80% of reads go to 50 keys owned by this instance.
		if p.r.IntN(10) < 8 {
			return (p.instance*50 + p.r.IntN(50)) % p.s.Keys
		}
		return p.r.IntN(p.s.Keys)
	case Shifting:
		epoch := int(now.Sub(p.start) / p.s.ShiftEvery)
		return (p.zipf() + epoch*7919) % p.s.Keys
	default:
		return p.r.IntN(p.s.Keys)
	}
}

// nextWrite returns the key index for a write.
func (p *picker) nextWrite(now time.Time) int {
	if p.s.HotWrites {
		return p.next(now)
	}
	return p.r.IntN(p.s.Keys)
}
