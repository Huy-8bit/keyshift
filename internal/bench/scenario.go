package bench

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// worker accumulates one load goroutine's measurements.
type worker struct {
	hist                       histogram
	ops, reads, writes, errors uint64
	stale                      uint64
	maxStale                   time.Duration
}

func (wk *worker) record(latency time.Duration, isWrite bool, err error) {
	wk.hist.record(latency)
	wk.ops++
	if isWrite {
		wk.writes++
	} else {
		wk.reads++
	}
	if err != nil {
		wk.errors++
	}
}

// runOne runs one scenario in one mode: start the simulated instances,
// drive the workload through a warmup and a measured interval, and
// summarize client- and Redis-side measurements.
func runOne(ctx context.Context, cfg Config, s Scenario, mode string) (Result, error) {
	res := Result{Scenario: s.Name, Mode: mode, Unfavorable: s.Unfavorable, Concurrency: cfg.Concurrency, TargetRate: cfg.Rate}
	res.Instances = cfg.Instances
	if s.Instances > 0 {
		res.Instances = s.Instances
	}
	if err := preload(ctx, cfg, s, s.Cluster); err != nil {
		return res, fmt.Errorf("preload: %w", err)
	}
	nodes, closeNodes, err := adminNodes(ctx, cfg, s.Cluster)
	if err != nil {
		return res, err
	}
	defer closeNodes()

	targets, slowestStart, err := startTargets(cfg, s, mode, res.Instances)
	defer closeTargets(targets)
	if err != nil {
		return res, err
	}
	if s.Fault == FaultStartupSurgeRun {
		res.StartupMsMax = float64(slowestStart.Microseconds()) / 1000
	}

	ld := newLoad(cfg, s, targets)
	ld.start(ctx)
	warmup := cfg.Warmup
	if s.Fault == FaultStartupSurgeRun {
		warmup = 0 // the startup surge itself is measured
	}
	sleepCtx(ctx, warmup)
	m, err := ld.measure(ctx, nodes)
	if err != nil {
		return res, err
	}
	res.summarize(ld.workers, targets, m)
	return res, nil
}

// startTargets creates one client per simulated instance concurrently and
// returns them with the slowest startup time. Targets created before an
// error are returned so the caller can close them.
func startTargets(cfg Config, s Scenario, mode string, n int) ([]target, time.Duration, error) {
	opts := targetOptions{mode: mode, addr: cfg.RedisAddr, maxEntries: cfg.MaxEntries, maxBytes: cfg.MaxBytes,
		maxStaleness: cfg.MaxStaleness, window: cfg.Window, trackedPool: cfg.TrackedPoolSize, drain: cfg.DrainInterval}
	if s.Cluster {
		opts.clusterAddrs = cfg.ClusterAddrs
	}
	if s.Fault == FaultControllerDown {
		opts.telemetryURL = "http://127.0.0.1:1"
	}
	targets := make([]target, n)
	startup := make([]time.Duration, n)
	var wg sync.WaitGroup
	var startErr atomic.Pointer[error]
	for i := range n {
		wg.Go(func() {
			o := opts
			o.name = "ksbench-" + mode + "-" + strconv.Itoa(i)
			t0 := time.Now()
			t, err := newTarget(o)
			if err != nil {
				startErr.Store(&err)
				return
			}
			startup[i] = time.Since(t0)
			targets[i] = t
		})
	}
	wg.Wait()
	if p := startErr.Load(); p != nil {
		return targets, 0, *p
	}
	var slowest time.Duration
	for _, d := range startup {
		slowest = max(slowest, d)
	}
	return targets, slowest, nil
}

func closeTargets(targets []target) {
	for _, t := range targets {
		if t != nil {
			t.Close()
		}
	}
}

// load drives a scenario's workload against the targets with
// cfg.Concurrency workers, spread round-robin over the instances.
type load struct {
	cfg     Config
	s       Scenario
	targets []target
	perm    []int32 // key rank -> key index, so hot keys are spread out
	vs      *versions
	seq     atomic.Uint64

	measuring, stop atomic.Bool
	began           time.Time
	workers         []*worker
	wg              sync.WaitGroup
}

func newLoad(cfg Config, s Scenario, targets []target) *load {
	l := &load{cfg: cfg, s: s, targets: targets, perm: keyPermutation(s.Keys, cfg.Seed), vs: newVersions(s.Keys)}
	l.seq.Store(1)
	return l
}

func keyPermutation(n int, seed uint64) []int32 {
	perm := make([]int32, n)
	for i := range perm {
		perm[i] = int32(i)
	}
	r := rand.New(rand.NewPCG(seed, 7))
	r.Shuffle(n, func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
	return perm
}

func (l *load) start(ctx context.Context) {
	l.began = time.Now()
	l.workers = make([]*worker, l.cfg.Concurrency)
	for w := range l.cfg.Concurrency {
		wk := &worker{}
		l.workers[w] = wk
		inst := w % len(l.targets)
		p := newPicker(l.s, l.cfg.Seed+uint64(w)*1000003, inst, l.perm, l.began)
		l.wg.Go(func() { l.run(ctx, w, wk, l.targets[inst], p) })
	}
}

// halt stops the workers and waits for them.
func (l *load) halt() {
	l.stop.Store(true)
	l.wg.Wait()
}

// run is one worker's loop. In open-loop mode (cfg.Rate > 0) each worker
// issues operations on a fixed schedule and latency is measured from the
// scheduled start, which avoids coordinated omission.
func (l *load) run(ctx context.Context, w int, wk *worker, t target, p *picker) {
	cfg, s := l.cfg, l.s
	r := rand.New(rand.NewPCG(cfg.Seed^uint64(w), 3))
	var interval time.Duration
	next := time.Now()
	if cfg.Rate > 0 {
		interval = time.Duration(float64(cfg.Concurrency) / cfg.Rate * float64(time.Second))
		next = next.Add(time.Duration(r.Int64N(int64(interval) + 1)))
	}
	for !l.stop.Load() {
		var scheduled time.Time
		if interval > 0 {
			scheduled, next = pace(next, interval)
		}
		now := time.Now()
		if s.Burst && int(now.Sub(l.began)/time.Second)%2 == 1 && w >= max(1, cfg.Concurrency/10) {
			time.Sleep(time.Millisecond) // quiet second: only 10% of the workers run
			continue
		}
		isWrite := r.Float64() >= s.ReadRatio
		var k int
		if isWrite {
			k = p.nextWrite(now)
		} else {
			k = p.next(now)
		}
		key := keyName(k)
		t0 := time.Now()
		if !scheduled.IsZero() {
			t0 = scheduled
		}
		err := l.op(ctx, t, wk, k, key, isWrite)
		if l.measuring.Load() {
			wk.record(time.Since(t0), isWrite, err)
		}
	}
}

// pace sleeps until the next scheduled start and returns it together with
// the one after. A worker far behind schedule restarts from now instead of
// building an unbounded backlog.
func pace(next time.Time, interval time.Duration) (scheduled, following time.Time) {
	if d := time.Until(next); d > 0 {
		time.Sleep(d)
	} else if -d > time.Second {
		next = time.Now()
	}
	return next, next.Add(interval)
}

// op performs one write, or one cache-aside read that counts stale values
// (older than the version committed before the read started).
func (l *load) op(ctx context.Context, t target, wk *worker, k int, key string, isWrite bool) error {
	if isWrite {
		return write(ctx, t, l.vs, &l.seq, k, key, l.s)
	}
	floor := l.vs.v[k].Load()
	b, err := t.Get(ctx, key)
	if errors.Is(err, errNil) {
		return write(ctx, t, l.vs, &l.seq, k, key, l.s) // load and populate on a miss
	}
	if err == nil && l.measuring.Load() && versionOf(b) < floor {
		wk.stale++
		if at := l.vs.at[k].Load(); at > 0 {
			wk.maxStale = max(wk.maxStale, time.Since(time.Unix(0, at)))
		}
	}
	return err
}

// measurement holds what is sampled at the start and end of the measured
// interval.
type measurement struct {
	elapsed                 time.Duration
	redisBefore, redisAfter redisStats
	targetsBefore           []targetStats
	mem0, mem1              runtime.MemStats
	cpu0, cpu1              time.Duration
	peakHeap                uint64 // written by sample only
	faultNote               string // written by sample only
}

// measure runs the measured interval and stops the load. The scenario's
// fault, if any, is injected halfway through.
func (l *load) measure(ctx context.Context, nodes []*redis.Client) (measurement, error) {
	var m measurement
	var err error
	if m.redisBefore, err = snapshot(ctx, nodes); err != nil {
		l.halt()
		return m, err
	}
	for _, t := range l.targets {
		m.targetsBefore = append(m.targetsBefore, t.Stats())
	}
	runtime.GC()
	runtime.ReadMemStats(&m.mem0)
	m.cpu0 = processCPU()
	l.measuring.Store(true)
	mStart := time.Now()
	sampled := make(chan struct{})
	go l.sample(ctx, nodes, mStart, &m, sampled)

	sleepCtx(ctx, l.cfg.Duration)
	m.elapsed = time.Since(mStart)
	l.measuring.Store(false)
	m.cpu1 = processCPU()
	runtime.ReadMemStats(&m.mem1)
	m.redisAfter, err = snapshot(ctx, nodes)
	l.halt()
	<-sampled
	return m, err
}

// sample tracks the peak heap and injects the fault until the load stops.
func (l *load) sample(ctx context.Context, nodes []*redis.Client, mStart time.Time, m *measurement, done chan<- struct{}) {
	defer close(done)
	tk := time.NewTicker(500 * time.Millisecond)
	defer tk.Stop()
	faulted := false
	for !l.stop.Load() {
		<-tk.C
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		m.peakHeap = max(m.peakHeap, ms.HeapInuse)
		if !faulted && time.Since(mStart) >= l.cfg.Duration/2 {
			faulted = true
			m.faultNote = injectFault(ctx, l.cfg, l.s, nodes)
		}
	}
}

// summarize fills res from the workers, the targets, and the measurement.
func (res *Result) summarize(workers []*worker, targets []target, m measurement) {
	if m.faultNote != "" {
		res.Notes = append(res.Notes, m.faultNote)
	}
	var h histogram
	for _, wk := range workers {
		h.merge(&wk.hist)
		res.Ops += wk.ops
		res.Reads += wk.reads
		res.Writes += wk.writes
		res.Errors += wk.errors
		res.StaleReads += wk.stale
		res.MaxStaleMs = max(res.MaxStaleMs, float64(wk.maxStale.Microseconds())/1000)
	}
	res.Seconds = m.elapsed.Seconds()
	res.Throughput = float64(res.Ops) / res.Seconds
	res.P50us, res.P95us = h.quantile(0.50)/1e3, h.quantile(0.95)/1e3
	res.P99us, res.P999us = h.quantile(0.99)/1e3, h.quantile(0.999)/1e3
	for i, t := range targets {
		a, b := t.Stats(), m.targetsBefore[i]
		res.LocalHits += a.LocalHits - b.LocalHits
		res.Invalidations += a.Invalidations - b.Invalidations
		res.CacheEntries += a.Entries
		res.CacheMemory += a.MemoryBytes
		res.AdmittedKeys += a.Admitted
	}
	if res.Reads > 0 {
		res.HitRatio = min(1, float64(res.LocalHits)/float64(res.Reads))
		res.StaleRatio = float64(res.StaleReads) / float64(res.Reads)
	}

	before, after := m.redisBefore, m.redisAfter
	if after.getCalls < before.getCalls || after.cmdCalls < before.cmdCalls || after.cpu < before.cpu {
		// Server counters were reset (Redis restarted during the run): only
		// the post-restart part can be measured, so it is a lower bound.
		res.Notes = append(res.Notes, "Redis counters reset by the restart: Redis GET/s and CPU cover only the post-restart part (lower bound)")
		before = redisStats{}
	}
	res.RedisGetCalls = after.getCalls - before.getCalls
	res.RedisGetPerSec = float64(res.RedisGetCalls) / res.Seconds
	res.RedisCmdPerSec = float64(after.cmdCalls-before.cmdCalls) / res.Seconds
	res.RedisCPUSeconds = after.cpu - before.cpu
	res.RedisUsedMemory = after.usedMemory
	res.TrackingKeys, res.TrackingItems, res.TrackingClients = after.trackKeys, after.trackItem, after.trackClients

	if res.Ops > 0 {
		res.AllocBytesPerOp = float64(m.mem1.TotalAlloc-m.mem0.TotalAlloc) / float64(res.Ops)
		res.AllocsPerOp = float64(m.mem1.Mallocs-m.mem0.Mallocs) / float64(res.Ops)
		res.ClientCPUusOp = float64((m.cpu1 - m.cpu0).Microseconds()) / float64(res.Ops)
	}
	res.GCCycles = m.mem1.NumGC - m.mem0.NumGC
	res.GCPauseMs = float64(m.mem1.PauseTotalNs-m.mem0.PauseTotalNs) / 1e6
	res.PeakHeapMB = float64(m.peakHeap) / (1 << 20)
}
