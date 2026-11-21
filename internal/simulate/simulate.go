// Package simulate runs a large-scale logical simulation of KeyShift
// telemetry: thousands of virtual SDK instances upload realistic batches to
// a controller over real HTTP.
//
// This is a logical simulation of the control plane, not a deployment of
// thousands of pods: virtual instances do not run Redis workloads, and their
// telemetry is synthesized from per-service Zipfian key distributions.
package simulate

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Huy-8bit/keyshift/internal/controller"
	"github.com/Huy-8bit/keyshift/internal/telemetry"
)

// Config configures a simulation.
type Config struct {
	Services            int
	InstancesPerService int
	CandidatesPerBatch  int
	KeySpace            int
	Rounds              int
	Concurrency         int
	// ControllerURL targets an external controller; empty starts one
	// in-process on a loopback listener.
	ControllerURL string
	Token         string
	// Paced spreads each round's uploads over this interval (0 = as fast
	// as possible, which measures capacity).
	Paced time.Duration
	Seed  uint64
	// InstancePrefix makes instance IDs unique when several simulators
	// target one controller.
	InstancePrefix string
}

// DefaultConfig models 100 services x 50 instances x 20 candidates.
func DefaultConfig() Config {
	return Config{Services: 100, InstancesPerService: 50, CandidatesPerBatch: 20, KeySpace: 1_000_000,
		Rounds: 3, Concurrency: 64, Seed: 1}
}

// RoundResult measures one upload round.
type RoundResult struct {
	Round         int     `json:"round"`
	Batches       int     `json:"batches"`
	Records       int     `json:"records"`
	Seconds       float64 `json:"seconds"`
	BatchesPerSec float64 `json:"batches_per_sec"`
	RecordsPerSec float64 `json:"records_per_sec"`
	P50ms         float64 `json:"p50_ms"`
	P99ms         float64 `json:"p99_ms"`
	MaxMs         float64 `json:"max_ms"`
	Errors        int     `json:"errors"`
	Rejected      int     `json:"rejected"`
	// FirstFailure describes the first transport error or non-200 reply.
	FirstFailure  string  `json:"first_failure,omitempty"`
	BytesSent     int64   `json:"bytes_sent"`
	AvgBatchBytes float64 `json:"avg_batch_bytes"`
}

// Result is the simulation outcome.
type Result struct {
	Config            Config        `json:"config"`
	Instances         int           `json:"instances"`
	Rounds            []RoundResult `json:"rounds"`
	AggregationMs     float64       `json:"aggregation_ms"`
	ActiveInstances   int           `json:"active_instances"`
	GlobalKeys        int           `json:"global_keys"`
	TopKeyInstances   int           `json:"top_key_instances"`
	TopKeyServices    int           `json:"top_key_services"`
	HeapInuseMB       float64       `json:"heap_inuse_mb"`
	InProcess         bool          `json:"in_process"`
	GoMaxProcs        int           `json:"gomaxprocs"`
	EncodeSecondsMean float64       `json:"encode_seconds_total"`
}

type instance struct {
	service, id string
	seq         uint64
	keys        []int
}

// Run executes the simulation.
func Run(ctx context.Context, cfg Config) (*Result, error) {
	res := &Result{Config: cfg, GoMaxProcs: runtime.GOMAXPROCS(0)}
	url := cfg.ControllerURL
	var srv *controller.Server
	if url == "" {
		res.InProcess = true
		token := "simulation-token-0123456789"
		cc := controller.DefaultConfig()
		cc.Auth = controller.NewAuth([]string{token}, nil, nil)
		cc.PerInstanceMinInterval = 0
		cc.IngestRatePerSecond = 0
		cc.MaxInstances = max(cc.MaxInstances, cfg.Services*cfg.InstancesPerService*2)
		cc.MaxCandidatesPerBatch = cfg.CandidatesPerBatch
		var err error
		if srv, err = controller.New(cc); err != nil {
			return nil, err
		}
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		hs := &http.Server{Handler: srv.Handler(), ReadHeaderTimeout: 5 * time.Second}
		go hs.Serve(ln)
		defer hs.Close()
		url = "http://" + ln.Addr().String()
		cfg.Token = token
	}

	r := rand.New(rand.NewPCG(cfg.Seed, 99))
	zipf := rand.NewZipf(r, 1.1, 1, uint64(cfg.KeySpace-1))
	var insts []*instance
	for s := range cfg.Services {
		svc := fmt.Sprintf("service-%03d", s)
		// Each service mixes globally shared hot keys with its own keys.
		offset := s * 7919
		for i := range cfg.InstancesPerService {
			in := &instance{service: svc, id: fmt.Sprintf("%s%s-pod-%02d", cfg.InstancePrefix, svc, i)}
			seen := map[int]bool{}
			for len(in.keys) < cfg.CandidatesPerBatch {
				k := int(zipf.Uint64())
				if r.IntN(2) == 0 {
					k = (k + offset) % cfg.KeySpace
				}
				if !seen[k] {
					seen[k] = true
					in.keys = append(in.keys, k)
				}
			}
			insts = append(insts, in)
		}
	}
	res.Instances = len(insts)

	tr := &http.Transport{MaxIdleConns: cfg.Concurrency * 2, MaxIdleConnsPerHost: cfg.Concurrency * 2}
	hc := &http.Client{Timeout: 30 * time.Second, Transport: tr}
	defer tr.CloseIdleConnections()
	var encodeNs atomic.Int64

	for round := 1; round <= cfg.Rounds; round++ {
		rr := RoundResult{Round: round, Batches: len(insts)}
		lat := make([]time.Duration, len(insts))
		var errs, rejected atomic.Int64
		var firstFailure atomic.Pointer[string]
		fail := func(msg string) { firstFailure.CompareAndSwap(nil, &msg) }
		var sent atomic.Int64
		jobs := make(chan int)
		var wg sync.WaitGroup
		start := time.Now()
		for range cfg.Concurrency {
			wg.Go(func() {
				lr := rand.New(rand.NewPCG(cfg.Seed+uint64(round), uint64(time.Now().UnixNano())))
				for idx := range jobs {
					in := insts[idx]
					in.seq++
					t0 := time.Now()
					body, gz, err := telemetry.Encode(makeBatch(in, lr, cfg.CandidatesPerBatch))
					encodeNs.Add(int64(time.Since(t0)))
					if err != nil {
						errs.Add(1)
						continue
					}
					req, _ := http.NewRequestWithContext(ctx, "POST", url+"/v1/telemetry", bytes.NewReader(body))
					req.Header.Set("Authorization", "Bearer "+cfg.Token)
					if gz {
						req.Header.Set("Content-Encoding", "gzip")
					}
					t1 := time.Now()
					resp, err := hc.Do(req)
					lat[idx] = time.Since(t1)
					sent.Add(int64(len(body)))
					if err != nil {
						errs.Add(1)
						fail(err.Error())
						continue
					}
					if resp.StatusCode != 200 {
						rejected.Add(1)
						msg, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
						fail(resp.Status + ": " + string(msg))
					}
					_, _ = io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
				}
			})
		}
		for i := range insts {
			if cfg.Paced > 0 {
				target := start.Add(time.Duration(float64(cfg.Paced) * float64(i) / float64(len(insts))))
				if d := time.Until(target); d > 0 {
					time.Sleep(d)
				}
			}
			select {
			case jobs <- i:
			case <-ctx.Done():
			}
		}
		close(jobs)
		wg.Wait()
		rr.Seconds = time.Since(start).Seconds()
		slices.Sort(lat)
		rr.Records = len(insts) * cfg.CandidatesPerBatch
		rr.BatchesPerSec = float64(rr.Batches) / rr.Seconds
		rr.RecordsPerSec = float64(rr.Records) / rr.Seconds
		rr.P50ms = float64(lat[len(lat)/2].Microseconds()) / 1000
		rr.P99ms = float64(lat[len(lat)*99/100].Microseconds()) / 1000
		rr.MaxMs = float64(lat[len(lat)-1].Microseconds()) / 1000
		rr.Errors, rr.Rejected = int(errs.Load()), int(rejected.Load())
		if p := firstFailure.Load(); p != nil {
			rr.FirstFailure = *p
		}
		rr.BytesSent = sent.Load()
		rr.AvgBatchBytes = float64(rr.BytesSent) / float64(rr.Batches)
		res.Rounds = append(res.Rounds, rr)
		if ctx.Err() != nil {
			return res, ctx.Err()
		}
	}
	res.EncodeSecondsMean = float64(encodeNs.Load()) / 1e9
	if srv != nil {
		t0 := time.Now()
		v := srv.Aggregate(time.Now())
		res.AggregationMs = float64(time.Since(t0).Microseconds()) / 1000
		res.ActiveInstances = v.Overview.ActiveInstances
		res.GlobalKeys = len(v.Keys)
		if len(v.Keys) > 0 {
			res.TopKeyInstances = v.Keys[0].Instances
			res.TopKeyServices = len(v.Keys[0].Services)
		}
		runtime.GC()
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		res.HeapInuseMB = float64(ms.HeapInuse) / (1 << 20)
	}
	return res, nil
}

var states = []string{"admitted", "admitted", "candidate", "cold", "cold", "cooldown", "bypassed"}

func makeBatch(in *instance, r *rand.Rand, n int) *telemetry.Batch {
	now := time.Now()
	b := &telemetry.Batch{SchemaVersion: telemetry.SchemaVersion, Service: in.service, Instance: in.id, Seq: in.seq,
		SDKVersion: "0.1.0", Driver: "go-redis", Topology: "standalone",
		WindowStart: now.Add(-30 * time.Second), WindowEnd: now,
		FingerprintScope: "shared", FingerprintKeyID: "sim00001", PolicySource: "local", CacheMode: "adaptive",
		Tracking: telemetry.Tracking{State: "healthy", Mode: "default"},
		Totals:   telemetry.Totals{Reads: 300_000, CachedReads: 120_000, LocalHits: 110_000, RedisReads: 190_000, Writes: 9_000, Invalidations: 2_000},
		Cache:    telemetry.Cache{Entries: 400, MemoryBytes: 2 << 20, MaxEntries: 1000, MaxMemoryBytes: 16 << 20, Admitted: 40},
		Detector: telemetry.Detector{Observed: 300_000, Recorded: 300_000, Tracked: 1000, Coverage: 0.6},
		Runtime:  telemetry.Runtime{HeapAllocBytes: 64 << 20, Goroutines: 120},
	}
	b.Candidates = make([]telemetry.Candidate, 0, n)
	for rank, k := range in.keys[:n] {
		reads := uint64(30_000/(rank+1)) + uint64(r.IntN(100))
		st := states[r.IntN(len(states))]
		if rank < 3 {
			st = "admitted"
		}
		b.Candidates = append(b.Candidates, telemetry.Candidate{
			FP: fmt.Sprintf("%032x", uint64(k)*0x9e3779b97f4a7c15), Reads: reads, Writes: uint64(r.IntN(50)),
			Fills: reads / 50, SizeBytes: int64(64 + r.IntN(4096)), State: st, SavedRate: float64(reads) / 30 * 0.9, HitRatio: 0.9,
		})
	}
	return b
}
