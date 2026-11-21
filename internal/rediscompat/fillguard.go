package rediscompat

import (
	"hash/maphash"
	"sync"
	"sync/atomic"
)

// fillGuard closes a read-your-writes race between local writes and cache
// fills that were already in flight:
//
//  1. goroutine A starts a cached GET (reservation) and Redis returns v1;
//  2. goroutine B writes v2 through the untracked client and drops the
//     local entry (Forget);
//  3. A's fill completes after B's Forget and caches v1.
//
// v1 would then be served until the server invalidation for A's connection
// is processed (usually a few milliseconds later). The guard records a
// sequence number when a fill starts and a per-key-bucket sequence when a
// local write forgets a key; a fill that started before a later forget of
// its key is not cached. Buckets are shared by hash, so collisions only cause
// extra (safe) rejections. Memory is constant.
type fillGuard struct {
	seq     atomic.Uint64
	seed    maphash.Seed
	forgets [fillGuardBuckets]atomic.Uint64

	mu      sync.Mutex
	pending map[uint64]pendingFill // reservation token -> fill start (bounded by in-flight fills)
}

const fillGuardBuckets = 4096

type pendingFill struct {
	start  uint64
	bucket uint32
}

func newFillGuard() *fillGuard {
	return &fillGuard{seed: maphash.MakeSeed(), pending: map[uint64]pendingFill{}}
}

func (g *fillGuard) bucket(key string) uint32 {
	return uint32(maphash.String(g.seed, key) % fillGuardBuckets)
}

// begin returns the sequence at which a fill for key starts.
func (g *fillGuard) begin() uint64 { return g.seq.Load() }

// forgot records a local write to key. It must be called before the local
// entry is dropped.
func (g *fillGuard) forgot(key string) {
	g.forgets[g.bucket(key)].Store(g.seq.Add(1))
}

// stale reports whether a fill of key that began at start may hold a value
// older than a local write.
func (g *fillGuard) stale(key string, start uint64) bool {
	return g.forgets[g.bucket(key)].Load() > start
}

// track and finish bind a reservation token to its fill start (go-redis).
func (g *fillGuard) track(token uint64, key string) {
	p := pendingFill{start: g.begin(), bucket: g.bucket(key)}
	g.mu.Lock()
	g.pending[token] = p
	g.mu.Unlock()
}

// finish removes the token and reports whether its fill must be rejected.
func (g *fillGuard) finish(token uint64) (reject bool) {
	g.mu.Lock()
	p, ok := g.pending[token]
	delete(g.pending, token)
	g.mu.Unlock()
	return ok && g.forgets[p.bucket].Load() > p.start
}
