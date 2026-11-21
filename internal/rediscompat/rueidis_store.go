package rediscompat

import (
	"container/list"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/redis/rueidis"
)

// rueidisCmdSuffix is the command part rueidis appends to the Redis key to
// form a cache-store key for a plain GET (see rueidis cmds.CacheKey).
const rueidisCmdSuffix = "GET"

const storeEntryOverhead = 96

// storeRegistry owns the per-connection cache stores that rueidis creates
// through ClientOption.NewCacheStoreFn. rueidis wraps each store with its
// SimpleCache adapter, which implements single-flight fills, TTL checks, and
// flushing on connection loss. The registry adds a global memory and entry
// budget across connections, per-key forgetting after local writes, and
// statistics.
type storeRegistry struct {
	maxEntries int64
	maxBytes   int64
	hooks      Hooks

	mu     sync.Mutex
	stores map[*lruStore]struct{}

	entries, bytes             atomic.Int64
	invals, evictions, flushes atomic.Uint64
	guard                      *fillGuard
}

func newStoreRegistry(l CacheLimits, hooks Hooks) *storeRegistry {
	return &storeRegistry{
		maxEntries: int64(max(l.MaxEntries, 1)),
		maxBytes:   max(l.MaxMemoryBytes, 1),
		hooks:      hooks,
		stores:     map[*lruStore]struct{}{},
		guard:      newFillGuard(),
	}
}

// NewCacheStoreFn is passed to rueidis.ClientOption.NewCacheStoreFn.
func (r *storeRegistry) NewCacheStoreFn(rueidis.CacheStoreOption) rueidis.CacheStore {
	s := &lruStore{reg: r, ll: list.New(), m: map[string]*list.Element{}}
	r.mu.Lock()
	r.stores[s] = struct{}{}
	r.mu.Unlock()
	return rueidis.NewSimpleCacheAdapter(s)
}

func (r *storeRegistry) forget(key string) {
	r.guard.forgot(key)
	r.mu.Lock()
	stores := make([]*lruStore, 0, len(r.stores))
	for s := range r.stores {
		stores = append(stores, s)
	}
	r.mu.Unlock()
	for _, s := range stores {
		s.remove(key + rueidisCmdSuffix)
	}
}

func (r *storeRegistry) flushAll() int {
	r.mu.Lock()
	stores := make([]*lruStore, 0, len(r.stores))
	for s := range r.stores {
		stores = append(stores, s)
	}
	r.mu.Unlock()
	n := 0
	for _, s := range stores {
		n += s.clear()
	}
	return n
}

func (r *storeRegistry) overBudget() bool {
	return r.entries.Load() > r.maxEntries || r.bytes.Load() > r.maxBytes
}

type storeItem struct {
	key  string
	val  rueidis.RedisMessage
	size int64
}

// lruStore implements rueidis.SimpleCache for one connection.
type lruStore struct {
	reg  *storeRegistry
	mu   sync.Mutex
	ll   *list.List
	m    map[string]*list.Element
	dead bool
}

func (s *lruStore) Get(key string) rueidis.RedisMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.m[key]; ok {
		s.ll.MoveToFront(e)
		return e.Value.(*storeItem).val
	}
	return rueidis.RedisMessage{}
}

func (s *lruStore) Set(key string, val rueidis.RedisMessage) {
	size := int64(len(key)+val.CacheSize()) + storeEntryOverhead
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dead {
		return
	}
	if e, ok := s.m[key]; ok {
		s.removeElem(e)
	}
	if size > s.reg.maxBytes {
		return // larger than the whole budget: do not cache
	}
	s.m[key] = s.ll.PushFront(&storeItem{key: key, val: val, size: size})
	s.reg.entries.Add(1)
	s.reg.bytes.Add(size)
	for s.reg.overBudget() && s.ll.Len() > 1 {
		s.removeElem(s.ll.Back())
		s.reg.evictions.Add(1)
	}
}

// Del is called by the rueidis adapter for server invalidations.
func (s *lruStore) Del(key string) {
	s.remove(key)
	s.reg.invals.Add(1)
	if s.reg.hooks.OnInvalidate != nil {
		s.reg.hooks.OnInvalidate(strings.TrimSuffix(key, rueidisCmdSuffix))
	}
}

// Flush is called by the rueidis adapter when the connection closes. The
// store is retired because rueidis creates a new one for the new connection.
func (s *lruStore) Flush() {
	s.clear()
	s.mu.Lock()
	s.dead = true
	s.mu.Unlock()
	s.reg.flushes.Add(1)
	s.reg.mu.Lock()
	delete(s.reg.stores, s)
	s.reg.mu.Unlock()
}

func (s *lruStore) remove(key string) {
	s.mu.Lock()
	if e, ok := s.m[key]; ok {
		s.removeElem(e)
	}
	s.mu.Unlock()
}

func (s *lruStore) clear() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.ll.Len()
	for e := s.ll.Front(); e != nil; e = s.ll.Front() {
		s.removeElem(e)
	}
	return n
}

func (s *lruStore) removeElem(e *list.Element) {
	it := e.Value.(*storeItem)
	s.ll.Remove(e)
	delete(s.m, it.key)
	s.reg.entries.Add(-1)
	s.reg.bytes.Add(-it.size)
}
