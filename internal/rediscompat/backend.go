// Package rediscompat adapts Redis client libraries to the small surface the
// KeyShift SDK needs: direct (untracked) reads, reads through the client's
// own server-assisted client-side cache, and writes.
//
// KeyShift never parses RESP, manages connections, or implements the client
// tracking protocol itself. Invalidation, connection-loss eviction, and
// in-flight fill races are handled by the underlying client library.
package rediscompat

import (
	"context"
	"errors"
	"time"
)

// ErrNil is returned when a key does not exist.
var ErrNil = errors.New("keyshift: nil")

// Topology describes how the backend is deployed.
type Topology string

const (
	TopologyStandalone Topology = "standalone"
	TopologyCluster    Topology = "cluster"
	TopologyOther      Topology = "other"
)

// Capabilities reports what the backend supports at runtime.
type Capabilities struct {
	Driver   string   `json:"driver"`
	Topology Topology `json:"topology"`
	// Caching reports whether reads can be served from a tracked local cache.
	Caching bool `json:"caching"`
	// TrackingMode describes how server-assisted invalidation is scoped.
	TrackingMode string `json:"tracking_mode"`
	// Reason explains why caching is unavailable, if it is.
	Reason        string `json:"reason,omitempty"`
	ServerVersion string `json:"server_version,omitempty"`
}

// Hooks are callbacks invoked by backends. They must be cheap and must not
// call back into the backend.
type Hooks struct {
	// OnFill is called with the Redis key whenever a cache-path read misses
	// the local cache and is fetched from Redis.
	OnFill func(key string)
	// OnInvalidate is called with the Redis key for every server
	// invalidation, when the backend can observe them.
	OnInvalidate func(key string)
}

// CacheStats are cumulative local cache statistics.
type CacheStats struct {
	Hits          uint64 `json:"hits"`
	Misses        uint64 `json:"misses"`
	Entries       int64  `json:"entries"`
	MemoryBytes   int64  `json:"memory_bytes"`
	Invalidations uint64 `json:"invalidations"`
	Evictions     uint64 `json:"evictions"`
	ConnEvictions uint64 `json:"conn_evictions"`
	Flushes       uint64 `json:"flushes"`
	// ResidencyKnown is false when the backend cannot report entries and
	// memory (they are then -1).
	ResidencyKnown bool `json:"residency_known"`
}

// Result is the outcome of reading one key of a batch.
type Result struct {
	Value string
	// Err is nil, ErrNil when the key does not exist, or the error for this
	// key (a Redis error reply or a transport error).
	Err error
}

// Backend is the adapter contract.
//
// Reads return strings because both client libraries hold replies as
// strings: returning []byte would copy every value once more.
type Backend interface {
	// GetDirect reads from Redis without client-side caching or tracking.
	GetDirect(ctx context.Context, key string) (string, error)
	// GetDirectMulti reads keys without client-side caching in one pipeline
	// (one round trip per Redis node; cross-slot keys are allowed). Results
	// are in key order.
	GetDirectMulti(ctx context.Context, keys []string) []Result
	// GetCached reads through the tracked client-side cache.
	GetCached(ctx context.Context, key string) (string, error)
	// Set writes key and drops any local entry for it.
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	// Del deletes keys and drops their local entries.
	Del(ctx context.Context, keys ...string) (int64, error)
	// Forget drops the local entry for key, if any. It never contacts Redis.
	Forget(key string)
	// FlushLocal drops every local entry and returns how many were removed
	// (-1 when unknown).
	FlushLocal() int
	// CacheStats returns local cache statistics.
	CacheStats() CacheStats
	// Capabilities reports runtime capabilities.
	Capabilities() Capabilities
	// CheckTracking verifies that client tracking is active on the cache
	// path. It returns an error when tracking cannot be confirmed.
	CheckTracking(ctx context.Context) error
	// Ping checks basic connectivity on the direct path.
	Ping(ctx context.Context) error
	// Close releases resources owned by the backend.
	Close() error
}

// CacheLimits bounds the local cache owned by a backend.
type CacheLimits struct {
	MaxEntries     int
	MaxMemoryBytes int64
	MaxStaleness   time.Duration
}
