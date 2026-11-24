package keyshift

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/rueidis"

	"github.com/Huy-8bit/keyshift/internal/admission"
	"github.com/Huy-8bit/keyshift/internal/observability"
)

// errUseDirect is returned by readCached when the cache path failed with a
// transport error and the read should be retried on the direct path.
var errUseDirect = errors.New("keyshift: retry on the direct path")

// KeyValue is one result of MGet.
type KeyValue struct {
	Key   string
	Value string
	// Found is false when the key does not exist (Value is then empty).
	Found bool
}

// Get returns the string value of key, or Nil if it does not exist.
// Admitted keys are served from the local cache when present; all other
// reads go to Redis.
func (c *Client) Get(ctx context.Context, key string) (string, error) {
	if c.closed.Load() {
		return "", ErrClosed
	}
	c.reads.Inc()
	if ks := c.engine.Lookup(key); ks != nil {
		v, err := c.readCached(ctx, key, ks)
		if !errors.Is(err, errUseDirect) {
			return v, err
		}
	}
	return c.readDirect(ctx, key, &c.directReads)
}

// GetBytes is Get returning a byte slice that the caller owns.
func (c *Client) GetBytes(ctx context.Context, key string) ([]byte, error) {
	v, err := c.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	return []byte(v), nil
}

// GetFresh reads key directly from Redis, never from the local cache. Use it
// for reads that must reflect the authoritative Redis state (for example
// before a conditional write). The access still counts toward detection.
func (c *Client) GetFresh(ctx context.Context, key string) (string, error) {
	if c.closed.Load() {
		return "", ErrClosed
	}
	c.reads.Inc()
	return c.readDirect(ctx, key, &c.bypasses)
}

// MGet returns the values of keys in order. Admitted keys are read through
// the local cache like Get; all other keys are fetched in one pipeline (one
// round trip per Redis node, so keys may span Cluster slots). Every key
// counts toward detection exactly like an individual Get.
//
// If any key cannot be read (a Redis error reply such as WRONGTYPE, or a
// connection failure), MGet returns the first such error and no results.
func (c *Client) MGet(ctx context.Context, keys ...string) ([]KeyValue, error) {
	if c.closed.Load() {
		return nil, ErrClosed
	}
	out := make([]KeyValue, len(keys))
	direct := make([]int, 0, len(keys)) // indexes of keys read directly
	for i, key := range keys {
		c.reads.Inc()
		out[i].Key = key
		ks := c.engine.Lookup(key)
		if ks == nil {
			direct = append(direct, i)
			continue
		}
		v, err := c.readCached(ctx, key, ks)
		switch {
		case errors.Is(err, errUseDirect):
			direct = append(direct, i)
		case errors.Is(err, Nil):
		case err != nil:
			return nil, err
		default:
			out[i].Value, out[i].Found = v, true
		}
	}
	if len(direct) == 0 {
		return out, nil
	}
	if err := c.readDirectMulti(ctx, out, direct); err != nil {
		return nil, err
	}
	return out, nil
}

// readCached serves an admitted key through the tracked cache path. It
// returns errUseDirect after a transport failure, which may mean tracking was
// lost: caching is then suspended and the caller retries once on the direct
// path, so the application still sees ordinary Redis semantics.
func (c *Client) readCached(ctx context.Context, key string, ks *admission.KeyStats) (string, error) {
	start := c.now()
	v, err := c.backend.GetCached(ctx, key)
	if err == nil || errors.Is(err, Nil) {
		ks.AddRead(int64(len(v)))
		c.cachedReads.Inc()
		c.observe(start, pathCache)
		return v, err
	}
	if ctx.Err() != nil || !isTransportError(err) {
		// Redis replied with an error (or the caller gave up): return it
		// unchanged, as a direct read would.
		c.errorsTotal.Inc()
		return "", err
	}
	c.monitor.ReportError(err)
	c.fallbacks.Inc()
	return "", errUseDirect
}

// readDirect reads key from Redis without the local cache, counting the read
// in n (direct reads or bypasses) and recording it for detection.
func (c *Client) readDirect(ctx context.Context, key string, n *observability.Counter) (string, error) {
	start := c.now()
	v, err := c.backend.GetDirect(ctx, key)
	n.Inc()
	if err != nil && !errors.Is(err, Nil) {
		c.errorsTotal.Inc()
		return "", err
	}
	c.observe(start, pathDirect)
	c.observeRead(key, len(v))
	return v, err
}

// readDirectMulti fills out[i] for every index in idx with one pipelined
// read.
func (c *Client) readDirectMulti(ctx context.Context, out []KeyValue, idx []int) error {
	keys := make([]string, len(idx))
	for j, i := range idx {
		keys[j] = out[i].Key
	}
	start := c.now()
	res := c.backend.GetDirectMulti(ctx, keys)
	c.directReads.Add(uint64(len(keys)))
	for _, r := range res {
		if r.Err != nil && !errors.Is(r.Err, Nil) {
			c.errorsTotal.Inc()
			return r.Err
		}
	}
	c.observe(start, pathDirect)
	for j, r := range res {
		kv := &out[idx[j]]
		kv.Value, kv.Found = r.Value, r.Err == nil
		c.observeRead(kv.Key, len(r.Value))
	}
	return nil
}

func (c *Client) observeRead(key string, size int) {
	if !c.cfg.Detection.Disabled {
		c.engine.ObserveRead(key, int64(size))
	}
}

// Set writes value with an optional TTL (0 means no expiry). The write always
// goes to Redis; any local entry for key is dropped.
func (c *Client) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if c.closed.Load() {
		return ErrClosed
	}
	c.writes.Inc()
	c.engine.ObserveWrite(key)
	err := c.backend.Set(ctx, key, value, ttl)
	if err != nil {
		c.errorsTotal.Inc()
	}
	return err
}

// SetString is Set for string values.
func (c *Client) SetString(ctx context.Context, key, value string, ttl time.Duration) error {
	return c.Set(ctx, key, []byte(value), ttl)
}

// Del deletes keys and drops their local entries. It returns the number of
// keys removed by Redis.
func (c *Client) Del(ctx context.Context, keys ...string) (int64, error) {
	if c.closed.Load() {
		return 0, ErrClosed
	}
	for _, k := range keys {
		c.deletes.Inc()
		c.engine.ObserveWrite(k)
	}
	n, err := c.backend.Del(ctx, keys...)
	if err != nil {
		c.errorsTotal.Inc()
	}
	return n, err
}

// NotifyWrite tells KeyShift that keys were modified outside this client
// (through GoRedis or Rueidis, a Lua script, INCR, EXPIRE, and so on). Their
// local entries are dropped immediately, which restores read-your-writes for
// this instance without waiting for the server invalidation, and the writes
// count toward admission decisions like writes made with Set (they are
// reported in Stats.NotifiedWrites, not Stats.Writes). It never contacts
// Redis.
func (c *Client) NotifyWrite(keys ...string) {
	if c.closed.Load() {
		return
	}
	for _, k := range keys {
		c.notifiedWrites.Inc()
		c.engine.ObserveWrite(k)
		c.backend.Forget(k)
	}
}

// isTransportError reports whether err means the request may not have
// reached Redis or its reply was lost (as opposed to an error reply).
func isTransportError(err error) bool {
	if err == nil {
		return false
	}
	var ne net.Error
	switch {
	case errors.As(err, &ne),
		errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF),
		errors.Is(err, os.ErrDeadlineExceeded),
		errors.Is(err, syscall.ECONNRESET), errors.Is(err, syscall.ECONNREFUSED), errors.Is(err, syscall.EPIPE),
		errors.Is(err, redis.ErrClosed), errors.Is(err, redis.ErrPoolTimeout), errors.Is(err, redis.ErrPoolExhausted),
		errors.Is(err, rueidis.ErrClosing),
		// ErrDoCacheAborted: the cached fetch failed as a whole (connection
		// closed or the wrapper aborted); a direct read is still meaningful.
		errors.Is(err, rueidis.ErrDoCacheAborted):
		return true
	}
	return false
}
