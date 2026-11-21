// Package observability contains low-overhead instrumentation primitives
// shared by the SDK and the controller.
package observability

import (
	"math/rand/v2"
	"sync/atomic"
)

const stripes = 16

type paddedUint64 struct {
	v atomic.Uint64
	_ [56]byte
}

// Counter is a monotonically increasing counter striped across cache lines
// so that concurrent increments from many goroutines do not contend on a
// single atomic. Load is O(stripes).
type Counter struct {
	s [stripes]paddedUint64
}

// Inc adds one.
func (c *Counter) Inc() { c.s[rand.Uint32()&(stripes-1)].v.Add(1) }

// Add adds n.
func (c *Counter) Add(n uint64) { c.s[rand.Uint32()&(stripes-1)].v.Add(n) }

// Load returns the current total.
func (c *Counter) Load() uint64 {
	var t uint64
	for i := range c.s {
		t += c.s[i].v.Load()
	}
	return t
}
