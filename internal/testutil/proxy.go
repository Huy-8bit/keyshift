package testutil

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Proxy is a TCP proxy for fault injection. It can silently blackhole
// traffic (simulating a network partition where no FIN/RST is delivered),
// cut all connections, or add latency.
type Proxy struct {
	ln       net.Listener
	target   string
	blackhol atomic.Bool
	latency  atomic.Int64
	mu       sync.Mutex
	conns    map[net.Conn]struct{}
	wg       sync.WaitGroup
	closed   atomic.Bool
}

// NewProxy starts a proxy to target on a random local port.
func NewProxy(t testing.TB, target string) *Proxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &Proxy{ln: ln, target: target, conns: map[net.Conn]struct{}{}}
	p.wg.Add(1)
	go p.accept()
	t.Cleanup(p.Close)
	return p
}

// Addr returns the proxy address.
func (p *Proxy) Addr() string { return p.ln.Addr().String() }

// Blackhole stops forwarding bytes in both directions without closing
// connections. New connections are accepted but never reach the target.
func (p *Proxy) Blackhole(on bool) { p.blackhol.Store(on) }

// SetLatency delays every forwarded chunk.
func (p *Proxy) SetLatency(d time.Duration) { p.latency.Store(int64(d)) }

// CutAll closes every proxied connection (clients see EOF/RST).
func (p *Proxy) CutAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for c := range p.conns {
		_ = c.Close()
	}
}

// Close stops the proxy.
func (p *Proxy) Close() {
	if p.closed.Swap(true) {
		return
	}
	_ = p.ln.Close()
	p.CutAll()
	p.wg.Wait()
}

func (p *Proxy) track(c net.Conn) {
	p.mu.Lock()
	p.conns[c] = struct{}{}
	p.mu.Unlock()
}

func (p *Proxy) accept() {
	defer p.wg.Done()
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		up, err := net.Dial("tcp", p.target)
		if err != nil {
			_ = c.Close()
			continue
		}
		p.track(c)
		p.track(up)
		p.wg.Add(2)
		go p.pipe(c, up)
		go p.pipe(up, c)
	}
}

func (p *Proxy) pipe(dst, src net.Conn) {
	defer p.wg.Done()
	defer dst.Close()
	defer src.Close()
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			// While blackholed, hold the data until traffic resumes, the
			// way a partitioned network would after TCP retransmission.
			for p.blackhol.Load() && !p.closed.Load() {
				time.Sleep(time.Millisecond)
			}
			if d := time.Duration(p.latency.Load()); d > 0 {
				time.Sleep(d)
			}
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			if err != io.EOF {
				return
			}
			return
		}
	}
}
