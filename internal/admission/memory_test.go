package admission

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/Huy-8bit/keyshift/internal/detector"
	"github.com/Huy-8bit/keyshift/internal/policy"
)

// TestMemoryFootprint measures the heap held by a full detector and admission
// table at the default capacity (reported in docs/scaling.md).
func TestMemoryFootprint(t *testing.T) {
	if testing.Short() {
		t.Skip("memory measurement")
	}
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	e, err := New(Config{Detector: detector.Config{Capacity: 1000, TopK: 1000, SampleRate: 1}, MaxStaleness: 3 * time.Second}, policy.Resolve(nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	e.SetHealthy(true)
	now := time.Now()
	// 1,000 distinct 40-byte keys, each hot enough to be admitted: the
	// detector, the admission table, and the admitted set are all full.
	for range 6 {
		for i := range 1000 {
			key := fmt.Sprintf("service:entity:%024d", i)
			for range 60 {
				if ks := e.Lookup(key); ks != nil {
					ks.AddRead(256)
				} else {
					e.ObserveRead(key, 256)
				}
			}
		}
		now = now.Add(10 * time.Second)
		e.Rotate(now)
	}
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	held := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	t.Logf("detector capacity 1000 + admission table %d records + %d admitted: heap held ≈ %d KiB",
		e.TableSize(), e.AdmittedCount(), held/1024)
	runtime.KeepAlive(e)
	if held > 4<<20 {
		t.Fatalf("footprint %d bytes exceeds 4 MiB", held)
	}
}
