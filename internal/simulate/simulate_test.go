package simulate

import (
	"math/rand/v2"
	"testing"
	"time"
)

func TestSyntheticBatchesAreValid(t *testing.T) {
	in := &instance{service: "service-000", id: "service-000-pod-00", seq: 1}
	for i := range 50 {
		in.keys = append(in.keys, i*7919)
	}
	b := makeBatch(in, rand.New(rand.NewPCG(1, 2)), 50)
	if err := b.Validate(time.Now()); err != nil {
		t.Fatalf("synthetic batch rejected: %v", err)
	}
}
