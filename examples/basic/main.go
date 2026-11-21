// Command basic is the smallest KeyShift program: it connects to Redis,
// reads a hot key repeatedly, and shows how KeyShift decides to cache it.
//
//	go run ./examples/basic -redis localhost:6379
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Huy-8bit/keyshift/pkg/keyshift"
)

func main() {
	addr := flag.String("redis", "localhost:6379", "Redis address")
	flag.Parse()

	client, err := keyshift.New(keyshift.Config{
		ServiceName: "basic-example",
		Redis:       keyshift.RedisConfig{Addr: *addr},
		// A short window so the example converges in seconds; production
		// deployments typically keep the 10s default.
		Detection: keyshift.DetectionConfig{Window: time.Second},
		Cache: keyshift.CacheConfig{
			Mode:         keyshift.Adaptive,
			MaxEntries:   1000,
			MaxStaleness: 3 * time.Second,
			DenyPatterns: []string{"balance:*"}, // never cache strict-consistency data
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	if err := client.SetString(ctx, "product:123", `{"name":"coffee","price":4}`, 0); err != nil {
		log.Fatal(err)
	}
	if err := client.SetString(ctx, "balance:42", "100", 0); err != nil {
		log.Fatal(err)
	}

	for second := 1; second <= 4; second++ {
		deadline := time.Now().Add(time.Second)
		for time.Now().Before(deadline) {
			if _, err := client.Get(ctx, "product:123"); err != nil {
				log.Fatal(err)
			}
			if _, err := client.Get(ctx, "balance:42"); err != nil {
				log.Fatal(err)
			}
		}
		s := client.Stats()
		fmt.Printf("t=%ds reads=%d local_hits=%d redis_reads=%d admitted=%d tracking=%s\n",
			second, s.Reads, s.LocalHits, s.RedisReads, s.AdmittedKeys, s.Tracking.State)
	}

	// Authoritative read that never uses the local cache.
	if v, err := client.GetFresh(ctx, "balance:42"); err == nil {
		fmt.Println("fresh balance:", v)
	}
	if _, err := client.Get(ctx, "missing"); errors.Is(err, keyshift.Nil) {
		fmt.Println("missing key returns keyshift.Nil")
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	fmt.Println("why is product:123 cached?")
	enc.Encode(client.Explain("product:123"))
	fmt.Println("why is balance:42 not cached?")
	enc.Encode(client.Explain("balance:42"))
}
