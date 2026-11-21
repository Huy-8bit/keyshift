// Command redis-cluster shows KeyShift on Redis Cluster with the rueidis
// driver, which supports OPTIN client tracking across cluster nodes.
//
//	go run ./examples/redis-cluster -addrs 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/redis/rueidis"

	"github.com/Huy-8bit/keyshift/pkg/keyshift"
)

func main() {
	addrs := flag.String("addrs", "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003", "cluster seed addresses")
	flag.Parse()

	client, err := keyshift.NewFromRueidis(
		rueidis.ClientOption{InitAddress: strings.Split(*addrs, ",")},
		keyshift.Config{
			ServiceName: "cluster-example",
			Detection:   keyshift.DetectionConfig{Window: time.Second},
		},
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	ctx := context.Background()

	// Keys hash to different slots and therefore to different nodes.
	keys := make([]string, 10)
	for i := range keys {
		keys[i] = "catalog:item:" + strconv.Itoa(i)
		if err := client.SetString(ctx, keys[i], "v1", 0); err != nil {
			log.Fatal(err)
		}
	}
	for second := 1; second <= 3; second++ {
		deadline := time.Now().Add(time.Second)
		for time.Now().Before(deadline) {
			for _, k := range keys {
				if _, err := client.Get(ctx, k); err != nil {
					log.Fatal(err)
				}
			}
		}
	}
	s := client.Stats()
	fmt.Printf("topology=%s driver=%s tracking=%s admitted=%d reads=%d local_hits=%d\n",
		s.Capabilities.Topology, s.Capabilities.Driver, s.Tracking.State, s.AdmittedKeys, s.Reads, s.LocalHits)

	// A write from this instance is visible to its next read.
	client.SetString(ctx, keys[0], "v2", 0)
	v, _ := client.Get(ctx, keys[0])
	fmt.Println(keys[0], "=", v)
}
