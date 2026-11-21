// Command keyshift-bench runs the KeyShift benchmark suite. It is equivalent
// to `keyshift bench`; see `keyshift-bench -h`.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/Huy-8bit/keyshift/internal/cli"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := cli.RunBench(ctx, os.Args[1:], os.Stdout); err != nil && !errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(os.Stderr, "keyshift-bench:", err)
		os.Exit(1)
	}
}
