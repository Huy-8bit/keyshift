// Command keyshift is the KeyShift command-line tool: compatibility checks,
// Redis-side hot-key diagnostics, controller queries, policy management,
// benchmarks, and large-scale simulations. Run `keyshift help`.
package main

import (
	"os"

	"github.com/Huy-8bit/keyshift/internal/cli"
)

func main() { os.Exit(cli.Main(os.Args[1:], os.Stdout, os.Stderr)) }
