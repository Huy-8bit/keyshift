# Benchmarks

`run-all.sh` runs the scenario benchmarks and logical simulations described in [docs/benchmarks.md](../docs/benchmarks.md). Results vary by environment; selected historical rows on that page were measured separately.

```bash
make test-infra-up build
OUT=/tmp/keyshift-benchmark-results ./benchmarks/run-all.sh  # ~1 hour
```

**The script runs `FLUSHALL` on its target Redis servers and restarts the designated restart container.** Only use isolated test infrastructure. Without `OUT`, it writes to `benchmarks/results/` and overwrites the checked-in result files.

| File | Content |
|---|---|
| `results/closed-loop.{json,md}` | every scenario, modes none / csc / keyshift, closed loop |
| `results/open-loop.{json,md}` | fixed offered load (20,000 ops/s), adds keyshift-rueidis |
| `results/redis-restart.{json,md}` | Redis restarted mid-run |
| `results/default-window.{json,md}` | KeyShift with the production default 10 s window |
| `results/simulate-*.json` | 5,000-instance logical controller simulation |

Lightweight CI mode (as in `.github/workflows/ci.yml`):

```bash
bin/keyshift bench -redis localhost:16379 -yes -scenarios zipf,write-heavy -duration 3s -warmup 3s -concurrency 16
bin/keyshift simulate -services 10 -instances-per-service 50 -rounds 2
```

Micro-benchmarks of the detector, admission lookup, fingerprinting, and counters: `make bench`.
