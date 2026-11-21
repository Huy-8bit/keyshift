#!/bin/sh
# Reproduces the results in docs/benchmarks.md. Requires `make test-infra-up`.
# WARNING: runs FLUSHALL on the target Redis instances.
set -eu
OUT=${OUT:-benchmarks/results}
KS=${KS:-bin/keyshift}
R=${REDIS:-localhost:16379}
C=${CLUSTER:-127.0.0.1:17001,127.0.0.1:17002,127.0.0.1:17003}
mkdir -p "$OUT"

# 1. Closed-loop suite: every scenario, three modes.
$KS bench -yes -redis "$R" -cluster "$C" -modes none,csc,keyshift \
  -scenarios uniform,zipf,single-hot,local-hot,read-only-zipf,write-heavy,expiring,small-values,large-values,oversized-values,hot-shift,burst,tracking-interruption,startup-surge,controller-outage,cluster-zipf \
  -duration 10s -warmup 6s -concurrency 32 \
  -json "$OUT/closed-loop.json" -markdown "$OUT/closed-loop.md"

# 2. Open-loop at a fixed offered load: compares Redis load and latency
#    for the same work instead of turning cache hits into extra throughput.
$KS bench -yes -redis "$R" -modes none,csc,keyshift,keyshift-rueidis \
  -scenarios zipf,single-hot,local-hot,read-only-zipf,write-heavy,large-values,uniform \
  -rate 20000 -duration 10s -warmup 6s -concurrency 64 \
  -json "$OUT/open-loop.json" -markdown "$OUT/open-loop.md"

# 3. Redis restart (restartable instance).
$KS bench -yes -redis "${RESTART_REDIS:-localhost:16380}" -restart-container "${RESTART_CONTAINER:-keyshift-test-redis-restart}" \
  -modes none,csc,keyshift -scenarios redis-restart -duration 10s -warmup 6s \
  -json "$OUT/redis-restart.json" -markdown "$OUT/redis-restart.md"

# 4. Production default detection window (10s) needs a longer warmup.
$KS bench -yes -redis "$R" -modes keyshift -scenarios zipf -window 10s -warmup 30s -duration 20s -rate 20000 -concurrency 64 \
  -json "$OUT/default-window.json" -markdown "$OUT/default-window.md"

# 5. Logical control-plane simulation: 100 services x 50 instances.
$KS simulate -services 100 -instances-per-service 50 -candidates 20 -rounds 3 -json > "$OUT/simulate-5000x20.json"
$KS simulate -services 100 -instances-per-service 50 -candidates 50 -rounds 3 -json > "$OUT/simulate-5000x50.json"
$KS simulate -services 100 -instances-per-service 50 -candidates 20 -rounds 2 -paced 30s -concurrency 32 -json > "$OUT/simulate-5000x20-paced.json"
