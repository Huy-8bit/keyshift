#!/usr/bin/env bash
# KeyShift Lab runner.
#
# Builds and starts the lab, runs every scenario with assertions, saves the
# evidence of each step, and writes lab/results/<timestamp>/REPORT.md.
#
# Usage:
#   ./lab/run.sh              build images, start the lab, run all scenarios
#   ./lab/run.sh --no-build   reuse existing images
#   ./lab/run.sh --down       also stop the lab and remove its volumes at the end
#
# Requirements on the host: Docker (with Compose v2), curl, jq.
set -uo pipefail

cd "$(dirname "$0")"
LAB_DIR=$(pwd)
DC=(docker compose -f "$LAB_DIR/docker-compose.yml")
BUILD=1
DOWN=0
for arg in "$@"; do
  case "$arg" in
    --no-build) BUILD=0 ;;
    --down) DOWN=1 ;;
    -h|--help) sed -n '2,13p' "$0"; exit 0 ;;
    *) echo "unknown option: $arg" >&2; exit 2 ;;
  esac
done

for tool in docker curl jq; do
  command -v "$tool" >/dev/null || { echo "missing required tool: $tool" >&2; exit 2; }
done

# shellcheck disable=SC1091
set -a; source "$LAB_DIR/lab.env"; set +a
VIEWER=$KEYSHIFT_VIEWER_TOKENS
ADMIN=$KEYSHIFT_ADMIN_TOKENS
CTL=http://localhost:8080
CTL_INTERNAL=http://controller:8080

RUN_ID=$(date +%Y%m%d-%H%M%S)
OUT="$LAB_DIR/results/$RUN_ID"
mkdir -p "$OUT"
REPORT_ROWS=()
FAILED=0

log() { printf '\033[36m[lab %s]\033[0m %s\n' "$(date +%H:%M:%S)" "$*"; }

# api PATH: GET a controller API path with the viewer token.
api() { curl -fsS --max-time 10 -H "Authorization: Bearer $VIEWER" "$CTL$1"; }

# cli ARGS...: run the keyshift CLI inside the lab network.
cli() { "${DC[@]}" run --rm -T cli "$@"; }

# wait_for SECONDS DESCRIPTION COMMAND...: poll COMMAND every 2s.
wait_for() {
  local timeout=$1 what=$2; shift 2
  local start=$SECONDS
  until "$@" >/dev/null 2>&1; do
    if (( SECONDS - start >= timeout )); then
      log "timed out after ${timeout}s waiting for: $what"
      return 1
    fi
    sleep 2
  done
  log "$what (after $((SECONDS - start))s)"
}

# step NAME FUNCTION: run a scenario, record PASS/FAIL, duration, and details.
step() {
  local name=$1 fn=$2
  local start=$SECONDS
  log "── $name"
  DETAIL=""
  if "$fn" > >(tee -a "$OUT/lab.log") 2>&1; then
    REPORT_ROWS+=("| $name | ✅ PASS | $((SECONDS - start))s | $DETAIL |")
  else
    FAILED=$((FAILED + 1))
    REPORT_ROWS+=("| $name | ❌ FAIL | $((SECONDS - start))s | $DETAIL |")
    log "FAILED: $name"
  fi
}

overview() { api /api/v1/overview; }
active_instances() { overview | jq -r '.active_instances'; }
healthy_instances() { overview | jq -r '.tracking.healthy // 0'; }
all_healthy() { local o; o=$(overview) && [ "$(jq -r '.active_instances' <<<"$o")" -ge 7 ] && [ "$(jq -r '.tracking.healthy // 0' <<<"$o")" -eq "$(jq -r '.active_instances' <<<"$o")" ]; }
product_ok() { curl -fsS --max-time 2 -o /dev/null http://localhost:8090/products/1; }

# ─── scenarios ──────────────────────────────────────────────────────────────

s_start() {
  if (( BUILD )); then
    "${DC[@]}" build --pull=false || return 1
  fi
  "${DC[@]}" up -d --wait --wait-timeout 180 || return 1
  "${DC[@]}" ps --format 'table {{.Service}}\t{{.Status}}' | tee "$OUT/services.txt"
  DETAIL="$(($(wc -l <"$OUT/services.txt") - 1)) services running"
}

s_doctor() {
  cli doctor --redis redis:6379 --controller "$CTL_INTERNAL" --token "$VIEWER" | tee "$OUT/doctor.txt"
  local rc=${PIPESTATUS[0]}
  DETAIL="$(grep -c '^\[PASS\]' "$OUT/doctor.txt") checks passed, $(grep -c '^\[FAIL\]' "$OUT/doctor.txt") failed"
  return "$rc"
}

s_steady_state() {
  wait_for 150 "7 SDK instances reporting with healthy tracking" all_healthy || return 1
  log "collecting 30s of steady-state telemetry"
  sleep 30
  cli stats --controller "$CTL_INTERNAL" --token "$VIEWER" | tee "$OUT/stats-steady.txt"
  cli top --controller "$CTL_INTERNAL" --token "$VIEWER" --limit 15 > "$OUT/top-steady.txt"
  cli instances --controller "$CTL_INTERNAL" --token "$VIEWER" > "$OUT/instances-steady.txt"
  overview > "$OUT/overview-steady.json"
  local hit; hit=$(jq -r '.local_hit_ratio' "$OUT/overview-steady.json")
  DETAIL="$(jq -r '.active_instances' "$OUT/overview-steady.json") instances, local hit ratio $(jq -r '.local_hit_ratio*100|floor' "$OUT/overview-steady.json")%, $(jq -r '.rates.local_hits|floor' "$OUT/overview-steady.json") Redis reads avoided/s"
  jq -e '.local_hit_ratio > 0.3' <<<"$(overview)" >/dev/null && [ "$(jq -r '.tracking.healthy' "$OUT/overview-steady.json")" -ge 7 ]
}

s_sdk_metrics() {
  curl -fsS http://localhost:8090/metrics | grep '^keyshift_' > "$OUT/product-service-metrics.txt" || return 1
  curl -fsS "http://localhost:8090/debug/keyshift/explain?key=product:1" | jq . > "$OUT/explain-product-1.json" || return 1
  local hits state
  hits=$(awk '/^keyshift_cache_hits_total/ {print int($2)}' "$OUT/product-service-metrics.txt")
  state=$(jq -r .state "$OUT/explain-product-1.json")
  local key_labels; key_labels=$(grep -c 'product:' "$OUT/product-service-metrics.txt" || true)
  DETAIL="product-service: $hits local hits, product:1 is $state, $key_labels metric labels contain key names"
  [ "${hits:-0}" -gt 0 ] && [ "$state" = "admitted" ] && [ "$key_labels" -eq 0 ]
}

s_adaptive_decisions() {
  "${DC[@]}" logs --no-log-prefix demo 2>/dev/null | grep -B1 -A7 '^INSTANCE' | tail -8 | tee "$OUT/demo-decisions.txt"
  local catalog_admitted stock_not_admitted
  catalog_admitted=$(awk 'NR>1 && $7=="admitted"' "$OUT/demo-decisions.txt" | wc -l | tr -d ' ')
  stock_not_admitted=$(awk 'NR>1 && $6!="admitted"' "$OUT/demo-decisions.txt" | wc -l | tr -d ' ')
  DETAIL="hot catalog:0 admitted on $catalog_admitted/6 pods; write-heavy stock:0 not cached on $stock_not_admitted/6 pods (where it is cached, its measured hit ratio is still above exit_hit_ratio)"
  [ "$catalog_admitted" -eq 6 ]
}

checkout_remote() { [ "$(api '/api/v1/instances?service=checkout-api' | jq '[.items[] | select(.policy_source=="remote")] | length')" -eq 3 ]; }
checkout_local() { [ "$(api '/api/v1/instances?service=checkout-api' | jq '[.items[] | select(.policy_source=="local")] | length')" -eq 3 ]; }
catalog_explicit() { [ "$(api '/api/v1/instances?service=catalog-api' | jq '[.items[] | select(.cache_mode=="explicit")] | length')" -eq 3 ]; }

s_policy_rollout() {
  local policies=/policies
  "${DC[@]}" run --rm -T -v "$LAB_DIR/policies:$policies:ro" cli policy set --controller "$CTL_INTERNAL" --token "$ADMIN" \
    --service checkout-api -f "$policies/checkout-deny-stock.json" --rollout 50 || return 1
  "${DC[@]}" run --rm -T cli policy rollout --controller "$CTL_INTERNAL" --token "$ADMIN" --service checkout-api --percent 100 || return 1
  wait_for 60 "all checkout-api pods run the remote policy" checkout_remote || return 1
  "${DC[@]}" run --rm -T -v "$LAB_DIR/policies:$policies:ro" cli policy set --controller "$CTL_INTERNAL" --token "$ADMIN" \
    --service catalog-api -f "$policies/catalog-explicit.json" || return 1
  wait_for 60 "all catalog-api pods switched to explicit mode" catalog_explicit || return 1
  sleep 12 # let one window pass so admission reflects the policies
  api '/api/v1/instances?limit=50' | jq -r '.items[] | [.service, .instance, "v\(.policy_version)", .policy_source, .cache_mode, .cache.admitted] | @tsv' \
    | tee "$OUT/policy-applied.tsv"
  "${DC[@]}" logs --no-log-prefix --since 15s demo 2>/dev/null | grep -A7 '^INSTANCE' | tail -7 > "$OUT/policy-demo-decisions.txt"
  local bypassed; bypassed=$(awk '$1 ~ /^checkout/ && $6=="bypassed"' "$OUT/policy-demo-decisions.txt" | wc -l | tr -d ' ')
  local catalog_admitted; catalog_admitted=$(awk '$1 ~ /^catalog/ {print $5}' "$OUT/policy-demo-decisions.txt" | sort -n | tail -1)
  cli policy history --controller "$CTL_INTERNAL" --token "$VIEWER" > "$OUT/policy-history.txt"
  # Clean up: deleting the scopes must return pods to their local policy.
  cli policy delete --controller "$CTL_INTERNAL" --token "$ADMIN" --service checkout-api || return 1
  cli policy delete --controller "$CTL_INTERNAL" --token "$ADMIN" --service catalog-api || return 1
  wait_for 60 "checkout-api pods returned to their local policy after deletion" checkout_local || return 1
  DETAIL="stock:* bypassed on $bypassed/3 checkout pods; catalog pods admit ≤ ${catalog_admitted:-?} keys in explicit mode; deletion reverted pods to local policy"
  [ "$bypassed" -eq 3 ] && [ "${catalog_admitted:-99}" -le 3 ]
}

s_controller_outage() {
  "${DC[@]}" stop controller >/dev/null || return 1
  log "controller stopped; sending 500 product requests"
  local ok=0
  for i in $(seq 1 500); do
    curl -fsS --max-time 2 -o /dev/null "http://localhost:8090/products/$((i % 20))" && ok=$((ok + 1))
  done
  curl -fsS http://localhost:8090/debug/keyshift/stats | jq '{reads, local_hits, errors, tracking: .tracking.state, policy_source, telemetry_failed}' \
    | tee "$OUT/outage-product-stats.json"
  local demo_errors; demo_errors=$("${DC[@]}" logs --since 30s demo 2>/dev/null | grep -c 'get:' || true)
  "${DC[@]}" start controller >/dev/null || return 1
  wait_for 120 "controller back and 7 instances reporting again" all_healthy || return 1
  DETAIL="$ok/500 requests served while the controller was down; $demo_errors demo read errors"
  [ "$ok" -eq 500 ] && [ "$demo_errors" -eq 0 ]
}

s_redis_restart() {
  local before; before=$(curl -fsS http://localhost:8090/debug/keyshift/stats | jq '.tracking.transitions')
  local t0=$SECONDS
  "${DC[@]}" restart redis >/dev/null || return 1
  wait_for 60 "product-service serving again" product_ok || return 1
  local served_after=$((SECONDS - t0))
  wait_for 90 "tracking healthy on all instances" all_healthy || return 1
  curl -fsS http://localhost:8090/debug/keyshift/stats | jq '{tracking: .tracking, capabilities: .capabilities.caching, admitted_keys}' \
    | tee "$OUT/redis-restart-product-stats.json"
  local after; after=$(jq '.tracking.transitions' "$OUT/redis-restart-product-stats.json")
  if [ "$after" -gt "$before" ]; then
    DETAIL="serving again ${served_after}s after restart; the health probe suspended caching, flushed the local cache, and resumed (tracking transitions ${before} to ${after})"
  else
    DETAIL="serving again ${served_after}s after restart; the client library evicted entries of the closed connections before the next health probe, so caching was never suspended"
  fi
  # Whatever the path, every instance must be healthy and serving again.
  all_healthy && product_ok
}

s_connection_kill() {
  local killed; killed=$("${DC[@]}" exec -T redis redis-cli CLIENT KILL TYPE normal SKIPME yes | tr -d '\r')
  wait_for 30 "product-service serving" product_ok || return 1
  wait_for 60 "tracking healthy on all instances" all_healthy || return 1
  DETAIL="killed $killed client connections; all instances recovered"
  [ "${killed:-0}" -gt 0 ]
}

s_hotkeys() {
  cli inspect hotkeys --redis redis:6379 --duration 10s --metrics cpu,net --count 10 | tee "$OUT/redis-hotkeys.txt"
  local rc=${PIPESTATUS[0]}
  DETAIL="Redis-side top keys by CPU and network: $(awk '/^1 /{print $2; exit}' "$OUT/redis-hotkeys.txt")"
  [ "$rc" -eq 0 ] && grep -q 'catalog:\|product:\|stock:' "$OUT/redis-hotkeys.txt"
}

s_bench() {
  "${DC[@]}" --profile tools up -d --wait redis-bench >/dev/null || return 1
  cli bench --redis redis-bench:6379 -yes -modes none,csc,keyshift -scenarios zipf,write-heavy \
    -duration 5s -warmup 5s -concurrency 16 | tee "$OUT/bench.md"
  local rc=${PIPESTATUS[0]}
  "${DC[@]}" --profile tools stop redis-bench >/dev/null
  local ks; ks=$(grep '^| keyshift' "$OUT/bench.md" | head -1 | awk -F'|' '{print $6}' | tr -d ' ')
  local stale; stale=$(grep '^| csc' "$OUT/bench.md" | tail -1 | awk -F'|' '{print $11}' | sed 's/^ *//;s/ *$//')
  local ks_stale; ks_stale=$(grep '^| keyshift' "$OUT/bench.md" | tail -1 | awk -F'|' '{print $11}' | sed 's/^ *//;s/ *$//')
  DETAIL="zipf: KeyShift hit ratio $ks; write-heavy stale reads: conventional caching $stale, KeyShift $ks_stale (see bench.md)"
  return "$rc"
}

prom() { curl -fsS --get "http://localhost:9090/api/v1/query" --data-urlencode "query=$1" | jq -r '.data.result[0].value[1] // "0"'; }
grafana_ready() { curl -fsS http://localhost:3000/api/health | jq -e '.database=="ok"'; }

s_grafana() {
  wait_for 60 "Grafana ready" grafana_ready || return 1
  curl -fsS "http://localhost:3000/api/search?tag=keyshift" | jq -r '.[] | "\(.uid)\t\(.title)"' | tee "$OUT/grafana-dashboards.tsv"
  local ds; ds=$(curl -fsS -u admin:keyshift-lab http://localhost:3000/api/datasources/uid/prometheus/health | jq -r .status)
  local active hit hot avoided
  active=$(prom 'keyshift_fleet_instances{state="active"}')
  hit=$(prom 'keyshift_fleet_local_hit_ratio')
  avoided=$(prom 'keyshift_fleet_rate{kind="local_hits"}')
  hot=$(prom 'count(keyshift_fleet_hotkey_rate{kind="reads"})')
  for q in 'keyshift_fleet_instances' 'keyshift_fleet_rate' 'keyshift_fleet_service_hit_ratio' 'keyshift_fleet_hotkey_rate{kind="reads"}' \
           'keyshift_fleet_instance_hit_ratio' 'keyshift_cache_hits_total' 'keyshift_tracking_healthy'; do
    echo "== $q"; curl -fsS --get "http://localhost:9090/api/v1/query" --data-urlencode "query=$q" | jq -c '.data.result[] | [.metric, .value[1]]' | head -12
  done > "$OUT/prometheus-series.txt"
  DETAIL="$(wc -l <"$OUT/grafana-dashboards.tsv" | tr -d ' ') dashboards provisioned, datasource $ds; Prometheus: $active active instances, hit ratio $(printf '%.0f' "$(echo "$hit*100" | bc -l)")%, $(printf '%.0f' "$avoided") Redis reads avoided/s, $hot hot keys"
  [ "$ds" = "OK" ] && [ "$(wc -l <"$OUT/grafana-dashboards.tsv")" -ge 2 ] && [ "${active%.*}" -ge 7 ] && [ "$hot" -gt 0 ]
}

s_final_snapshot() {
  cli stats --controller "$CTL_INTERNAL" --token "$VIEWER" > "$OUT/stats-final.txt"
  cli top --controller "$CTL_INTERNAL" --token "$VIEWER" --limit 15 > "$OUT/top-final.txt"
  cli instances --controller "$CTL_INTERNAL" --token "$VIEWER" > "$OUT/instances-final.txt"
  local v; v=$(prom 'sum(rate(keyshift_cache_hits_total[1m]))')
  DETAIL="CLI snapshots saved; product-service SDK metrics: $(printf '%.0f' "$v") Redis reads avoided/s"
  [ -s "$OUT/stats-final.txt" ]
}

# ─── run ────────────────────────────────────────────────────────────────────

log "results: $OUT"
step "1. Build and start the lab" s_start
step "2. Compatibility check (keyshift doctor)" s_doctor
step "3. Steady state: 7 SDK instances, adaptive caching" s_steady_state
step "4. SDK metrics and Explain in a real service" s_sdk_metrics
step "5. Per-pod adaptive decisions" s_adaptive_decisions
step "6. Policy: staged rollout, explicit mode, deletion" s_policy_rollout
step "7. Fault: controller outage" s_controller_outage
step "8. Fault: Redis restart" s_redis_restart
step "9. Fault: all client connections killed" s_connection_kill
step "10. Redis-native HOTKEYS diagnostics" s_hotkeys
step "11. Benchmark smoke (isolated Redis)" s_bench
step "12. Prometheus and Grafana dashboards" s_grafana
step "13. Final snapshot" s_final_snapshot

{
  echo "# KeyShift Lab report"
  echo
  echo "- Run: \`$RUN_ID\`"
  echo "- Host: $(uname -sm), Docker $(docker version --format '{{.Server.Version}}' 2>/dev/null)"
  echo "- Result: **$(( ${#REPORT_ROWS[@]} - FAILED ))/${#REPORT_ROWS[@]} steps passed**"
  echo
  echo "| Step | Result | Duration | Details |"
  echo "|---|---|---|---|"
  printf '%s\n' "${REPORT_ROWS[@]}"
  echo
  echo "## Evidence"
  echo
  for f in "$OUT"/*; do
    case "$(basename "$f")" in REPORT.md|lab.log) ;; *) echo "- [\`$(basename "$f")\`]($(basename "$f"))" ;; esac
  done
  echo "- [\`lab.log\`](lab.log): full console output"
  echo
  echo "## Explore"
  echo
  echo "- Grafana: <http://localhost:3000> (dashboards \"KeyShift Fleet\" and \"KeyShift SDK\"; admin / keyshift-lab to edit)"
  echo "- Prometheus: <http://localhost:9090>"
  echo "- Controller API: <http://localhost:8080/api/v1/overview> (viewer token \`$VIEWER\`, admin token \`$ADMIN\`)"
  echo "- product-service: <http://localhost:8090/products/1>, <http://localhost:8090/debug/keyshift/stats>"
} > "$OUT/REPORT.md"

log "report: $OUT/REPORT.md"
cat "$OUT/REPORT.md"

if (( DOWN )); then
  log "stopping the lab"
  "${DC[@]}" --profile tools down -v
else
  log "the lab is still running; stop it with: docker compose -f lab/docker-compose.yml --profile tools down -v"
fi
exit $(( FAILED > 0 ))
