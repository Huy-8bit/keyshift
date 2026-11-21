# Troubleshooting

Start with:

```bash
keyshift doctor --redis <addr> [--cluster] [--controller <url>]
```

and in the application:

```go
s := client.Stats()
fmt.Println(s.Capabilities, s.Tracking, s.PolicySource, s.PolicyVersion)
fmt.Printf("%+v\n", client.Explain("the:key"))
```

## Nothing is cached (`AdmittedKeys == 0`)

| Symptom | Cause | Fix |
|---|---|---|
| `Tracking.State = unsupported`, reason mentions ClusterClient | go-redis has no cluster client-side caching | use `NewFromRueidis` |
| reason mentions `DB 0` or credentials provider | go-redis restrictions | use DB 0 / fixed credentials, or the rueidis driver |
| reason mentions `NOPERM` | ACL lacks `CLIENT` subcommands | grant the ACL in [compatibility.md](compatibility.md#minimum-acl) |
| `Tracking.State = unhealthy` | probe failing or timing out | check network latency vs `HealthTimeout` (500 ms); see the reason field |
| `Tracking.State = disabled` | `Cache.Mode = Off` or detection disabled | |
| `Explain` says `read rate … below min_read_rate` | traffic per instance is too spread out | expected; check coverage and per-instance rates before lowering thresholds |
| `Explain` says `qualifying window 1/2` | admission needs consecutive windows | wait `MinObservationWindows × Window` (20 s by default) |
| `Explain` says `explicit mode` | policy mode is explicit | add pin patterns or switch to adaptive |
| `Explain` says `denied by policy` | local or remote deny rule | check `keyshift policy get --service <svc>` |

## Low hit ratio on an admitted key

- `max_staleness` refreshes each entry at least every 3 s by default, so a key read k times per second cannot exceed `1 - 1/(3k)`.
- External writes invalidate the entry; the engine moves such keys to `cooldown` when the observed hit ratio drops below `exit_hit_ratio`.
- `keyshift_cache_evictions_total` rising with `cache_entries` at `MaxEntries`: the budget is too small.

## Many keys in cooldown, high admission churn

The keyspace has writers you cannot see locally. Admission probes them, measures invalidations, and backs off exponentially. To avoid the probation fills entirely, deny those patterns.

## Stale reads

1. Confirm the key is meant to be cached. Data that needs authoritative reads must be denied or read with `GetFresh`.
2. Check `keyshift_tracking_errors_total` and transitions in `Stats().Tracking`.
3. Writes through another client library or another Redis endpoint (a different DB, or a replica you read from) are only visible through server invalidation of the server you read from.
4. Lower `Tracking.HealthInterval` / `HealthTimeout` to shorten the partition window, and `Cache.MaxStaleness` to lower the worst case.

Please report reproducible stale reads; see the bug template.

## Telemetry does not arrive

- `keyshift_controller_requests_total{result="failed"}` and `keyshift_telemetry_dropped_total`.
- HTTP 401/403: wrong token or wrong role (SDKs need an **ingest** token).
- HTTP 429: global or per-instance rate limit (default minimum 5 s between uploads per instance).
- HTTP 503 with reason `capacity`: `--max-instances` or `--max-services` reached.
- The dashboard warns `instance_fingerprints`: SDKs run without a shared `FingerprintSecret`, so keys cannot be aggregated across pods.
- The first upload is delayed by a random fraction of the interval (up to 30 s by default).

## Dashboard shows "Dashboard not built"

The controller was built without `web/dist`. Run `make build` (requires npm) or use the container image.

## Redis memory or CPU rose after enabling KeyShift

Compare `INFO stats` `tracking_total_keys` and `tracking_total_items` with `cache_admitted_keys` across instances. With KeyShift they should be of the same order. If you also enabled go-redis's own `ClientSideCacheConfig` on your application client, Redis tracks every key that client reads; leave it disabled and let KeyShift manage caching.

## go-redis log "failed to initialize maintnotifications … cannot overwrite existing handler"

This appears when a go-redis client is built from another live client's `Options` struct. KeyShift builds its tracked client with its own push processor, so it does not trigger this; if you see it, check application code that copies `*client.Options()`.
