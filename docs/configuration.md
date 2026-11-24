# Configuration

## SDK (`keyshift.Config`)

Optional zero-valued fields select the defaults below. `ServiceName` is required, and `New` also requires a Redis address. Constructors validate the configuration and return errors for invalid settings.

### Identity

| Field | Default | Notes |
|---|---|---|
| `ServiceName` | required | 1–128 chars of `[A-Za-z0-9._-]`; selects service-scoped policies |
| `InstanceID` | `$POD_NAME` or hostname + random suffix | rollout bucket and telemetry identity |
| `Logger` | discard | `*slog.Logger`; passwords and values are never logged |

### `Redis` (used by `New`; ignored by `NewFromGoRedis` / `NewFromRueidis`)

| Field | Default | Notes |
|---|---|---|
| `Driver` | `go-redis` | `go-redis` or `rueidis` |
| `Addr`, `Addrs` | – | one address, or cluster seeds |
| `Cluster` | false | Redis Cluster (cache with `rueidis`) |
| `Username`, `Password` | – | fixed ACL credentials |
| `DB` | 0 | go-redis caching requires DB 0 |
| `TLSConfig` | nil | TLS to Redis |
| `PoolSize`, `DialTimeout`, `ReadTimeout`, `WriteTimeout` | client defaults | |

### `Detection`

| Field | Default | Notes |
|---|---|---|
| `Disabled` | false | disabling detection also disables caching |
| `Window` | 10s | 10ms–1h |
| `TopK` | 100 | new candidates evaluated per window (in addition to admitted keys) |
| `SampleRate` | 1.0 | (0, 1]; uncached reads recorded with probability p and weight 1/p |
| `MaxTrackingCandidates` | 1000 | detector counters and admission records |
| `MaxKeyBytes` | 1024 | longer keys are never tracked or cached |

### `Cache`

| Field | Default | Notes |
|---|---|---|
| `Mode` | `adaptive` | `adaptive`, `explicit` (pin patterns only), `off` |
| `MaxMemoryBytes` | 16 MiB | estimated entry memory (key + value + overhead) |
| `MaxEntries` | 1000 | the number of admitted keys is also bounded by `Detection.MaxTrackingCandidates` |
| `MaxEntryBytes` | 64 KiB | |
| `MaxStaleness` | 3s | ≥ 100ms; entries are refetched at least this often |
| `AdmissionCooldown` | 30s | base cooldown; doubles per strike, capped at 16× |
| `MinReadRate` | 2 reads/s | candidate threshold |
| `MinHitRatio` | 0.6 | predicted hit ratio needed for admission |
| `ExitHitRatio` | 0.3 | observed hit ratio below which a cached key goes to cooldown (must be ≤ `MinHitRatio`) |
| `MinObservationWindows` | 2 | consecutive qualifying windows before admission |
| `ExitWindows` | 3 | consecutive cold windows before demotion |
| `PinPatterns` | – | globs admitted without scoring (read-mostly keys only) |
| `DenyPatterns` | – | globs never cached |

Glob syntax: `*` matches any sequence (including `:`), `?` one byte, `\` escapes. At most 8 wildcards per pattern and 256 patterns per list.

### `Tracking`

| Field | Default | Notes |
|---|---|---|
| `HealthInterval` | 1s | ±10% jitter |
| `HealthTimeout` | 500ms | enforced by KeyShift even if the client ignores context deadlines |
| `TrackedPoolSize` | go-redis default | pool size of the dedicated tracked client |
| `DrainInterval` | 1ms | go-redis driver: how often idle tracked connections are swept for invalidations. Cached reads never touch the network, so this bounds how long an already-delivered invalidation waits. 1ms instead of go-redis's 5ms reduced stale reads 4–8× in the `local-hot` benchmark for about 6% more client CPU. |

### `Telemetry`

| Field | Default | Notes |
|---|---|---|
| `Endpoint` | empty (disabled) | controller base URL |
| `Token` | – | ingest token |
| `Interval` | 30s | jittered ±20%; the first report is spread over a full interval |
| `MaxCandidates` | 20 | ≤ 200 per batch |
| `FingerprintSecret` | random per process | ≥ 16 bytes; use the same secret across instances to aggregate keys |
| `DisplayKeyPatterns` | none | keys allowed to be sent by name |
| `DisableRemotePolicy` | false | ignore controller policies |
| `TLSConfig`, `HTTPClient` | – | controller transport |

### `Metrics`

| Field | Default | Notes |
|---|---|---|
| `Registerer` | nil | registers the collector; unregistered on `Close` |
| `DisableLatencyHistograms` | false | skip per-read timing |

## Policies

Policies are JSON documents distributed by the controller or applied with `client.ApplyPolicy`.

```json
{
  "mode": "adaptive",
  "admission": {
    "min_read_rate": 5,
    "min_hit_ratio": 0.7,
    "exit_hit_ratio": 0.3,
    "min_observation_windows": 2,
    "exit_windows": 3,
    "cooldown_seconds": 60,
    "max_entry_bytes": 32768
  },
  "budget": { "max_entries": 500, "max_memory_bytes": 8388608 },
  "pin_patterns": ["config:*"],
  "deny_patterns": ["balance:*", "lock:*", "ratelimit:*"],
  "deny_fingerprints": ["9f2c4e0b1a7d3e5f60718293a4b5c6d7"],
  "valid_for_seconds": 600,
  "comment": "checkout hardening"
}
```

Merge rules (`policy.Resolve`):

- Local cache budgets and maximum entry size are hard limits. Remote `budget` values and `max_entry_bytes` can only lower them.
- A remote policy cannot enable caching when the local `Mode` is `off`.
- Pin, deny, and deny-fingerprint lists are the union of local and remote lists.
- Remote policies can change admission thresholds and add pin patterns, including when the local mode is `explicit`. Treat policy administration as a privileged operation.
- Unknown fields, invalid modes, broken hysteresis (`exit_hit_ratio > min_hit_ratio`), oversized documents (> 256 KiB), and malformed fingerprints are rejected. The SDK keeps its last valid policy.
- A remote policy expires after `valid_for_seconds` (default 600) without controller contact, and the SDK reverts to its local policy.
- When an administrator deletes the policy that applies to a service, the controller tells SDKs on their next upload that no policy applies, and they revert to their local policy immediately.
- A policy installed with `ApplyPolicy` takes precedence over controller policies until `ClearPolicy` is called. Clearing it restores the local policy; a controller policy may be applied again on the next contact.

Validate a file locally:

```bash
keyshift policy validate -f policy.json
```

## Controller (`keyshift-controller`)

| Flag | Env | Default |
|---|---|---|
| `--addr` | `KEYSHIFT_ADDR` | `:8080` |
| `--data-dir` | `KEYSHIFT_DATA_DIR` | in-memory policies |
| `--tls-cert`, `--tls-key` | `KEYSHIFT_TLS_CERT`, `KEYSHIFT_TLS_KEY` | plain HTTP |
| `--ingest-tokens-file`, `--viewer-tokens-file`, `--admin-tokens-file` | `KEYSHIFT_INGEST_TOKENS`, `KEYSHIFT_VIEWER_TOKENS`, `KEYSHIFT_ADMIN_TOKENS` | required (comma or newline separated, ≥ 16 chars each) |
| `--insecure-no-auth` | – | loopback addresses only |
| `--max-instances` | – | 20000 |
| `--max-services` | – | 1000 |
| `--max-candidates` | – | 50 retained per instance |
| `--max-global-keys` | – | 5000 |
| `--instance-ttl` | – | 10m |
| `--fresh-for` | – | 2m |
| `--aggregate-interval` | – | 10s |
| `--ingest-rate`, `--ingest-burst` | – | 2000/s, 4000 |
| `--instance-min-interval` | – | 5s |
| `--metrics-public` | – | true (metrics contain no key names) |
| `--log-level`, `--log-json` | `KEYSHIFT_LOG_LEVEL`, `KEYSHIFT_LOG_FORMAT` | info, json |

## CLI environment

| Variable | Used by |
|---|---|
| `KEYSHIFT_CONTROLLER`, `KEYSHIFT_TOKEN` | `top`, `stats`, `instances`, `policy`, `doctor` |
| `KEYSHIFT_REDIS_ADDR`, `KEYSHIFT_REDIS_USERNAME`, `KEYSHIFT_REDIS_PASSWORD` | `doctor`, `inspect`, `bench` |
| `KEYSHIFT_FINGERPRINT_SECRET` | `inspect fingerprint`, `inspect key`, `inspect hotkeys --redact` |
