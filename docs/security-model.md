# Security model

## Assets

| Asset | Where |
|---|---|
| Redis data (values) | Redis; copies of admitted values in application memory |
| Redis credentials | application configuration |
| Key names | application; optionally the controller (display patterns only) |
| Workload metadata (rates, sizes, admission states) | SDK telemetry, controller memory |
| Policies | controller (`policies.json`), SDK memory |
| Controller tokens, fingerprint secret | Secrets / environment |

## Trust boundaries

```text
[application process + SDK] ──RESP/TLS──► [Redis]
           │
           └──HTTPS + bearer(ingest)──► [controller] ◄──HTTPS + bearer(viewer/admin)── [CLI]
                                             └── /metrics ──► [Prometheus] ──► [Grafana]
```

- The SDK trusts Redis and the application. It treats the controller as a **constrained** authority: policies are validated, size-limited, and can only tighten local limits or add denies. A compromised controller can disable or reduce caching for a service (availability and performance impact, shifting load back to Redis), but it cannot make an SDK cache a key that local configuration denies, exceed local memory limits, or read Redis data.
- The controller does not trust SDKs. Every upload is authenticated, rate limited (global token bucket, per-instance minimum interval), size limited (1 MiB compressed, 4 MiB decompressed, which protects against zip bombs), schema-validated (lengths, enums, fingerprint format, rate ranges, timestamps), and bounded in effect (instance, service, candidate, and global-key limits).
- Nothing in the control plane can execute Redis commands: the controller has no Redis connection at all.
- `/metrics` is unauthenticated by default and contains workload metadata (service names, rates, and the names of hot keys that SDKs explicitly allow via `DisplayKeyPatterns`; other keys appear as truncated fingerprints). Restrict network access to it, or use `--metrics-public=false`.

## Threats and mitigations

| Threat | Mitigation |
|---|---|
| Key names leak through telemetry | Only HMAC-SHA256 fingerprints (128-bit) are sent. Raw names only for keys matching explicit `DisplayKeyPatterns`. No values are ever sent. |
| Fingerprints reversed by guessing | Keyed HMAC: guessing requires the secret. Protect `FingerprintSecret` like a credential. Holders of the secret can confirm guessed key names. |
| Key names in metrics | No key labels exist (tested by `TestMetricsCollectorHasNoKeyLabels`); label sets are fixed. |
| Credentials in logs | Passwords and tokens are never logged; tokens are compared as SHA-256 digests in constant time. |
| Forged or flooded telemetry | Ingest token; global and per-instance rate limits; size and schema limits; bounded registry (`--max-instances`, `--max-services`); idempotent sequence numbers. |
| Malicious policy | Admin token required; schema validation on controller and SDK; local hard limits; per-SDK `DisableRemotePolicy`; history and rollback. |
| Workload metadata via `/metrics` | No values; keys only as fingerprints unless allowed by `DisplayKeyPatterns`; series bounded by `--metrics-top-keys`, `--max-services`, and opt-in `--metrics-instances`; `--metrics-public=false` requires a viewer token. |
| Unauthenticated controller on a network | Refuses to start without tokens; `--insecure-no-auth` only on loopback. |
| Stale cached authorization or financial data | Deny patterns, `GetFresh`, `explicit` mode; see [consistency.md](consistency.md). |
| Local memory exhaustion | Hard entry and memory limits in the client library cache; bounded detector, admission table, telemetry aggregate, and queue. |
| Privileged Redis commands | The SDK uses only `GET`, `SET`, `DEL`, `PING`, `INFO server`, `CLIENT TRACKINGINFO`, plus the client library's connection setup (`HELLO`, `CLIENT TRACKING`, `CLIENT SETNAME`/`SETINFO`). `HOTKEYS` is CLI-only and opt-in. `MONITOR` is never used. |
| Supply chain | Pinned dependencies (`go.sum`); `govulncheck` in CI; Dependabot; distroless non-root images with read-only filesystem compatibility. |

## Data retention

- **SDK**: in-memory only; cached values live until invalidation, eviction, `max_staleness`, or `Close`.
- **Controller**: telemetry is held in memory as the latest batch per instance, dropped after `--instance-ttl` (default 10 min) without reports, plus bounded time series (360 points) and key trends (top 20 keys × 60 points). Only policies and policy history (last 200 changes) are written to disk. Nothing is persisted when `--data-dir` is empty.

## Transport security

- Redis: `RedisConfig.TLSConfig` (or your own client's TLS settings).
- Controller: `--tls-cert/--tls-key` (TLS 1.2+), or terminate TLS at an ingress or service mesh. SDKs accept `TelemetryConfig.TLSConfig` / `HTTPClient` for custom CAs and mTLS.

## Limitations

- Local caching places copies of Redis values in application memory. Any process memory disclosure (core dumps, debug endpoints, heap profiles) can expose cached values.
- Fingerprints with a shared secret allow everyone holding the secret to confirm whether a guessed key name is hot. Use separate secrets per administrative scope.
- Tokens are static bearer tokens; rotate them by configuring old and new tokens together, then removing the old one. OIDC or SSO is not built into the controller API; put it behind your authenticating proxy (Grafana has its own authentication) for user-level access control and auditing.
- The controller is a single replica; policy writes are not replicated.

Report vulnerabilities as described in [SECURITY.md](../SECURITY.md).
