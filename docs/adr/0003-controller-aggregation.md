# ADR 0003: Controller state and aggregation semantics

- Status: accepted
- Date: 2026-11-20

## Context

The controller is designed for approximately 5,000 SDK instances reporting every 30 seconds. It must handle retries, duplicates, restarts, and autoscaling without becoming a request-path dependency or requiring additional infrastructure for the first release.

## Decision

- **In-memory registry of the latest batch per instance**, bounded by `--max-instances`, `--max-services`, and `--max-candidates` per instance. Instances expire after `--instance-ttl`.
- **Idempotency by `(instance, seq)`**: a batch with `seq ≤ last seq` is acknowledged as a duplicate and not aggregated. Automatically generated instance IDs include a random suffix to distinguish restarts; operators who supply `InstanceID` must manage its uniqueness themselves.
- **Aggregation by recomputation**: every `--aggregate-interval`, the global view is rebuilt from the latest batch of every fresh instance. Counts are never accumulated, so retries cannot double count and departed instances drop out automatically.
- **Bounded outputs**: the global ranking keeps `--max-global-keys`, time series keep 360 points, and trends keep 20 keys × 60 points.
- **Policies persisted** as one JSON file with atomic replacement; versions are monotonic; history keeps 200 entries.
- **Local admission stays authoritative**: the controller publishes guidance and limits (policies) but never pushes per-key admissions to pods.

## Consequences

- Restarting the controller loses only the current view, which rebuilds within one report interval.
- Per-key history is coarse (trends for top keys only).
- In the recorded logical simulation, one replica aggregated 5,000 instances × 20 candidates in about 90–105 ms (214 ms with 50 candidates), and sustained about 8,000 uploads/s against a target of 167/s ([benchmarks](../benchmarks.md)). This does not establish distributed production capacity. Horizontal scaling would need additional coordination or sharding, which is not implemented.
