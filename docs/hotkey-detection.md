# Hot-key detection

Each SDK instance finds its own hot keys with a bounded streaming detector (`internal/detector`). Algorithm selection is recorded in [ADR 0001](adr/0001-hotkey-detection-algorithm.md).

## Algorithm

**Sharded Space-Saving with per-window decay.**

- The detector holds `MaxTrackingCandidates` counters (default 1000), split across up to 64 shards by key hash. Each shard is a min-heap plus an index map, protected by its own mutex.
- An observation of a tracked key increments its counter. An untracked key replaces the minimum counter of its shard and inherits that count as its error bound (Space-Saving). Memory is O(capacity) regardless of key cardinality.
- At each window rotation every counter is halved in place, and counters that reach zero are removed. Rotation does not allocate proportionally to the key space.
- Each counter also records the **window count** (a lower bound for the true count in the window) and the last observed value size.

Guarantees (tested in `TestTopKRecallZipf` and `FuzzDetector`): for every reported key, `score - err ≤ true count ≤ score`, and the window count never exceeds the true count.

## Four quantities that are never mixed

| Quantity | Where it comes from |
|---|---|
| logical reads (local request frequency) | every `Get`/`GetBytes`/`GetFresh` call and every key of an `MGet`, including local cache hits |
| Redis reads | direct reads + bypasses + cache fills (requests that reached Redis) |
| reads avoided | local cache hits |
| global frequency | controller aggregation of per-instance telemetry |
| Redis-side cost | optional `HOTKEYS` sessions (CPU µs, network bytes), reported separately |

Local hits are counted on a per-key atomic counter and folded into the detector at rotation, so a cached key keeps its rank.

## Bounded overhead

| Measure | Value (Apple M1 Pro, 8 threads, `make bench`) |
|---|---|
| `Observe`, Zipf keys, parallel | ~150 ns/op, 0 allocs |
| `Observe`, 10% sampling, parallel | ~32 ns/op, 0 allocs |
| window rotation, 1000 counters | ~0.47 ms including 4096 observations; 1 allocation |
| hot path for an admitted key | one atomic load + one map lookup + one atomic add |

The hottest key is the worst case for a sharded lock, because every reader contends on one shard. KeyShift sidesteps this: once admitted, a key's reads use a per-key atomic counter instead of the detector.

`SampleRate < 1` trades accuracy for CPU on very high read rates; sampled observations are weighted by `1/SampleRate`. Keys longer than `MaxKeyBytes` are counted as dropped and never cached.

## Accuracy

On a 500,000-access Zipf(1.1) stream over 1,000,000 distinct keys, recall of the true top 50 is 1.00 with 500, 1,000, and 4,000 counters. Coverage, the share of recorded reads attributed to the reported top keys, is reported per window. Low coverage means traffic is spread thinly and local caching cannot help much.

## Smoothing

Admission decisions for keys that are not yet cached use the detector's **decayed** count (lower bound `score - err`), which approximates twice the recent per-window count. Cached keys use exact per-window counters with an EWMA (α = 0.5). This prevents keys near a threshold from flipping in and out of the cache because of Poisson noise in a single short window. On the lab demo it reduced admission churn from 645 to 210 transitions per minute at the same hit ratio.

## Redis-native HOTKEYS (optional)

Redis Open Source 8.6 added `HOTKEYS START/GET/STOP/RESET`, which measure server CPU time and network bytes per key. KeyShift exposes it only as a privileged, opt-in diagnostic:

```bash
keyshift inspect hotkeys --redis localhost:6379 --duration 10s --metrics cpu,net --count 10
keyshift inspect hotkeys --redis 127.0.0.1:7001 --cluster --redact   # fingerprints instead of names
```

- Capability, version, and ACL permission are detected first (`COMMAND INFO HOTKEYS`, `HOTKEYS GET`).
- Sessions are time-bounded (1 s–10 min) and auto-stop on the server.
- If another session is already running, KeyShift reports a conflict and never stops or resets it.
- On cancellation (Ctrl-C), KeyShift stops and resets only the session it started.
- On Cluster, sessions run on every master in parallel.
- The SDK never uses HOTKEYS, and local detection never depends on it. `MONITOR` is never used.
