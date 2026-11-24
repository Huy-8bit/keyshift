# ADR 0001: Hot-key detection algorithm

- Status: accepted
- Date: 2026-11-20

## Context

Each SDK instance needs to identify locally hot keys among potentially millions of distinct keys. The detector must use bounded memory, support concurrent updates, keep per-read overhead low, and adapt as the workload changes.

## Options considered

| Option | Memory | Per-update cost | Top-K | Notes |
|---|---|---|---|---|
| Exact hash map | O(distinct keys) | O(1) | sort | unbounded; rejected |
| Count-Min Sketch + heap | O(w·d) + K | d hashes | needs a separate candidate heap | over-estimates collisions; frequency only, candidates still needed |
| HeavyKeeper | O(w·d) | d hashes + probabilistic decay | good | more tuning parameters; probabilistic decay complicates explanations |
| **Space-Saving** | **O(capacity)** | **O(log capacity)** | **built in** | deterministic error bound per key (`count − err ≤ true ≤ count`) |

## Decision

Use **Space-Saving**, sharded by key hash (up to 64 shards, power of two) with one mutex per shard, plus **per-window exponential decay** (halve counters at rotation, drop zeros). Track a per-window count (lower bound) and the last value size per counter.

Mitigations for the known weaknesses:

- *Lock contention on the hottest key*: once admitted, a key is counted with a per-key atomic counter and folded into the detector at rotation.
- *Global atomics*: the observed counter is striped across cache lines; recorded counts live under shard locks.
- *High read rates*: optional sampling with weight `1/p`.

## Consequences

- Memory is bounded by `MaxTrackingCandidates`; keys beyond it can displace each other, which is acceptable because only the top keys matter.
- In the recorded benchmark, recall@50 was 1.0 on a 1M-key Zipf(1.1) stream with 500 counters; updates took about 150 ns/op unsampled and 32 ns/op at 10% sampling under 8-way contention, with 0 allocations per observation. These measurements are workload- and hardware-dependent.
- Decay makes the decayed count a long-memory estimate, which the admission engine uses as a conservative, smoothed read rate.
