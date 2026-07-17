# Algorithm Comparison

> Part of the [Resilience](../README.md) documentation.
> See also: [Algorithm deep-dives](algorithms.md) · [Distributed rate limiting](distributed.md)

Canonical identifiers used by the demo server and HTTP API: `token_bucket`,
`leaky_bucket`, `sliding_window` (log and counter variants),
`fixed_window`, `gcra`, `adaptive`.

## Quick Comparison Table

| Algorithm | Burst | Exact | Memory | Distributed | Latency Added | Use When |
|-----------|-------|-------|--------|-------------|--------------|----------|
| Token Bucket | ✅ | ✅ | O(keys) | ✅ | None | General API rate limiting |
| Leaky Bucket | ❌ | ✅ | O(queue) | ⚠️ | Queue depth | Constant output rate |
| Sliding Window Log | ❌ | ✅ | O(req) | ✅ | None | Exact counting |
| Sliding Window Counter | ❌ | ~99% | O(keys) | ✅ | None | High-volume approximate |
| Fixed Window | ❌ | ✅ | O(keys) | ✅ | None | Simplest, boundary burst ok |
| GCRA | ✅ | ✅ | O(keys) | ✅ | None | High-performance API |

## Performance

All in-memory limiters operate in the tens of nanoseconds per `Allow` call and
allocate nothing on the hot path. Relative cost ordering (cheapest first):

1. **Fixed Window / Sliding Counter** — a couple of counter updates.
2. **Token Bucket / GCRA** — O(1) arithmetic per key, zero allocations.
3. **Sliding Window Log** — grows with the number of timestamps in the window.
4. **Leaky Bucket** — channel-backed queue per key.

Run the suite yourself to get numbers for your hardware and Go version:

```bash
make bench
```

## Decision Guide

### Need burst allowance?
→ **Token Bucket** or **GCRA**
- Redis-optimal: GCRA
- Well-understood: Token Bucket

### Need strictly constant output rate?
→ **Leaky Bucket** (accepts queuing latency)

### Need exact count, memory not a concern?
→ **Sliding Window Log**

### Need approximate count, minimal memory?
→ **Sliding Window Counter** (1% error at boundary)

### Simplest possible implementation?
→ **Fixed Window** (if boundary burst is acceptable)

## Boundary Burst Explained

Fixed Window allows a burst of `2 × limit` requests at a window boundary:

```
Window N:    [           100 requests           ]
Window N+1:  [100 requests           ...]
                         ↑
             200 requests in 1 second possible here
```

Sliding Window algorithms eliminate this: the effective window always spans
exactly `window` duration ending at `now`.

## Distributed Considerations

Every distributed variant executes a single atomic Lua script on one key, so it
is safe under concurrency and compatible with Redis Cluster.

| Algorithm | Distributed variant | Atomic? | Notes |
|-----------|--------------------|:-------:|-------|
| Token Bucket | `tokenbucket.NewDistributed` | ✅ | Lua read-refill-consume in one round-trip |
| GCRA | `gcra.NewDistributed` | ✅ | Single-timestamp update; most Redis-efficient |
| Fixed Window | `fixedwindow.NewDistributed` | ✅ | INCR + PEXPIRE inside one script |
| Sliding Log | `slidingwindow.NewDistributedLog` | ✅ | Sorted-set trim + count in one script |
| Sliding Counter | `slidingwindow.NewDistributedCounter` | ✅ | Two-window weighted count |
| Leaky Bucket | — | — | No distributed variant (ordered queue is hard to distribute) |

See [Distributed rate limiting](distributed.md) for constructor signatures,
fallback modes, and key naming.

---

See also: [Algorithm deep-dives](algorithms.md) · [Distributed rate limiting](distributed.md) · [README](../README.md)
