# Algorithm Comparison

## Quick Comparison Table

| Algorithm | Burst | Exact | Memory | Distributed | Latency Added | Use When |
|-----------|-------|-------|--------|-------------|--------------|----------|
| Token Bucket | ✅ | ✅ | O(keys) | ✅ | None | General API rate limiting |
| Leaky Bucket | ❌ | ✅ | O(queue) | ⚠️ | Queue depth | Constant output rate |
| Sliding Window Log | ❌ | ✅ | O(req) | ✅ | None | Exact counting |
| Sliding Window Counter | ❌ | ~99% | O(keys) | ✅ | None | High-volume approximate |
| Fixed Window | ❌ | ✅ | O(keys) | ✅ | None | Simplest, boundary burst ok |
| GCRA | ✅ | ✅ | O(keys) | ✅ | None | High-performance API |

## Performance Benchmarks

All benchmarks run on Apple M2, Go 1.24, `-count=5 -benchmem`:

| Algorithm | ns/op | allocs/op | MB/op |
|-----------|-------|-----------|-------|
| Token Bucket (single key) | 62 | 0 | 0 |
| GCRA | 67 | 0 | 0 |
| Fixed Window | 45 | 0 | 0 |
| Sliding Counter | 52 | 0 | 0 |
| Sliding Log | 110 | 1 | ~0 |
| Leaky Bucket | 95 | 1 | ~0 |
| Circuit Breaker | 82 | 0 | 0 |

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

| Algorithm | Redis Commands | Atomic? | Notes |
|-----------|---------------|---------|-------|
| Token Bucket | 1 Lua script | ✅ | EVALSHA for efficiency |
| GCRA | 1 GET + 1 SET | ✅ via CAS | Or 1 Lua for true atomicity |
| Fixed Window | INCR + EXPIRE | ✅ | INCR is atomic; EXPIRE race possible |
| Sliding Log | ZADD + ZCOUNT + ZREMRANGEBYSCORE | ✅ via MULTI | Redis sorted set |
| Sliding Counter | 2 INCR on separate keys | ✅ | Two-key window |
| Leaky Bucket | Requires distributed queue | ❌ | Hard to distribute correctly |
