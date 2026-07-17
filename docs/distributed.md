# Distributed Rate Limiting

## Overview

All algorithms in this library have a distributed variant backed by Redis. The distributed implementations maintain the same `Limiter` interface — your application code doesn't change.

## Architecture

```
Application → Distributed Limiter → Redis Store → Redis
                     ↓
               (on Redis failure)
               Fallback Store → In-Memory Limiter
```

## Redis Store

```go
import "github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/store"

redisStore, err := store.NewRedis(store.RedisConfig{
    Addr:     "localhost:6379",
    Password: os.Getenv("REDIS_PASSWORD"),
    DB:       0,

    // Connection pool
    PoolSize:     10,
    MinIdleConns: 2,

    // Timeouts
    DialTimeout:  5 * time.Second,
    ReadTimeout:  3 * time.Second,
    WriteTimeout: 3 * time.Second,

    // Retry on transient failure
    MaxRetries:      3,
    MinRetryBackoff: 8 * time.Millisecond,
    MaxRetryBackoff: 512 * time.Millisecond,
})
```

## Distributed Token Bucket

```go
import "github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"

// Global limit: 100 req/s shared across all instances
limiter := tokenbucket.NewDistributed(
    redisStore,
    100,    // capacity
    10.0,   // refillRate tokens/sec
    tokenbucket.WithKeyPrefix("myapp:rl"),
)
```

The Redis key is `myapp:rl:tokenbucket:{key}`. The Lua script atomically:
1. Reads current tokens + last refill timestamp
2. Computes new token count with lazy refill
3. Checks if n tokens available
4. Decrements if allowed
5. Updates key with new state + TTL

## Distributed GCRA

```go
import "github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/gcra"

limiter := gcra.NewDistributed(
    redisStore,
    100,           // limit
    5,             // burst
    time.Second,   // window
    gcra.WithKeyPrefix("myapp:rl"),
)
```

GCRA is the most Redis-efficient algorithm: single `GET` + conditional `SET` in a CAS loop.

## Key Naming Convention

All distributed limiters use the pattern:
```
{prefix}:{algorithm}:{user_key}
```

Examples:
- `myapp:rl:tokenbucket:user:123`
- `myapp:rl:gcra:api-key:xyz`
- `myapp:rl:fixedwindow:ip:192.168.1.1`

## Fallback Behaviour

When Redis is unavailable (all retries exhausted), the limiter falls back to:
- **Default**: in-memory limiter (allows traffic through; no global rate limit)
- **DenyAll**: reject all requests (use for security-critical limits)
- **Custom**: implement the `store.Store` interface

```go
limiter := tokenbucket.NewDistributed(
    redisStore,
    100, 10.0,
    tokenbucket.WithFallback(store.DenyAll{}), // strict mode
)
```

## Multi-Region Considerations

For true multi-region rate limiting, use Redis Cluster or a Redis-compatible
distributed database (e.g., AWS ElastiCache with cluster mode, Upstash Redis).

The Lua scripts in this library use only single-key operations, making them
compatible with Redis Cluster (keys are hash-tagged by `{prefix}:{algo}:{user_key}`).

## Testing Distributed Limiters

Integration tests require Docker:

```bash
# Run integration tests (requires Docker)
go test -tags=integration -run Integration ./ratelimit/...
```

The integration tests use `testcontainers-go` to spin up a real Redis instance.

## Performance

Redis round-trip adds ~0.5-2ms latency per request depending on network.
Use connection pooling and pipelining for high-throughput scenarios.

For local rate limiting (single instance), always prefer the in-memory implementations
which operate at 45-110 ns/op — 10,000x faster than Redis round-trips.
