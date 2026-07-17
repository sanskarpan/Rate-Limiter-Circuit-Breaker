# Distributed Rate Limiting

> Part of the [Resilience](../README.md) documentation.
> See also: [Algorithm deep-dives](algorithms.md) · [Algorithm comparison](comparison.md)

## Overview

Every rate limiter in this library has a Redis-backed distributed variant. The
distributed implementations satisfy the same `ratelimit.Limiter` interface as
their in-memory counterparts — your application code does not change, only the
constructor.

Redis is an **optional** dependency. The core in-memory algorithms have zero
external runtime dependencies; you only pull in `go-redis` when you use the
Redis store.

## Architecture

```
Application → Distributed Limiter → Redis Store → Redis
                     │
                     └── (on Redis failure) → Fallback Store
```

## Redis Store

Create a store with `store.NewRedis`. It returns a `*store.RedisStore`
immediately (it does not dial until first use); call `Ping` to verify
connectivity.

```go
import "github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/store"

s := store.NewRedis(store.RedisOptions{
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

    // Prepended to all keys (default "rl:").
    KeyPrefix: "myapp:rl:",
})
defer s.Close()

if err := s.Ping(context.Background()); err != nil {
    log.Fatalf("cannot reach Redis: %v", err)
}
```

## Distributed Token Bucket

```go
import "github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"

// NewDistributed(rate, capacity, store, prefix)
//   rate:     tokens added per second (sustained rate)
//   capacity: maximum burst size
//   prefix:   key namespace (defaults to "rl" if empty)
limiter := tokenbucket.NewDistributed(100, 20, s, "myapp")
result := limiter.Allow(ctx, "user:123")
```

The Redis key is `myapp:tokenbucket:user:123`. The Lua script atomically:

1. Reads the current token count and last-refill timestamp.
2. Computes the new token count with lazy refill.
3. Checks whether `n` tokens are available.
4. Decrements if allowed.
5. Writes the new state back with a TTL.

## Distributed GCRA

```go
import "github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/gcra"

// NewDistributed(rate, burst, store, prefix)
//   rate:  requests per second
//   burst: allowed burst size
limiter := gcra.NewDistributed(100, 5, s, "myapp")
```

The Redis key is `myapp:gcra:user:123`. GCRA is the most Redis-efficient
algorithm — its entire state is a single timestamp updated in one atomic
script, making it ideal for high-throughput distributed deployments.

## Other Distributed Limiters

All follow the `NewDistributed(..., s store.Store, prefix string)` convention:

```go
import (
    "github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/fixedwindow"
    "github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/slidingwindow"
)

// Fixed window: NewDistributed(limit, window, store, prefix)
fw := fixedwindow.NewDistributed(1000, time.Hour, s, "myapp")

// Sliding window log: NewDistributedLog(limit, window, store, prefix)
swl := slidingwindow.NewDistributedLog(100, time.Minute, s, "myapp")

// Sliding window counter: NewDistributedCounter(limit, window, store, prefix)
swc := slidingwindow.NewDistributedCounter(100, time.Minute, s, "myapp")
```

Leaky bucket has no distributed variant — a strictly-ordered FIFO queue is hard
to distribute correctly. Use GCRA or a distributed token bucket for
distributed constant-rate needs.

## Key Naming Convention

All distributed limiters use the pattern:

```
{prefix}:{algorithm}:{user_key}
```

Examples:

- `myapp:tokenbucket:user:123`
- `myapp:gcra:api-key:xyz`
- `myapp:fixedwindow:ip:192.168.1.1`

## Fallback Behaviour

When Redis is unreachable, the limiter delegates to a fallback `store.Store`.
The fallback is set on `RedisOptions.Fallback`:

- **Default (nil fallback):** each process installs a fresh in-memory store.
  During a Redis outage every instance rate-limits against its own local
  counters with no shared state, so the effective global limit is multiplied by
  the number of instances. This is **fail-open** — it preserves availability at
  the cost of enforcement accuracy.
- **Fail-closed:** supply a deny-all store so requests are rejected while Redis
  is down. Use this for security-critical limits where over-admitting is worse
  than rejecting.

```go
s := store.NewRedis(store.RedisOptions{
    Addr:     "localhost:6379",
    Fallback: myDenyAllStore, // reject when Redis is unavailable
})
```

## Multi-Region and Redis Cluster

The Lua scripts use only single-key operations, so they are compatible with
Redis Cluster. For true multi-region rate limiting, use Redis Cluster or a
Redis-compatible distributed database (e.g. AWS ElastiCache with cluster mode,
or Upstash Redis).

## Testing Distributed Limiters

Integration tests spin up a real Redis via Docker and run behind the
`integration` build tag:

```bash
go test -tags=integration ./ratelimit/...
```

## Performance

A Redis round-trip adds roughly 0.5–2 ms per request depending on the network.
For single-instance rate limiting always prefer the in-memory implementations,
which operate in the tens of nanoseconds — orders of magnitude faster than a
network round-trip. Use connection pooling and pipelining for high-throughput
distributed scenarios.
</content>
