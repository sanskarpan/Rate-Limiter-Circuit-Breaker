# Distributed rate limit with Redis + fail-open

A distributed limiter enforces one shared limit across every instance of your
service by keeping the counters in Redis. Each `Allow` call runs an atomic Lua
script, so N replicas behind a load balancer collectively honour a single global
budget.

The core pieces:

- `store.NewRedis(store.RedisOptions{...})` — the Redis-backed store.
- `tokenbucket.NewDistributed(rate, capacity, store, prefix)` — the distributed
  limiter. It satisfies `ratelimit.Limiter`, so middleware and pipelines treat
  it exactly like the in-memory one.

> **Argument order.** `tokenbucket.NewDistributed(rate, capacity, ...)` takes
> **rate (tokens/sec) first, capacity (burst) second** — the *opposite* of the
> in-memory `tokenbucket.New(capacity, refillRate)`. Get this right or your
> limits will be swapped.

## Basic setup

```go
package main

import (
	"context"
	"log"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/store"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
)

func main() {
	s := store.NewRedis(store.RedisOptions{
		Addr:      "localhost:6379",
		KeyPrefix: "svc:", // isolates this service's keys in shared Redis
	})
	defer s.Close()

	ctx := context.Background()
	if err := s.Ping(ctx); err != nil {
		log.Fatalf("redis unreachable: %v", err)
	}

	// 100 req/s sustained, burst up to 200 — shared across ALL instances.
	lim := tokenbucket.NewDistributed(100, 200, s, "api")

	res := lim.Allow(ctx, "tenant:123")
	if !res.Allowed {
		// 429
	}
}
```

## Fail-open on a Redis outage

Redis becoming unreachable must not take your service down. Fail-open behaviour
is configured on the **store**, via `RedisOptions.Fallback`:

- **Default (`Fallback` nil):** each process falls back to a fresh *in-memory*
  store during an outage. Every instance then rate-limits against its own local
  counters with no shared state, so the effective global limit is multiplied by
  the number of instances. This **fails open** — availability is preserved at
  the cost of enforcement accuracy while Redis is down.
- **Fail-closed:** supply a deny-all `store.Store` as `Fallback` so requests are
  rejected while Redis is down. Use this only for security-critical limits where
  over-admitting is worse than rejecting.

```go
// Explicit fail-open: use a local in-memory store when Redis is unreachable.
s := store.NewRedis(store.RedisOptions{
	Addr:     "localhost:6379",
	Fallback: store.NewMemory(), // local counters during an outage
})
defer s.Close()
```

> The distributed limiter itself denies a request only if the store returns an
> error *and* no fallback can serve it. With a `Fallback` store wired (or the
> default in-memory fallback), an outage degrades to local limiting rather than a
> hard deny. Choose the policy that matches the limit's purpose.

## Server-time mode (clock-skew mitigation)

Time-sensitive distributed limiters (token bucket, GCRA) compute token refills
from a timestamp. If each application instance passes its own wall clock and
those clocks drift apart, the shared bucket can over- or under-refill. Enable
**server-time mode** so the Lua script uses Redis's own `TIME` command as the
single authoritative clock, immune to per-instance skew.

```go
// Enable at the store level — every distributed limiter built on this store
// inherits server-time mode.
s := store.NewRedis(store.RedisOptions{
	Addr:          "localhost:6379",
	UseServerTime: true,
})
defer s.Close()

lim := tokenbucket.NewDistributed(100, 200, s, "api") // inherits server-time
```

You can override per-limiter with `WithServerTime`:

```go
lim := tokenbucket.NewDistributed(100, 200, s, "api",
	tokenbucket.WithServerTime(true))
```

To surface skew for monitoring, the Redis store can report the offset between
its clock and Redis's:

```go
skew, exceeded, err := s.CheckServerTimeSkew(ctx, 250*time.Millisecond)
if err == nil && exceeded {
	log.Printf("clock skew %v exceeds threshold", skew)
}
```

> **Redis Cluster:** the Lua scripts use only single-key operations, so they are
> Cluster-compatible. Server-time mode issues a `TIME` call inside the script;
> it works on standalone and Cluster deployments alike.

## Distributed GCRA

For smooth pacing instead of bursty token-bucket behaviour, swap the
constructor — the interface is identical:

```go
import "github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/gcra"

// rate=100/s (float), burst=200 (int).
lim := gcra.NewDistributed(100, 200, s, "api")
```

## See also

- [docs/distributed.md](../distributed.md) — architecture, cluster notes, testing.
- [Per-tenant / per-API-key quotas](per-tenant-quotas.md)
- Runnable example: `examples/distributed/main.go`
</content>
