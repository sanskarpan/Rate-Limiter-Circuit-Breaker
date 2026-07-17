# Per-tenant / per-API-key quotas

Two building blocks:

1. **Keyed limiters** — a single limiter instance holds one bucket per key, so
   per-tenant quotas need no map management. Just build the key from the tenant
   ID.
2. **Composite `AND`** — stack tiers (e.g. a global cap *and* a per-tenant cap)
   so a request must satisfy every layer. `composite.New(composite.AND, ...)`
   uses an atomic two-phase check-then-consume: it consumes from all limiters
   only if all of them would allow, so a denial never leaves one tier debited.

## Per-tenant quota (keyed limiter)

```go
import "github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"

// One instance, one bucket per tenant key, idle tenants auto-evicted.
perTenant := tokenbucket.New(1000, 100, tokenbucket.WithIdleCleanup(10*time.Minute))
defer perTenant.Close()

res := perTenant.Allow(ctx, "tenant:"+tenantID)
if !res.Allowed {
	http.Error(w, "quota exceeded", http.StatusTooManyRequests)
	return
}
```

## Stacked tiers: global cap AND per-tenant cap

Build one limiter per tier and combine them with `composite.AND`. The composite
itself satisfies `ratelimit.Limiter`, so it drops straight into middleware or a
pipeline.

```go
import (
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/composite"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
)

// Tier 1: a global ceiling protecting the whole service.
global := tokenbucket.New(5000, 5000) // 5000 burst, refill 5000/s
defer global.Close()

// Tier 2: a per-tenant quota.
perTenant := tokenbucket.New(1000, 100)
defer perTenant.Close()

// AND: the request must pass BOTH tiers, atomically.
gate := composite.New(composite.AND, global, perTenant)
defer gate.Close()

// The key threads through to every underlying limiter. Use a per-tenant key;
// the global limiter simply keeps one bucket per key too (or key it to a single
// constant if you want a truly shared global bucket — see note below).
if !gate.Allow(ctx, "tenant:"+tenantID).Allowed {
	http.Error(w, "rate limited", http.StatusTooManyRequests)
	return
}
```

> **Note on the global key.** `composite.AND` passes the *same* key to every
> underlying limiter. If you want the global tier to be one shared bucket across
> all tenants (rather than per-tenant), don't put it in the composite — check it
> separately with a constant key first, then check the per-tenant limiter:
>
> ```go
> if !global.Allow(ctx, "global").Allowed { /* 429 */ }
> if !perTenant.Allow(ctx, "tenant:"+tenantID).Allowed { /* 429 */ }
> ```
>
> Use the composite when both tiers should key on the same value (e.g. a global
> *per-tenant* cap plus a stricter *per-endpoint-per-tenant* cap).

## Tiered plans (free vs. pro) with a keyed lookup

Give each plan its own limiter instance, then dispatch on the tenant's plan:

```go
limiters := map[string]ratelimit.Limiter{
	"free": tokenbucket.New(60, 1),    // 60 burst, 1/s
	"pro":  tokenbucket.New(6000, 100), // 6000 burst, 100/s
}

lim := limiters[planFor(tenantID)]
if !lim.Allow(ctx, "tenant:"+tenantID).Allowed {
	http.Error(w, "quota exceeded", http.StatusTooManyRequests)
	return
}
```

Remember to `Close()` each limiter on shutdown.

## In middleware

Because the composite satisfies `ratelimit.Limiter`, wire it as middleware and
key by API key:

```go
r.Use(ginmw.RateLimit(gate, ginmw.WithKeyFunc(ginmw.KeyByHeader("X-API-Key"))))
```

## See also

- [Rate limit per IP](ratelimit-per-ip-frameworks.md)
- [Cost / weight-based limiting](cost-weighted-limiting.md)
- [Distributed rate limit with Redis](distributed-redis-failopen.md)
</content>
