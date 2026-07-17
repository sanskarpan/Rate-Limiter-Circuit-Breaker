# Cost / weight-based limiting

Not all requests are equal: a bulk export or a large query should consume more
of a client's budget than a cheap health check. Charge each request a **cost**
(weight) in tokens instead of a flat 1.

Two ways to spend more than one token:

- `lim.AllowN(ctx, key, n)` — consume `n` whole tokens, all-or-nothing.
- `lim.AllowCost(ctx, key, cost float64)` (token bucket only) — consume a
  **fractional** cost, all-or-nothing, since the token bucket stores fractional
  tokens internally.

Size the bucket so the most expensive request still fits: a request whose cost
exceeds `capacity` can never be admitted.

## Direct usage

```go
import "github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"

// Capacity 100 tokens, refilling 20/s.
lim := tokenbucket.New(100, 20)
defer lim.Close()

// A cheap endpoint costs 1.
lim.Allow(ctx, "user:42")

// An expensive endpoint costs 10 tokens.
res := lim.AllowN(ctx, "user:42", 10)
if !res.Allowed {
	// 429 — retry after res.RetryAfter
}

// Fractional cost (token bucket only): charge 2.5 tokens.
res = lim.AllowCost(ctx, "user:42", 2.5)
```

`AllowN`/`AllowCost` record the charged cost in `res.Metadata["cost"]` for
observability.

## In HTTP middleware

Every framework middleware accepts `WithCost`, a function that computes the cost
of each request. Values below 1 are clamped to 1, so a request always spends at
least one token. Under the hood the middleware calls `AllowN` when the cost is
> 1.

```go
import (
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/contrib/ginmw"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
)

lim := tokenbucket.New(100, 20)
defer lim.Close()

r.Use(ginmw.RateLimit(lim,
	ginmw.WithKeyFunc(ginmw.KeyByHeader("X-API-Key")),
	ginmw.WithCost(func(c *gin.Context) int {
		switch {
		case strings.HasPrefix(c.FullPath(), "/export"):
			return 10 // heavy: costs 10 tokens
		case strings.HasPrefix(c.FullPath(), "/search"):
			return 5
		default:
			return 1
		}
	}),
))
```

The same `WithCost` option exists for `chimw`, `echomw`, `fibermw`, and
`connectmw` (the core `ratelimit/middleware` and gRPC interceptor as well). The
middleware emits an `X-RateLimit-Cost` response header showing what the request
was charged.

## Cost by request size

Charge proportionally to a declared payload size, for instance:

```go
ginmw.WithCost(func(c *gin.Context) int {
	// 1 token per 10 KB of Content-Length, minimum 1.
	if c.Request.ContentLength <= 0 {
		return 1
	}
	return int(c.Request.ContentLength/10_000) + 1
})
```

## See also

- [Per-tenant / per-API-key quotas](per-tenant-quotas.md)
- [Rate limit per IP](ratelimit-per-ip-frameworks.md)
</content>
