# Protect a flaky downstream

Compose a **circuit breaker** (stop hammering a failing dependency), **retry
with a budget** (recover from transient errors without a retry storm), and a
**hedge or fallback** (cut tail latency / degrade gracefully) using the
`pipeline` builder.

The pipeline applies stages in a fixed canonical order:

```
load shed → rate limit → bulkhead → timeout → circuit breaker → retry → custom (Use)
```

so the breaker sees a single logical attempt and retries wrap the breaker.

## Circuit breaker + retry (with retry budget)

A **retry budget** caps retries as a fraction of throughput, so a cohort of
failing requests can't multiply the offered load on a brownout dependency.

```go
package main

import (
	"context"
	"errors"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/pipeline"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/retry"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/retry/backoff"
)

func build() *pipeline.Pipeline {
	// Open after 5 failures in the last 10 requests; probe once after 30s.
	cb := circuitbreaker.New(circuitbreaker.Config{
		Name:                "downstream",
		WindowType:          circuitbreaker.CountBased,
		WindowSize:          10,
		FailureThreshold:    5,
		HalfOpenMaxRequests: 1,
		OpenTimeout:         30 * time.Second,
	})

	// Retry policy: up to 3 attempts, exponential backoff, don't retry once the
	// breaker is open (that error is not transient — fail fast instead).
	policy := retry.New(
		retry.WithMaxAttempts(3),
		retry.WithBackoff(backoff.Exponential(100*time.Millisecond, 2*time.Second)),
		retry.WithRetryIf(func(err error) bool {
			return !errors.Is(err, circuitbreaker.ErrCircuitOpen)
		}),
	)

	// Retry budget: allow retries up to 10% of throughput, floor 1/s, burst 10.
	budget := retry.NewBudget(retry.BudgetConfig{
		Ratio:        0.1,
		MinPerSecond: 1,
		Burst:        10,
	})

	return pipeline.New().
		Timeout(2 * time.Second).
		CircuitBreaker(cb).
		RetryWithBudget(policy, budget).
		Build()
}

func call(ctx context.Context, p *pipeline.Pipeline) error {
	return p.Execute(ctx, func(ctx context.Context) error {
		return callDownstream(ctx) // your RPC/HTTP call
	})
}
```

## Adding a fallback

The pipeline builder doesn't have a dedicated fallback stage, but a `Use` stage
wraps the rest of the pipeline. Put the fallback *last* so it catches everything
below it (including `ErrCircuitOpen`):

```go
import "github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/fallback"

p := pipeline.New().
	Timeout(2 * time.Second).
	CircuitBreaker(cb).
	RetryWithBudget(policy, budget).
	Use(func(ctx context.Context, fn func(context.Context) error) error {
		// fn is everything above (timeout → breaker → retry).
		return fallback.Do(ctx, fn, func(ctx context.Context, err error) error {
			// Serve a cached / default response instead of failing.
			return serveFromCache(ctx)
		})
	}).
	Build()
```

`fallback.Do(ctx, fn, fb)` runs `fn`; if it returns an error it runs
`fb(ctx, err)`. For a typed result use
`fallback.DoWithResult[T](ctx, fn, fb)`.

Prefer the breaker's built-in helper for the simplest breaker+fallback case:

```go
err := cb.ExecuteWithFallback(ctx,
	func(ctx context.Context) error { return callDownstream(ctx) },
	func(ctx context.Context, err error) error { return serveFromCache(ctx) },
)
```

## Hedging to cut tail latency

A hedge fires a second attempt if the first hasn't returned within a delay, and
takes whichever finishes first — useful when p99 latency is your problem rather
than outright failure. Hedging is a standalone helper (not a pipeline stage):

```go
import "github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/fallback"

// Send a backup request if the primary hasn't answered within 50ms.
res := fallback.Hedge(ctx, 50*time.Millisecond, func(ctx context.Context) error {
	return callDownstream(ctx)
})
if res.Err != nil {
	// both attempts failed
}
_ = res.Primary // true if the primary attempt won the race
```

- `fallback.HedgeN(ctx, delay, n, fn)` fans out up to `n` staggered attempts.
- `fallback.HedgeCond(ctx, delay, fn, shouldHedge)` only hedges when the
  predicate allows it (e.g. skip hedging for non-idempotent writes).

To combine hedging with the breaker/retry pipeline, put the hedge *inside* the
`Execute` closure so each pipeline attempt is itself hedged:

```go
err := p.Execute(ctx, func(ctx context.Context) error {
	return fallback.Hedge(ctx, 50*time.Millisecond, callDownstream).Err
})
```

## See also

- [Adaptive concurrency + load shedding](adaptive-concurrency-loadshed.md)
- [Migrating from sony/gobreaker](../migration.md#from-sonygobreaker)
- Runnable example: `examples/pipeline/main.go`
</content>
