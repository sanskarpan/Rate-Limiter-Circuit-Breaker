# Resilience

A production-grade Go toolkit for rate limiting, circuit breaking, and resilience patterns — with **zero external runtime dependencies** in the core library.

[![CI](https://github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/actions/workflows/ci.yml/badge.svg)](https://github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/sanskarpan/Rate-Limiter-Circuit-Breaker)](https://goreportcard.com/report/github.com/sanskarpan/Rate-Limiter-Circuit-Breaker)
[![Go Reference](https://pkg.go.dev/badge/github.com/sanskarpan/Rate-Limiter-Circuit-Breaker.svg)](https://pkg.go.dev/github.com/sanskarpan/Rate-Limiter-Circuit-Breaker)
<!-- Coverage populates after the first CI run uploads a report to Codecov. -->
[![codecov](https://codecov.io/gh/sanskarpan/Rate-Limiter-Circuit-Breaker/branch/main/graph/badge.svg)](https://codecov.io/gh/sanskarpan/Rate-Limiter-Circuit-Breaker)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

---

## Why this exists

Most Go services eventually need to protect themselves and their dependencies:
throttle abusive clients, stop hammering a failing downstream, cap concurrency,
retry transient errors, and fall back gracefully. These patterns are usually
copy-pasted, subtly wrong, or pulled in as heavyweight frameworks.

This library provides each pattern as a small, correct, well-tested building
block behind a single consistent interface — and lets you compose them into a
resilience pipeline. The **core packages have zero external runtime
dependencies**; Redis (for distributed rate limiting) and gRPC (for
interceptors) are only needed if you use those optional adapters. Every rate
limiter shares the same `ratelimit.Limiter` interface, so switching algorithms
or going from single-instance to distributed is a one-line change.

---

## How this compares to other Go libraries

Most Go resilience tooling is **single-purpose**: `x/time/rate` and
`uber-go/ratelimit` do one rate-limiting algorithm each; `sony/gobreaker` and
`mercari/go-circuitbreaker` do circuit breaking. This library instead ships a
*suite* of rate-limiting algorithms **plus** circuit breaker, bulkhead, retry
(with a retry budget), timeout, and fallback/hedge — all composable through one
pipeline, with an optional Redis backend for distributed limiting.

**Choose this library if** you want more than one rate-limiting algorithm behind
a single interface, need distributed limiting *and* circuit breaking *and* a
composable pipeline from one dependency, and value a zero-dependency core.
**Choose a single-purpose library if** you only need exactly one primitive and
want the smallest possible import (e.g. just `x/time/rate` for a token bucket, or
just `gobreaker` for a breaker).

| Capability | **This library** | `x/time/rate` | `uber-go/ratelimit` | `sony/gobreaker` | `mercari/go-circuitbreaker` | `ulule/limiter` | `didip/tollbooth` | resilience4j *(JVM ref)* |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| Token bucket | ✅ | ✅ | — | — | — | — | ✅¹ | ✅ |
| GCRA | ✅ | — | — | — | — | — | — | — |
| Sliding window (log + counter) | ✅ | — | — | — | — | ✅² | — | — |
| Leaky bucket | ✅ | — | ✅ | — | — | — | — | — |
| Fixed window | ✅ | — | — | — | — | ✅ | — | — |
| Adaptive limiter | ✅ | — | — | — | — | — | — | — |
| Concurrency limiter | ✅ | — | — | — | — | — | — | ✅ (Bulkhead) |
| Load shedder | ✅ | — | — | — | — | — | — | — |
| Circuit breaker | ✅ | — | — | ✅ | ✅ | — | — | ✅ |
| Bulkhead | ✅ | — | — | — | — | — | — | ✅ |
| Retry + retry budget | ✅ | — | — | — | — | — | — | ✅³ |
| Timeout / fallback / hedge | ✅ | — | — | — | — | — | — | ✅⁴ |
| Composable pipeline | ✅ | — | — | — | — | — | — | ✅ (decorators) |
| Distributed (Redis) | ✅⁵ | — | — | — | — | ✅ | — | — |
| Framework middleware | ✅⁶ | — | — | — | — | ✅ | ✅ (net/http) | ✅ (Spring) |
| Metrics (Prometheus / OTel) | ✅⁷ | — | — | — | — | — | — | ✅ (Micrometer) |
| Zero-dependency core | ✅ | ✅⁸ | ✅ | ✅ | — | — | — | n/a |
| Language | Go | Go | Go | Go | Go | Go | Go | JVM |

Legend: **✅** = supported · **—** = not provided / not applicable. Competitor
entries reflect each project's documented capabilities; where a feature isn't a
stated capability we use "—" rather than assert its absence.

Footnotes:

1. `didip/tollbooth` builds its limiter on `golang.org/x/time/rate` (token bucket).
2. `ulule/limiter` implements a fixed-window counter and a sliding-window
   approximation; it does not ship a token bucket or GCRA.
3. resilience4j retry supports max-attempts/backoff; a dedicated *retry budget*
   (token-bucket guard against retry storms) is provided here as `retry.Budget`.
4. resilience4j offers `TimeLimiter` and fallback via decorators; N-copy hedging
   is provided here in the `fallback` package.
5. Distributed backends via atomic Redis Lua scripts for token bucket, GCRA,
   fixed window, and both sliding-window variants (`ratelimit/store/redis.go`,
   `ratelimit/*/distributed*.go`). Leaky bucket and adaptive are node-local.
6. Rate-limit and circuit-breaker middleware for stdlib `net/http` and gRPC in
   the core repo, plus chi/gin/echo/fiber/connect adapters in the separate
   `contrib/` module (kept out of the core to preserve zero deps).
7. Metrics via a pluggable `metric.Recorder` with Prometheus (`metric/prometheus`)
   and OpenTelemetry (`observability/otel`) adapters; the core stays no-op and
   dependency-free by default.
8. `golang.org/x/time/rate` lives in the `golang.org/x` extended standard library
   (a Go-team-maintained module, not the compiled-in stdlib).

Per-algorithm trade-offs (not library positioning) are in
[docs/comparison.md](docs/comparison.md); migration guides from `x/time/rate` and
`gobreaker` are in [docs/migration.md](docs/migration.md); published
microbenchmarks are in [docs/benchmarks.md](docs/benchmarks.md).

---

## Features

| Package | What it does | Key property |
|---------|--------------|--------------|
| `ratelimit` | Core `Limiter` interface (`Allow`/`AllowN`/`Wait`/`Peek`/`Reset`) | Uniform API across all algorithms |
| `ratelimit/tokenbucket` | Token bucket — smooth rate with configurable burst | O(1), 0 allocs, lazy refill |
| `ratelimit/gcra` | Generic Cell Rate Algorithm | One timestamp per key, exact, Redis-optimal |
| `ratelimit/slidingwindow` | Sliding window (log + counter variants) | Log = exact; counter = O(keys) approximate |
| `ratelimit/fixedwindow` | Fixed-window counter | Simplest, lowest memory |
| `ratelimit/leakybucket` | Leaky bucket — constant-rate output shaping | Strictly constant drain rate |
| `ratelimit/adaptive` | Adaptive limiter — retunes on latency/error signals | Dynamic load shedding |
| `ratelimit/composite` | Combine limiters with AND / OR logic | Layer burst + sustained limits |
| `ratelimit/store` | In-memory and Redis store adapters | Atomic Lua scripts, fail-open/closed |
| `ratelimit/middleware` | HTTP and gRPC rate-limit middleware | Standard `X-RateLimit-*` headers |
| `circuitbreaker` | Count- and time-based breaker with half-open probing | Fast-fail on failing dependencies |
| `circuitbreaker/middleware` | HTTP and gRPC circuit-breaker middleware | 503 on open, streams response |
| `retry` | Retry policy with pluggable backoff | Context-aware, `RetryIf` predicate |
| `retry/backoff` | Constant, exponential, full-jitter, decorrelated | AWS-style jitter strategies |
| `timeout` | Context-based timeout wrapper | Generic `DoWithResult` helper |
| `fallback` | Fallback, hedge, and hedged N-copy execution | Reduce tail latency |
| `bulkhead` | Semaphore-based concurrency limiter | Bounded in-flight work |
| `pipeline` | Composable policy pipeline | Fixed correct stage order |

---

## 30-second quickstart

```bash
go get github.com/sanskarpan/Rate-Limiter-Circuit-Breaker@latest
```

```go
package main

import (
	"context"
	"fmt"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
)

func main() {
	// capacity=10 (burst), refill=5 tokens/second (sustained rate).
	limiter := tokenbucket.New(10, 5)
	defer limiter.Close()

	result := limiter.Allow(context.Background(), "user:123")
	if !result.Allowed {
		fmt.Printf("rate limited, retry after %s\n", result.RetryAfter)
		return
	}
	fmt.Printf("allowed, %d tokens remaining\n", result.Remaining)
}
```

> **Constructor convention:** `tokenbucket.New(capacity, refillRate)` — the first
> argument is the burst capacity, the second is the sustained refill rate in
> tokens/second.

---

## Examples

### Token bucket

```go
import "github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"

limiter := tokenbucket.New(100, 20) // burst 100, 20 tokens/s sustained
defer limiter.Close()

// Single token, non-blocking.
result := limiter.Allow(ctx, "user:123")

// Consume N tokens atomically (all-or-nothing).
result = limiter.AllowN(ctx, "user:123", 5)

// Inspect state without consuming.
state := limiter.Peek(ctx, "user:123")

// Block until a token frees up (respects ctx cancellation).
if err := limiter.Wait(ctx, "user:123"); err != nil {
	// ctx cancelled or deadline exceeded
}

// Clear a key (admin/testing).
_ = limiter.Reset(ctx, "user:123")
```

### GCRA

```go
import (
	"time"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/gcra"
)

// limit=10 per window, burst=3, window=1s → smooth 10 req/s allowing 3 upfront.
limiter := gcra.New(10, 3, time.Second)
defer limiter.Close()

result := limiter.Allow(ctx, "api-key:xyz")
```

### Circuit breaker

```go
import (
	"errors"
	"time"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
)

cb := circuitbreaker.New(circuitbreaker.Config{
	Name:             "payments",
	WindowType:       circuitbreaker.CountBased, // or circuitbreaker.TimeBased
	WindowSize:       10,                        // track the last 10 calls
	FailureThreshold: 5,                         // open after 5 failures
	OpenTimeout:      30 * time.Second,          // stay open before half-open probe
})

err := cb.Execute(ctx, func(ctx context.Context) error {
	return callDownstream(ctx)
})
if errors.Is(err, circuitbreaker.ErrCircuitOpen) {
	// circuit open — fast-fail, use a fallback
}

snap := cb.Snapshot()
fmt.Printf("state=%s failures=%d rate=%.2f\n", snap.State, snap.Failures, snap.FailureRate)
```

### Resilience pipeline

Composes several patterns in a fixed, production-correct order. `Build()`
sorts stages into: rate limit → bulkhead → timeout → circuit breaker → retry,
regardless of the order you call the builder methods.

```go
import (
	"time"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/pipeline"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/retry"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/retry/backoff"
)

p := pipeline.New().
	RateLimit(limiter, pipeline.KeyByValue("api")).
	Bulkhead(10, 100*time.Millisecond). // max 10 concurrent, wait up to 100ms for a slot
	Timeout(2 * time.Second).
	CircuitBreaker(cb).
	Retry(&retry.Policy{
		MaxAttempts: 3,
		Backoff:     backoff.Exponential(100*time.Millisecond, 2*time.Second),
	}).
	Build()

err := p.Execute(ctx, func(ctx context.Context) error {
	return callBackend(ctx)
})
```

### Distributed rate limiting (Redis)

Distributed limiters implement the same `Limiter` interface — swapping in Redis
is a constructor change. All distributed algorithms use atomic Lua scripts (no
`WATCH`/`MULTI`/`EXEC` round-trips).

```go
import (
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/store"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
)

s := store.NewRedis(store.RedisOptions{
	Addr:      "localhost:6379",
	KeyPrefix: "myapp:rl:",
})
defer s.Close()

// NewDistributed(rate, capacity, store, prefix): 20 tokens/s, burst 20, shared globally.
limiter := tokenbucket.NewDistributed(20, 20, s, "api")
result := limiter.Allow(ctx, "user:123") // Redis key: api:tokenbucket:user:123
```

See [docs/distributed.md](docs/distributed.md) for fallback behaviour, key
naming, and Redis Cluster notes.

---

## When to use which algorithm

| Algorithm | Burst | Precision | Memory | Distributed | Use when |
|-----------|:-----:|-----------|:------:|:-----------:|----------|
| `token_bucket` | Yes | Exact | O(keys) | Yes | General API rate limiting; smooth traffic with allowed spikes |
| `gcra` | Yes | Exact | O(keys) | Yes (best) | High-throughput APIs; precise burst control; Redis-efficient |
| `sliding_window` (log) | No | Exact | O(requests) | Yes | Exact counting required; memory grows with request volume |
| `sliding_window` (counter) | No | ~99% | O(keys) | Yes | High volume, low memory; small boundary approximation acceptable |
| `fixed_window` | No | Exact-in-window | O(keys) | Yes | Simplest, cheapest; boundary burst of up to 2× limit is acceptable |
| `leaky_bucket` | No | Exact | O(keys+queue) | No | Strictly constant output rate; smoothing bursty input; adds queue latency |
| `adaptive` | Yes | Dynamic | O(keys) | No | Load shedding — retunes the limit from live latency/error signals |

Rules of thumb:

- **Need burst + smooth sustained rate?** `token_bucket` (well understood) or
  `gcra` (Redis-optimal, minimal memory).
- **Need strictly constant downstream rate (e.g. a partner SLA)?**
  `leaky_bucket` — at the cost of queuing latency.
- **Need exact counting and memory is not a concern?** `sliding_window` log
  variant.
- **High volume, tight memory, boundary burst unacceptable?**
  `sliding_window` counter variant.
- **Simplest possible, boundary burst OK?** `fixed_window`.

The identifiers above (`token_bucket`, `gcra`, `sliding_window`,
`fixed_window`, `leaky_bucket`, `adaptive`) are the canonical names used by the
demo server and its HTTP API. Full trade-off analysis lives in
[docs/comparison.md](docs/comparison.md) and per-algorithm deep dives in
[docs/algorithms.md](docs/algorithms.md).

---

## Other resilience patterns

### Retry with backoff

```go
import (
	"time"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/retry"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/retry/backoff"
)

p := &retry.Policy{
	MaxAttempts: 5, // total calls, including the first
	Backoff:     backoff.Exponential(100*time.Millisecond, 5*time.Second),
	MaxDelay:    2 * time.Second,
	RetryIf:     func(err error) bool { return errors.Is(err, ErrTransient) },
}
err := p.Do(ctx, func(ctx context.Context) error {
	return callExternalService(ctx)
})
```

Backoff strategies: `backoff.Constant`, `backoff.Exponential(base, max)`,
`backoff.FullJitter(base, max, rng)`, `backoff.Decorrelated(base, max, rng)`.

### Timeout

```go
import "github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/timeout"

err := timeout.Do(ctx, 2*time.Second, func(ctx context.Context) error {
	return slowCall(ctx)
})

// Typed variant:
val, err := timeout.DoWithResult(ctx, 2*time.Second, func(ctx context.Context) (int, error) {
	return fetch(ctx)
})
```

### Fallback and hedging

```go
import "github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/fallback"

// Fall back to a secondary path if the primary fails.
err := fallback.Do(ctx,
	func(ctx context.Context) error { return callPrimary(ctx) },
	func(ctx context.Context, primaryErr error) error { return callSecondary(ctx) },
)

// Hedge: fire a second attempt if the first hasn't finished within the delay.
res := fallback.Hedge(ctx, 50*time.Millisecond, func(ctx context.Context) error {
	return callReplica(ctx)
})
_ = res.Err // res.Primary reports whether the primary attempt won
```

### Bulkhead

```go
import "github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/bulkhead"

bh := bulkhead.New(10, 100*time.Millisecond) // max 10 concurrent, wait up to 100ms
err := bh.Execute(ctx, func(ctx context.Context) error {
	return doWork(ctx)
})
// errors.Is(err, bulkhead.ErrBulkheadFull) when the slot wait times out
```

### HTTP middleware

```go
import ratelimitmw "github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/middleware"

http.Handle("/api/", ratelimitmw.RateLimit(
	limiter,
	ratelimitmw.WithKeyFunc(ratelimitmw.KeyByIP()), // or KeyByHeader("X-User-ID")
)(myHandler))
```

Sets `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset`, and
`Retry-After` (on 429) automatically. gRPC interceptors are available via
`ratelimitmw.UnaryServerInterceptor(...)` and
`circuitbreaker/middleware.CBUnaryServerInterceptor(cb)`.

---

## Demo server and frontend

The repository ships a demo server (Go) and a Next.js frontend that visualize
each algorithm in real time. **These are demos, not part of the importable
library.**

### Server

```bash
# From the repo root — listens on :8080 by default.
go run ./server
```

Configuration via environment variables:

- `PORT` — listen port (default `8080`)
- `API_KEY` — if set, requests must present this key; if empty, the server runs
  unauthenticated (development mode)
- `CORS_ORIGINS` — comma-separated allowed origins (default
  `http://localhost:3000`)

### Frontend

```bash
cd frontend
npm install
npm run dev
```

The frontend reads `NEXT_PUBLIC_API_URL` to reach the demo server (default
`http://localhost:8080`). It runs on `http://localhost:3000`.

### Full stack (Docker)

```bash
docker-compose up    # demo server + Redis + Prometheus + Grafana
```

---

## Guides & recipes

- [docs/migration.md](docs/migration.md) — migrating from `golang.org/x/time/rate`
  (rate limiting) and `sony/gobreaker` (circuit breaking): API-mapping tables and
  before/after snippets.
- [docs/cookbook/](docs/cookbook/index.md) — copy-pasteable, API-accurate recipes:
  per-IP limiting in Gin/chi/echo/Fiber, per-tenant quotas, protecting a flaky
  downstream (breaker + retry budget + hedge), distributed Redis limits with
  fail-open, cost-weighted limiting, adaptive concurrency + load shedding, and
  Prometheus + OpenTelemetry observability.

---

## Documentation

- [docs/algorithms.md](docs/algorithms.md) — per-algorithm theory, formulas, and properties
- [docs/comparison.md](docs/comparison.md) — trade-offs and a decision guide
- [docs/distributed.md](docs/distributed.md) — Redis-backed limiters, fallback modes, cluster notes
- [docs/benchmarks.md](docs/benchmarks.md) — reproducible microbenchmarks (real ns/op, B/op, allocs/op)
- [docs/good-first-issues.md](docs/good-first-issues.md) — scoped starter tasks for new contributors
- [CONTRIBUTING.md](CONTRIBUTING.md) — development setup and contribution guidelines
- [SECURITY.md](SECURITY.md) — reporting security issues
- [CHANGELOG.md](CHANGELOG.md) — release history

---

## API stability

This project follows [Semantic Versioning](https://semver.org/). While the
module is pre-1.0, minor releases may contain breaking API changes; pin a
version in your `go.mod` for reproducible builds. Anything under `internal/` is
not part of the public API and may change at any time.

## License

MIT — see [LICENSE](LICENSE).
</content>
</invoke>
