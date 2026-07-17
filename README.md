# Resilience

[![Go Version](https://img.shields.io/badge/go-1.24-blue.svg)](https://golang.org)
[![Go Report Card](https://goreportcard.com/badge/github.com/sanskarpan/resilience)](https://goreportcard.com/report/github.com/sanskarpan/resilience)
[![pkg.go.dev](https://pkg.go.dev/badge/github.com/sanskarpan/resilience.svg)](https://pkg.go.dev/github.com/sanskarpan/resilience)

A production-grade Go library for rate limiting, circuit breaking, and resilience patterns — with **zero external runtime dependencies** in the core library.

---

## Features

| Package | Description |
|---------|-------------|
| `ratelimit/tokenbucket` | Classic token bucket — smooth rate with configurable burst |
| `ratelimit/gcra` | Generic Cell Rate Algorithm — precise, low-allocation |
| `ratelimit/slidingwindow` | Sliding window (log and counter variants) |
| `ratelimit/fixedwindow` | Fixed-window counter — minimal memory footprint |
| `ratelimit/leakybucket` | Leaky bucket — queue-based request shaping |
| `ratelimit/adaptive` | Adaptive limiter — adjusts rate based on error rate |
| `ratelimit/composite` | Composite — apply multiple limiters in sequence or parallel |
| `circuitbreaker` | Count- and time-based circuit breaker with half-open probe |
| `bulkhead` | Semaphore-based concurrency limiter + thread pool |
| `retry` | Retry with fixed, exponential, jitter, and linear backoff |
| `timeout` | Context-based timeout wrapper |
| `fallback` | Fallback, hedge, and speculative execution |
| `pipeline` | Composable policy pipeline (RateLimit → Bulkhead → Timeout → CB → Retry) |
| `ratelimit/middleware` | HTTP and gRPC middleware for rate limiting |
| `circuitbreaker/middleware` | HTTP and gRPC middleware for circuit breaking |
| `ratelimit/store` | Redis store adapter for distributed rate limiting |

---

## Quick Start

```go
import (
    "net/http"
    "github.com/sanskarpan/resilience/ratelimit/tokenbucket"
    ratelimitmw "github.com/sanskarpan/resilience/ratelimit/middleware"
)

limiter := tokenbucket.New(100, 20) // 100 req/s, burst of 20
http.Handle("/api/", ratelimitmw.RateLimit(limiter)(myHandler))
http.ListenAndServe(":8080", nil)
```

---

## Algorithm Comparison

| Algorithm | Burst | Exact | Memory | Distributed | Use When |
|-----------|-------|-------|--------|-------------|----------|
| Token Bucket | ✅ | ✅ | O(keys) | ✅ | API rate limiting, smooth traffic |
| GCRA | ✅ | ✅ | O(keys) | ✅ | High-performance APIs, low allocs |
| Sliding Window Log | ❌ | ✅ | O(req) | ✅ | Exact counting required |
| Sliding Window Counter | ❌ | ~✅ | O(keys) | ✅ | Low memory, near-exact |
| Fixed Window | ❌ | ✅ | O(keys) | ✅ | Simple, fastest |
| Leaky Bucket | ❌ | ✅ | O(keys) | ❌ | Queue-based smoothing |
| Adaptive | ✅ | ~✅ | O(keys) | ❌ | Dynamic load shedding |

---

## Performance Benchmarks

All benchmarks run on Apple M-series (11 cores), Go 1.24, `GOMAXPROCS=11`.

| Algorithm | Single Key (ns/op) | Parallel (ns/op) | Allocs/op |
|-----------|--------------------|------------------|-----------|
| Token Bucket | 62 | 167 | 0 |
| GCRA | 67 | 195 | 0 |
| Fixed Window | 69 | 188 | 0 |
| Sliding Window Counter | 75 | 203 | 0 |
| Sliding Window Log | 93 | — | 0 |
| Circuit Breaker (closed) | 82 | 169 | 0 |
| Circuit Breaker (open) | 45 | — | 1 |

---

## API Documentation

### Rate Limiting

#### Token Bucket

```go
import "github.com/sanskarpan/resilience/ratelimit/tokenbucket"

// New creates a local token bucket limiter.
// rate: tokens added per second
// capacity: maximum burst size
limiter := tokenbucket.New(100, 20)
defer limiter.Close()

result := limiter.Allow(ctx, "user:123")
if !result.Allowed {
    // result.RetryAfter contains how long to wait
    http.Error(w, "rate limited", http.StatusTooManyRequests)
    return
}

// Consume N tokens at once
result = limiter.AllowN(ctx, "user:123", 5)

// Non-blocking peek (does not consume tokens)
state := limiter.Peek(ctx, "user:123")

// Block until a token is available (respects context cancellation)
if err := limiter.Wait(ctx, "user:123"); err != nil {
    // ctx cancelled or deadline exceeded
}

// Reset a key (for testing or admin operations)
limiter.Reset(ctx, "user:123")
```

#### GCRA

```go
import "github.com/sanskarpan/resilience/ratelimit/gcra"

// New creates a GCRA limiter.
// rate: max requests per second
// burstSeconds: burst capacity in seconds (EmissionInterval * burst)
limiter := gcra.New(gcra.Options{Rate: 100, BurstSeconds: 0.2})
```

#### Composite Limiter

```go
import (
    "github.com/sanskarpan/resilience/ratelimit/composite"
    "github.com/sanskarpan/resilience/ratelimit/tokenbucket"
    "github.com/sanskarpan/resilience/ratelimit/fixedwindow"
)

// Both must allow for the request to proceed
combined := composite.NewAND(
    tokenbucket.New(100, 20),   // 100 req/s per key
    fixedwindow.New(1000, time.Hour), // 1000 req/hour global
)
```

---

### Circuit Breaker

```go
import "github.com/sanskarpan/resilience/circuitbreaker"

cb := circuitbreaker.New(circuitbreaker.Config{
    Name:             "my-service",
    WindowType:       circuitbreaker.CountBased, // or TimeBased
    WindowSize:       100,                       // last 100 calls
    FailureThreshold: 50,                        // open at 50% failure rate
    OpenTimeout:      30 * time.Second,
})

err := cb.Execute(ctx, func(ctx context.Context) error {
    return callDownstreamService(ctx)
})

if errors.Is(err, circuitbreaker.ErrCircuitOpen) {
    // Circuit is open — use fallback or return error to caller
}

// Get current state and metrics
snap := cb.Snapshot()
fmt.Printf("state=%s failures=%d failure_rate=%.2f\n",
    snap.State, snap.Failures, snap.FailureRate)
```

---

### Pipeline

```go
import (
    "github.com/sanskarpan/resilience/pipeline"
    "github.com/sanskarpan/resilience/retry/backoff"
)

p := pipeline.New().
    RateLimit(limiter, pipeline.KeyByValue("api")).
    Bulkhead(10, 100*time.Millisecond). // max 10 concurrent, 100ms wait
    Timeout(2 * time.Second).
    CircuitBreaker(cb).
    Retry(&retry.Policy{
        MaxAttempts: 3,
        Backoff:     backoff.Exponential(100*time.Millisecond, 2.0),
    }).
    Build()

err := p.Execute(ctx, func(ctx context.Context) error {
    return callBackend(ctx)
})
```

---

### HTTP Middleware

```go
import ratelimitmw "github.com/sanskarpan/resilience/ratelimit/middleware"

// Rate limit by IP address
http.Handle("/api/", ratelimitmw.RateLimit(
    limiter,
    ratelimitmw.WithKeyFunc(ratelimitmw.KeyByIP()),
)(myHandler))

// Rate limit by header value
http.Handle("/api/", ratelimitmw.RateLimit(
    limiter,
    ratelimitmw.WithKeyFunc(ratelimitmw.KeyByHeader("X-User-ID")),
    ratelimitmw.WithOnLimited(func(w http.ResponseWriter, r *http.Request, result ratelimit.Result) {
        http.Error(w, "slow down!", http.StatusTooManyRequests)
    }),
)(myHandler))
```

Response headers set automatically:
- `X-RateLimit-Limit: 100`
- `X-RateLimit-Remaining: 42`
- `X-RateLimit-Reset: 1735689600`
- `Retry-After: 1` (on 429)

---

### gRPC Interceptors

```go
import (
    ratelimitmw "github.com/sanskarpan/resilience/ratelimit/middleware"
    cbmw "github.com/sanskarpan/resilience/circuitbreaker/middleware"
)

srv := grpc.NewServer(
    grpc.ChainUnaryInterceptor(
        ratelimitmw.UnaryServerInterceptor(
            limiter,
            ratelimitmw.GRPCWithKeyFunc(ratelimitmw.GRPCKeyByMetadata("x-user-id")),
        ),
        cbmw.CBUnaryServerInterceptor(cb),
    ),
)
```

---

### Distributed Rate Limiting (Redis)

```go
import (
    "github.com/sanskarpan/resilience/ratelimit/store"
    "github.com/sanskarpan/resilience/ratelimit/tokenbucket"
)

s := store.NewRedis(store.RedisOptions{
    Addr:      "redis:6379",
    KeyPrefix: "myapp:rl:",
})
defer s.Close()

// Drop-in replacement for the local limiter — same API
limiter := tokenbucket.NewDistributed(100, 20, s, "api")
result := limiter.Allow(ctx, "user:123")
```

All distributed algorithms use atomic Lua scripts — **no WATCH/MULTI/EXEC overhead**.

---

## Running the Demo

```bash
# Full stack: demo server + Redis + Prometheus + Grafana
docker-compose up

# Demo server only
go run ./server/

# Run tests
make test-race

# Run benchmarks
make bench
```

- Demo server: http://localhost:8080
- Frontend: http://localhost:3000
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3001 (admin/admin)

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and contribution guidelines.

## License

MIT — see [LICENSE](LICENSE).
