# Migration guide

Moving to this library from the two most common Go resilience packages:

- **[From `golang.org/x/time/rate`](#from-golangorgxtimerate)** — rate limiting
- **[From `sony/gobreaker`](#from-sonygobreaker)** — circuit breaking

All snippets use the real public API. The module path is
`github.com/sanskarpan/Rate-Limiter-Circuit-Breaker`.

---

## From `golang.org/x/time/rate`

### The key conceptual difference: keyed limiters

`x/time/rate` gives you **one** `*rate.Limiter` that governs a single stream of
events. To rate-limit per user/IP/tenant you build and manage a `map[key]*rate.Limiter`
yourself (plus eviction).

This library's `ratelimit.Limiter` is **keyed**: a single limiter instance holds
per-key buckets internally, and you pass the key on every call. Idle keys are
evicted automatically (see `tokenbucket.WithIdleCleanup`).

```go
// x/time/rate: one limiter per stream, you manage the map
lim := rate.NewLimiter(rate.Limit(10), 20) // 10 events/s, burst 20
lim.Allow()                                 // no key argument

// this library: one limiter, keyed per call
lim := tokenbucket.New(20, 10)              // capacity 20, refill 10/s
defer lim.Close()
lim.Allow(ctx, "user:123").Allowed          // key is an argument
```

> **Argument order:** `rate.NewLimiter(rate.Limit(r), b)` takes **rate first,
> burst (capacity) second**. `tokenbucket.New(capacity, refillRate)` takes
> **capacity first, refill rate second** — the reverse. Read the mapping table
> carefully.

### API mapping

| `golang.org/x/time/rate` | This library (`tokenbucket`) | Notes |
| --- | --- | --- |
| `rate.NewLimiter(rate.Limit(r), b)` | `tokenbucket.New(b, r)` | `New(capacity, refillRate)` — **capacity first**. `capacity` is the burst `b`; `refillRate` is the steady-state rate `r` (tokens/sec). |
| `lim.Allow()` | `lim.Allow(ctx, key).Allowed` | `Allow` returns a `ratelimit.Result`; read `.Allowed`. |
| `lim.AllowN(time.Now(), n)` | `lim.AllowN(ctx, key, n).Allowed` | Consumes `n` tokens all-or-nothing. |
| `lim.Wait(ctx)` | `lim.Wait(ctx, key)` | Blocks until a token is free or `ctx` is done; returns `error`. |
| `lim.WaitN(ctx, n)` | `lim.WaitN(ctx, key, n)` | As above for `n` tokens. |
| `r := lim.Reserve(); r.Delay()` | `res := lim.Allow(ctx, key); res.RetryAfter` | No `Reservation` object. `Result.RetryAfter` is the wait until this key is allowed again; `Result.ResetAfter` is time to a full bucket. There is no cancel-and-refund. |
| `lim.Limit()` / `lim.Burst()` | `lim.Peek(ctx, key)` → `State{Limit, Remaining, ...}` | `Peek` reports state without consuming. |
| `lim.SetLimit(newR)` / `lim.SetBurst(newB)` | `lim.SetLimit(capacity, refillRate)` | Single call updates both; existing per-key buckets are preserved (clamped down on shrink). |
| fractional cost | `lim.AllowCost(ctx, key, cost float64)` | Consumes a non-integer cost all-or-nothing. |

`ctx` is a `context.Context` and `key` is a `string`. `Allow`/`AllowN` are
non-blocking; `Wait`/`WaitN` block.

### Before / after

**Global limiter (single stream):**

```go
// BEFORE — x/time/rate
lim := rate.NewLimiter(rate.Limit(100), 200) // 100 req/s, burst 200
if !lim.Allow() {
    http.Error(w, "rate limited", http.StatusTooManyRequests)
    return
}
```

```go
// AFTER — this library (single fixed key stands in for "global")
lim := tokenbucket.New(200, 100) // capacity 200, refill 100/s
defer lim.Close()
if !lim.Allow(ctx, "global").Allowed {
    http.Error(w, "rate limited", http.StatusTooManyRequests)
    return
}
```

**Per-user limiter (the case that needs a map with `x/time/rate`):**

```go
// BEFORE — x/time/rate: hand-rolled per-key map + mutex + eviction
type limiterSet struct {
    mu sync.Mutex
    m  map[string]*rate.Limiter
}
func (s *limiterSet) get(user string) *rate.Limiter {
    s.mu.Lock()
    defer s.mu.Unlock()
    l, ok := s.m[user]
    if !ok {
        l = rate.NewLimiter(rate.Limit(10), 20)
        s.m[user] = l // NOTE: never evicted — grows forever
    }
    return l
}
// ... s.get(userID).Allow()
```

```go
// AFTER — this library: keying is built in, idle keys auto-evicted
lim := tokenbucket.New(20, 10, tokenbucket.WithIdleCleanup(5*time.Minute))
defer lim.Close()

if !lim.Allow(ctx, "user:"+userID).Allowed {
    http.Error(w, "rate limited", http.StatusTooManyRequests)
    return
}
```

**Blocking wait:**

```go
// BEFORE
if err := lim.Wait(ctx); err != nil { return err }

// AFTER
if err := lim.Wait(ctx, "user:"+userID); err != nil { return err }
```

### Notes on parity

- **No `Reserve()`/`Reservation`.** `x/time/rate`'s reservation model (reserve
  now, act later, optionally cancel and refund) has no direct equivalent. Use
  `Wait`/`WaitN` to block, or read `Result.RetryAfter` from an `Allow` call to
  schedule your own retry. There is no token refund.
- **Other algorithms share the interface.** If you want smooth pacing rather
  than bursty token-bucket behavior, swap `tokenbucket.New` for
  `gcra.New(limit, burst, window)` — the `ratelimit.Limiter` interface is
  identical, so call sites don't change.
- **Distributed.** For a fleet-wide limit backed by Redis, swap the constructor
  for `tokenbucket.NewDistributed(rate, capacity, store, prefix)` (note: this
  distributed constructor takes **rate first, capacity second** — the opposite
  of the in-memory `New`). See
  [the distributed recipe](cookbook/distributed-redis-failopen.md).

---

## From `sony/gobreaker`

### API mapping

`gobreaker`'s `Execute` takes a `func() (any, error)` and returns
`(any, error)`. This library's `Execute` takes a `func(context.Context) error`
and returns `error` — the value is carried out through a closure variable (see
the example below), which keeps the breaker allocation-free and generics-free.

| `sony/gobreaker` | This library (`circuitbreaker`) | Notes |
| --- | --- | --- |
| `gobreaker.NewCircuitBreaker(gobreaker.Settings{...})` | `circuitbreaker.New(circuitbreaker.Config{...})` | Returns `*CircuitBreaker`. |
| `cb.Execute(func() (any, error) {...})` | `cb.Execute(ctx, func(ctx) error {...})` | Context-aware; result via a captured variable. |
| `Settings.Name` | `Config.Name` | Same. |
| `Settings.MaxRequests` | `Config.HalfOpenMaxRequests` | Max concurrent probe requests in half-open (default 1). |
| `Settings.Interval` (closed-state clearing window) | `Config.WindowDuration` (with `WindowType: TimeBased`) | Time-based failure window. Count-based (`WindowType: CountBased`, the default) uses `Config.WindowSize` instead. |
| `Settings.Timeout` (open → half-open) | `Config.OpenTimeout` | How long the circuit stays open (default 30s). |
| `Settings.ReadyToTrip(counts)` returning a bool | `Config.FailureThreshold` (count-based) and/or `Config.FailureRateThreshold` + `Config.MinimumRequests` (time-based) | Declarative thresholds instead of a callback. See mapping below. |
| `Settings.OnStateChange(name, from, to)` | `Config.OnStateChange(name string, from, to State)` | Same shape. `State` is `StateClosed` / `StateHalfOpen` / `StateOpen`. |
| `Settings.IsSuccessful(err) bool` | `Config.IsFailure(err) bool` | **Inverted polarity:** return `true` if the error *counts as a failure*. `nil` means all non-nil errors are failures. |
| (no equivalent) | `Config.SuccessThreshold` | Consecutive half-open successes needed to close (default 1). |
| (no equivalent) | `Config.RequestTimeout` | Per-`Execute` timeout; a timeout counts as a failure. |
| `cb.State()` → `gobreaker.State` | `cb.State()` → `circuitbreaker.State` | `StateClosed` / `StateHalfOpen` / `StateOpen`. |
| `cb.Counts()` | `cb.Snapshot()` → `Snapshot{Failures, Successes, Requests, FailureRate, ...}` | Point-in-time view. |
| `gobreaker.ErrOpenState` | `circuitbreaker.ErrCircuitOpen` | `errors.Is`-comparable; wrapped in a `*CircuitError`. |
| `gobreaker.ErrTooManyRequests` | `circuitbreaker.ErrTooManyRequests` | Half-open probe limit hit. |

### Mapping `ReadyToTrip`

`gobreaker` trips via a callback over `Counts`. This library uses declarative
thresholds. Two common patterns:

```go
// gobreaker: trip after 5 consecutive failures
ReadyToTrip: func(c gobreaker.Counts) bool {
    return c.ConsecutiveFailures >= 5
}
```

```go
// this library — count-based window (the default WindowType):
circuitbreaker.Config{
    WindowType:       circuitbreaker.CountBased,
    WindowSize:       10, // ring buffer of the last N requests
    FailureThreshold: 5,  // open when failures in the window reach 5
}
```

```go
// gobreaker: trip when >=60% of >=20 requests fail
ReadyToTrip: func(c gobreaker.Counts) bool {
    return c.Requests >= 20 && float64(c.TotalFailures)/float64(c.Requests) >= 0.6
}
```

```go
// this library — time-based window:
circuitbreaker.Config{
    WindowType:           circuitbreaker.TimeBased,
    WindowDuration:       60 * time.Second,
    BucketDuration:       1 * time.Second,
    MinimumRequests:      20,  // don't evaluate the rate below this volume
    FailureRateThreshold: 0.6, // open at 60% failures
    FailureThreshold:     1,   // also require at least 1 raw failure
}
```

### Before / after

```go
// BEFORE — sony/gobreaker
cb := gobreaker.NewCircuitBreaker(gobreaker.Settings{
    Name:        "payments",
    MaxRequests: 1,
    Interval:    0,
    Timeout:     30 * time.Second,
    ReadyToTrip: func(c gobreaker.Counts) bool {
        return c.ConsecutiveFailures >= 5
    },
    OnStateChange: func(name string, from, to gobreaker.State) {
        log.Printf("%s: %s -> %s", name, from, to)
    },
})

body, err := cb.Execute(func() (any, error) {
    return callPayments()
})
if err != nil {
    if errors.Is(err, gobreaker.ErrOpenState) {
        // fast-fail
    }
    return err
}
resp := body.(*Response)
```

```go
// AFTER — this library
cb := circuitbreaker.New(circuitbreaker.Config{
    Name:                "payments",
    WindowType:          circuitbreaker.CountBased,
    WindowSize:          10,
    FailureThreshold:    5,
    HalfOpenMaxRequests: 1,
    OpenTimeout:         30 * time.Second,
    OnStateChange: func(name string, from, to circuitbreaker.State) {
        log.Printf("%s: %s -> %s", name, from, to)
    },
})

// The result is carried out through a captured variable because Execute's fn
// returns only an error.
var resp *Response
err := cb.Execute(ctx, func(ctx context.Context) error {
    var callErr error
    resp, callErr = callPayments(ctx)
    return callErr
})
if err != nil {
    if errors.Is(err, circuitbreaker.ErrCircuitOpen) {
        // fast-fail
    }
    return err
}
// use resp
```

### Notes on parity

- **`IsSuccessful` is inverted.** `gobreaker.Settings.IsSuccessful` returns
  `true` for successes; `Config.IsFailure` returns `true` for **failures**. By
  default all non-nil errors are failures, and caller `context.Canceled` is
  *not* counted (unlike a `RequestTimeout`-induced deadline, which is).
- **Built-in fallback.** `cb.ExecuteWithFallback(ctx, fn, fallback)` runs
  `fallback(ctx, err)` when `fn` fails or the circuit is open — no wrapper
  needed. For richer composition (breaker + retry + hedge) use the
  [pipeline](cookbook/flaky-downstream-cb-retry-hedge.md).
- **Registry.** `circuitbreaker.NewRegistry()` manages many named breakers if
  you were keeping a `map[string]*gobreaker.CircuitBreaker`.
- **Observability.** Wire a `metric.Recorder` via `Config.WithRecorder(rec)` to
  export Prometheus state/transition/latency series. See the
  [observability recipe](cookbook/observability-prometheus-otel.md).

---

## See also

- [Recipe cookbook](cookbook/index.md) — copy-pasteable scenario recipes.
- [docs/algorithms.md](algorithms.md) — algorithm theory and trade-offs.
- [docs/distributed.md](distributed.md) — Redis-backed limiters and fallback modes.
</content>
</invoke>
