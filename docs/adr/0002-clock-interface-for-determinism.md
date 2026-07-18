# ADR-0002: Clock interface for deterministic, testable time

## Status

Accepted.

## Context

Every algorithm in this library is time-dependent: token buckets refill over
time, GCRA computes a theoretical arrival time, sliding windows expire old
requests, circuit breakers open for a timeout then probe, retries back off, and
timeouts fire on deadlines.

If these components called `time.Now()`, `time.Sleep()`, and `time.NewTicker()`
directly, tests would have to use **real wall-clock sleeps** to exercise
time-based behaviour. That makes tests slow, flaky (a `time.Sleep(10*time.Millisecond)`
races against the scheduler), and unable to deterministically reproduce edge
cases like "exactly at the window boundary" or "a single advance that spans many
ticker intervals."

## Decision

**All time-dependent components take a `clock.Clock` interface** rather than
calling the `time` package directly. The interface lives in `internal/clock`
(`internal/clock/clock.go`) and abstracts `Now`, `Sleep`, `Since`, `Until`,
`NewTimer`, `NewTicker`, and `AfterFunc`.

Two implementations exist:

- **`clock.RealClock`** — a thin, allocation-free pass-through to the stdlib
  `time` package. This is the production default.
- **`clock.ManualClock`** — a test double whose time advances *only* when
  `Advance(d)` is called. It deterministically fires all pending timers and
  tickers in order, unblocking any goroutine parked on `Sleep` or a timer
  channel.

The package doc comment states this outright: it is "the most important
architectural decision in this library — it makes every algorithm fully testable
without `time.Sleep`."

The `ManualClock` implementation is deliberately careful about concurrency: it
uses a consistent lock order between `Advance` and `Reset` to avoid a
lock-order-inversion deadlock, and its ticker channel is generously buffered
(`tickerChanBuffer = 1024`) so a single `Advance` spanning many intervals can
deposit one tick per interval, coalescing (dropping) only once a consumer falls
that far behind — matching real `time.Ticker` semantics.

`internal/clock` is an **internal** package: it is a foundational abstraction, not
part of the public API, and carries no external compatibility guarantee (see
[docs/STABILITY.md](../STABILITY.md)).

## Consequences

**Positive:**

- Time-based tests are **deterministic and instant**: advance the clock to the
  exact instant of interest and assert, with no wall-clock sleeps and no
  scheduler races.
- Boundary conditions (window edges, breaker open→half-open transitions, backoff
  schedules) are directly and reproducibly testable.
- Production overhead is negligible — `RealClock` is a zero-state pass-through to
  the stdlib.

**Negative / trade-offs:**

- Every time-dependent constructor must thread a `Clock` (typically via a
  `WithClock` functional option), a small amount of extra plumbing.
- `ManualClock` is itself concurrent code that must be maintained carefully; its
  history includes fixes for lock-order inversion and ticker coalescing, which is
  the price of a faithful test double.
- The distributed (Redis) path does **not** use the injected clock as the
  authoritative time source — server-time mode reads the Redis server clock
  in-script instead (see
  [ADR-0003](0003-store-interface-and-lua-for-distributed.md)); the injected
  clock only supplies the client-side `now` and cosmetic snapshot fields there.
