# Talk outline — Resilience patterns in Go, deterministically tested

A ~35-minute conference talk (plus ~10 min Q&A) drawn from the design of this
library. Every section maps to real code and an
[ADR](../adr/README.md), so the talk doubles as a guided tour of the repo.

**Audience:** intermediate Go engineers building services that call other
services. **Takeaway:** how to compose resilience primitives correctly, make them
distributed without losing atomicity, and — the throughline — test time-dependent
code deterministically instead of with `time.Sleep`.

---

## 0. Hook (2 min)

- The failure everyone has lived: a slow dependency, unbounded retries, a
  retry-storm that turns a brownout into an outage.
- Thesis: the individual patterns (rate limit, breaker, retry, timeout, bulkhead,
  fallback) are easy; **composing them in the right order and testing them
  deterministically** is the hard part.

## 1. The primitives, briefly (5 min)

- Rate limiting: eight algorithms behind one `ratelimit.Limiter` interface.
  Token bucket vs GCRA in one slide (see the companion post,
  [GCRA vs token bucket](gcra-vs-token-bucket.md)).
- Circuit breaker: count- vs time-based windows, half-open probing.
- Retry with backoff, timeout, bulkhead, fallback/hedging.
- The point: uniform interfaces let you swap algorithms without touching call
  sites.

## 2. Order is a correctness property, not a preference (8 min)

- Naive composition wraps these in whatever order you type them. That's a bug.
- The `pipeline` package enforces a **fixed canonical order** and `Build()` sorts
  stages into it regardless of builder call order
  (`pipeline/pipeline.go`): load shed → rate limit → bulkhead → timeout → circuit
  breaker → retry → operation.
- Walk the "why" for each edge:
  - Load-shed first — it's admission control; don't spend accounting on a request
    you'll drop.
  - Rate limit before bulkhead — don't burn a concurrency slot on a request you'll
    deny.
  - Bulkhead before timeout — don't start the clock while queuing for a slot.
  - Timeout before breaker — the breaker should see real failures, not queue-drain
    timeouts.
  - Breaker before retry — don't retry into an open circuit.
  - Retry innermost — retry the call, not the whole pipeline.
- Demo: the `resilience.Builder` facade printing `Layers()` after building from
  layers added in scrambled order.

## 3. Distributed without losing atomicity (8 min)

- The core problem: two instances both read "1 token left" and both admit.
- The solution: a narrow `store.Store` interface, with each algorithm's atomicity
  expressed as a named **Lua script** run in a single round-trip — no
  `WATCH`/`MULTI`/`EXEC` retry loops
  ([ADR-0003](../adr/0003-store-interface-and-lua-for-distributed.md)).
- Clock skew across a fleet corrupts a shared decision → optional **server-`TIME`
  mode**: the script reads the Redis server clock so every instance is pinned to
  one authoritative time.
- Fail-open philosophy ([ADR-0004](../adr/0004-fail-open-resilience-philosophy.md)):
  on a Redis outage, degrade to local enforcement rather than converting a
  dependency outage into a full outage — with the honest cost (per-instance
  divergence, effective limit × N instances) stated out loud.
- The float64/256-ns precision footnote as a war story — carefully documented and
  bounded, not hidden.

## 4. The throughline: deterministic time (8 min)

- Every algorithm here is time-dependent: refills, TATs, window expiry, breaker
  open→half-open, backoff, deadlines.
- If they called `time.Now()` / `time.Sleep()` directly, tests would need
  wall-clock sleeps — slow, flaky, and unable to hit exact boundaries.
- The decision ([ADR-0002](../adr/0002-clock-interface-for-determinism.md)):
  everything takes a `clock.Clock`. `RealClock` in production; `ManualClock` in
  tests, whose time advances only on `Advance(d)`, firing all pending timers and
  tickers in order.
- Live demo: a test that advances the clock to the exact window boundary and
  asserts, instantly and reproducibly — no `time.Sleep`.
- Note the care that makes the fake clock trustworthy: consistent lock ordering to
  avoid lock-order-inversion deadlocks, generously buffered ticker channels that
  match real `time.Ticker` coalescing semantics.

## 5. Keeping it honest (3 min)

- Zero-dependency core, **enforced in CI**
  ([ADR-0001](../adr/0001-zero-dependency-core.md)) — see the companion post,
  [Building a zero-dependency Go resilience library](zero-dependency-resilience.md).
- Fuzzing, a chaos harness, goroutine-leak detection, `-race`, real-Redis
  integration tests, and an in-memory store that emulates the Lua scripts to the
  bit.

## 6. Close & Q&A (1 min + 10)

- One line: **compose in a fixed order, keep atomicity in one Lua round-trip, and
  inject the clock so time is a value you control — then every edge case is a
  unit test.**
- Pointers: the repo's ADRs, the [examples](../examples.md), and the
  [cookbook](../cookbook/index.md).

---

### Speaker notes / possible cuts

- If short on time, cut §1 to a single slide and fold the token-bucket/GCRA
  contrast into §3.
- The deterministic-clock demo (§4) is the highest-value live segment — protect
  its time budget.
