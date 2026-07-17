# ENHANCEMENTS Roadmap

A prioritized, research-grounded roadmap for making the `github.com/sanskarpan/Rate-Limiter-Circuit-Breaker`
resilience toolkit **more complete, more competitive, and more adoptable**.

This is a *forward-looking* document. It is **not** a bug list — the codebase has
already been through several adversarial audit rounds (`AUDIT_REPORT.md`,
`ADVERSARIAL_AUDIT.md`, `ISSUES.md`) and is green on build/vet/`test -race`/lint/integration.
Every item below is grounded in what the code actually does or lacks, with file references,
and benchmarked against leading libraries: `go.uber.org/ratelimit`, `golang.org/x/time/rate`,
`sony/gobreaker`, `mercari/go-circuitbreaker`, `slok/goresilience`, resilience4j, Polly,
Netflix `concurrency-limits`, and the Stripe/Shopify GCRA writeups.

---

> ## ✅ Implementation status
>
> **All 15 top-priority items are implemented, tested, and merged**, plus a P2 batch
> (§1.4 distributed circuit breaker, §1.10 cache-aside fallback, §4.6 bulkhead saturation
> metrics, §6.1 property-based testing, §7.6 gosec/revive/depguard). Completed subsections
> are marked `✅ Implemented & merged` below. `main` CI is green across build/vet/`test -race`/
> golangci-lint(0)/verify-zero-deps/real-Redis integration/Playwright e2e/contrib.
>
> Remaining: the rest of the P2 backlog and P3 nice-to-haves.

## Table of Contents

- [Executive Summary](#executive-summary)
- [Top 15 Prioritized Enhancements](#top-15-prioritized-enhancements)
- [Legend](#legend)
- [1. Missing Algorithms & Patterns](#1-missing-algorithms--patterns)
- [2. API & Ergonomics](#2-api--ergonomics)
- [3. Performance](#3-performance)
- [4. Observability](#4-observability)
- [5. Distributed Correctness](#5-distributed-correctness)
- [6. Testing & Quality](#6-testing--quality)
- [7. Security & Hardening](#7-security--hardening)
- [8. DX & Docs](#8-dx--docs)
- [9. Operability & Release](#9-operability--release)
- [10. Frontend / Demo](#10-frontend--demo)
- [11. Community & Adoption](#11-community--adoption)
- [Appendix A: What Already Exists (Strengths)](#appendix-a-what-already-exists-strengths)
- [Appendix B: Enhancement Count by Category & Priority](#appendix-b-enhancement-count-by-category--priority)

---

## Executive Summary

This is a genuinely strong, correctness-first library. It ships **eight rate-limiting
algorithms** behind one uniform `ratelimit.Limiter` interface (`ratelimit/limiter.go:23`),
five of them with **atomic Redis-Lua distributed backends**, plus a full set of resilience
patterns (circuit breaker, retry, timeout, bulkhead, fallback/hedging) composable through
a fixed-order `pipeline` (`pipeline/pipeline.go:6-20`). It has a deterministic `ManualClock`
(`internal/clock/clock.go`), fuzzing, a chaos harness, goroutine-leak detection, zero core
dependencies (enforced in CI), and a polished Next.js playground driven by real WebSockets.
The audit trail is exceptional.

The gaps are therefore **not correctness gaps** — they are **completeness, competitiveness,
and adoption gaps**. Six themes dominate:

1. **Observability is a facade.** Prometheus counters are *declared but never incremented*
   in the library core; the OpenTelemetry tracer is a documented no-op stub
   (`server/telemetry/tracer.go:16-49`); and the shipped Grafana dashboard queries ~10
   metric series that are never emitted (`deploy/grafana/dashboards/resilience.json`). The
   *library* has no metric/trace integration at all — only the demo server's HTTP middleware
   records anything. This is the single biggest credibility gap for a "production-grade" claim.

2. **Missing modern algorithms.** No **concurrency limiter** (Netflix AIMD/Vegas/Gradient),
   no **load shedder** (CoDel / priority-aware admission), no **retry budget** (retry-storm
   guard), no **distributed circuit breaker**, and no **cost/weight-aware** limiting beyond
   integer `AllowN`. These are the features that differentiate a toolkit from `x/time/rate`.

3. **Ergonomics friction.** The core `Limiter` interface has **no functional-options
   constructor parity** across algorithms, no generic `Execute[T]` on the main resilience
   paths (only helper `DoWithResult[T]` in retry/timeout/fallback), no rate-limit callback
   hooks (only the circuit breaker has `OnStateChange` etc., `circuitbreaker/config.go:73-83`),
   and middleware for only stdlib `net/http` + gRPC — no chi/gin/echo/fiber/connect where
   most Go HTTP traffic actually lives.

4. **CI has adoption-blocking holes.** No `govulncheck`, **the frontend is not tested in CI
   at all** (Playwright exists but never runs), no benchmark-regression gate despite
   `make bench-compare` existing, and no supply-chain hardening (SBOM, signing, SLSA) in
   `release.yml`.

5. **Distributed backend is Redis-only** with **client-supplied time** in every Lua script
   (`ratelimit/store/redis.go:364-541`) — clock-skew is unmitigated — and the documented
   float64/256ns snapping in GCRA/sliding-window scores (`scripts_memory.go:242-268`) is a
   known accuracy limit worth a first-class fix or documented guarantee.

6. **Docs/adoption polish is missing.** No hosted docs site, no migration guides from
   `x/time/rate`/`gobreaker`, no published head-to-head benchmarks, and no recipe cookbook —
   all of which drive real-world adoption.

**Total: 72 concrete enhancements** across 11 dimensions. If you do only the
[Top 15](#top-15-prioritized-enhancements), you close every P0/P1 credibility gap and make
the library demonstrably more competitive than the incumbents it's measured against.

---

## Top 15 Prioritized Enhancements

| # | Title | Category | Priority | Effort |
|---|-------|----------|----------|--------|
| 1 | Wire real metrics into the library core (Prometheus + OTel meter via a `Recorder` interface) | Observability | **P0** | L |
| 2 | Add `govulncheck` + frontend lint/typecheck/Playwright to CI | Testing / Security | **P0** | S |
| 3 | Real OpenTelemetry tracing (replace the stub with a wired SDK path) | Observability | **P0** | M |
| 4 | Fix the Grafana dashboard ↔ emitted-metric mismatch | Observability | **P0** | S |
| 5 | Concurrency limiter (Netflix AIMD / Gradient2 / Vegas) | Algorithms | **P1** | L |
| 6 | Retry budget (token-bucket guard against retry storms) | Algorithms | **P1** | M |
| 7 | Rate-limit-by-cost / weight (first-class `Cost` beyond `AllowN`) | Algorithms / API | **P1** | M |
| 8 | Framework middleware: chi, gin, echo, fiber, connect | API | **P1** | M |
| 9 | Rate-limiter event hooks (`OnAllowed`/`OnDenied`/`OnWait`) | API / Observability | **P1** | S |
| 10 | Redis clock-skew mitigation (server `TIME`, skew detection) | Distributed | **P1** | M |
| 11 | Load shedder (CoDel-style + priority-aware admission) | Algorithms | **P1** | L |
| 12 | Supply-chain hardening: SBOM + cosign signing + SLSA provenance | Security / Release | **P1** | M |
| 13 | goreleaser + multi-arch Docker push + Helm chart | Release | **P1** | M |
| 14 | Benchmark regression gate in CI (benchstat) + published benchmarks | Performance / Community | **P1** | M |
| 15 | Migration guides (from `x/time/rate` & `gobreaker`) + recipe cookbook | DX / Docs | **P1** | M |

---

## Legend

- **Priority** — **P0** must-have (credibility/adoption blocker) · **P1** high · **P2** medium · **P3** nice-to-have.
- **Effort** — **S** ≤1 day · **M** a few days · **L** ~1–2 weeks · **XL** multi-week / cross-cutting.
- File references use `path:line`. All paths are relative to the repo root
  `/Users/sanskar/dev/Research/Projects/Rate-Limiter-Circuit-Breaker`.

---

## 1. Missing Algorithms & Patterns

### 1.1 Concurrency limiter (Netflix AIMD / Gradient2 / Vegas)  
> ✅ **Implemented & merged** (item 5)
- **Category:** Algorithms · **Priority:** P1 · **Effort:** L
- **Rationale:** Rate limits cap *request rate*; a concurrency limiter caps *in-flight work*
  adaptively from measured latency/RTT, which is the single most effective defense against
  latency-induced overload. This is the flagship pattern in Netflix `concurrency-limits` and
  has no Go-native, well-tested implementation. It is what separates a modern toolkit from
  a rate-limiter collection.
- **What exists today:** `bulkhead/bulkhead.go` is a *static* semaphore
  (`maxConcurrency` fixed at construction, `bulkhead.go:21-95`). `ratelimit/adaptive`
  (`adaptive/adaptive.go:11-29`) adjusts a token-bucket rate by ±5/10% on CPU/error/P99
  signals with gradient smoothing, but it is **rate-based, not concurrency-based**, and does
  **not** implement AIMD or Vegas gradient descent on RTT. No packet-loss / queueing-delay
  congestion signal exists.
- **Proposed approach:** New package `concurrency/` with pluggable limit algorithms:
  ```go
  lim := concurrency.NewGradient2(concurrency.Config{
      InitialLimit: 20, MaxLimit: 1000, MinLimit: 4,
      RTTTolerance: 1.5, // Vegas/Gradient smoothing knob
  })
  release, ok := lim.Acquire(ctx) // ok=false when at limit
  defer release(concurrency.Outcome{RTT: d, Dropped: false})
  ```
  Implement AIMD, Gradient2, and Vegas as strategies; feed observed RTT + drop into the
  limit update. Integrate as a new pipeline stage kind between bulkhead and timeout.
- **Risks/tradeoffs:** Adaptive limits can oscillate; needs windowed RTT percentiles and a
  minimum-limit floor to avoid deadlock under noisy latency. Requires careful benchmarking.
- **References:** Netflix `concurrency-limits` (Gradient2/Vegas), TCP Vegas paper,
  Google SRE "Handling Overload".

### 1.2 Load shedder (CoDel-style + priority-aware admission)  
> ✅ **Implemented & merged** (item 11)
- **Category:** Algorithms · **Priority:** P1 · **Effort:** L
- **Rationale:** When saturated, dropping *the right* requests (low-priority, or those that
  have already queued too long) preserves goodput far better than a flat limit. CoDel-style
  queue-sojourn shedding is what keeps p99 bounded under overload.
- **What exists today:** None. `pipeline/pipeline.go` treats every request identically; the
  leaky bucket queue (`leakybucket/leakybucket.go:50`) is FIFO with no sojourn-time drop and
  no priority. Bulkhead simply rejects when full (`bulkhead.go:48-56`).
- **Proposed approach:** New `loadshed/` package: a controlled-delay (CoDel) admission
  controller keyed on measured queue sojourn time, plus a `Priority` (int) carried on the
  context so shedding drops lowest priority first. Expose as a pipeline stage and a
  standalone `Admit(ctx) bool`.
- **Risks/tradeoffs:** Priority requires a call-site convention; mis-set priorities can
  starve. Keep CoDel target/interval tunable.
- **References:** Kathleen Nichols & Van Jacobson "Controlling Queue Delay" (CoDel),
  Google SRE overload chapter, Envoy adaptive concurrency filter.

### 1.3 Retry budget (retry-storm guard)  
> ✅ **Implemented & merged** (item 6)
- **Category:** Algorithms · **Priority:** P1 · **Effort:** M
- **Rationale:** Unbudgeted retries amplify load exactly when a dependency is failing,
  turning a brownout into an outage. Every mature stack (Polly, Hystrix, Envoy, gRPC) caps
  retries as a *fraction of throughput*, not per-request.
- **What exists today:** `retry/retry.go:26-174` supports `MaxAttempts`, `RetryIf`,
  `OnRetry`, `MaxDelay`, and four backoff strategies — but there is **no global budget**.
  The library relies entirely on the circuit breaker opening to stop cascades.
- **Proposed approach:** Add a `retry.Budget` (token bucket over retry attempts, e.g. "retry
  ratio 0.1 + burst 10") shared across calls; `retry.Do` consumes a retry token before each
  extra attempt and skips retrying when the budget is exhausted. Wire a default budget into
  the pipeline retry stage.
  ```go
  budget := retry.NewBudget(retry.BudgetConfig{Ratio: 0.1, MinPerSecond: 3})
  policy := retry.New(retry.WithMaxAttempts(3), retry.WithBudget(budget))
  ```
- **Risks/tradeoffs:** Shared budget is another contention point (use an atomic token bucket);
  needs per-target scoping to avoid one hot key starving others.
- **References:** Envoy retry budgets, gRPC `retryThrottling`, Google SRE retry amplification.

### 1.4 Distributed circuit breaker  
> ✅ **Implemented & merged** (P2)
- **Category:** Algorithms / Distributed · **Priority:** P2 · **Effort:** L
- **Rationale:** In a fleet, each instance learns a dependency is down independently and
  slowly. Shared breaker state (or shared failure counters) trips the whole fleet fast and
  consistently.
- **What exists today:** `circuitbreaker/` is strictly single-instance; no `distributed.go`,
  no store integration. Contrast with `ratelimit/store` which *does* have a Redis backend
  that could be reused.
- **Proposed approach:** Optional `circuitbreaker/distributed` that writes bucketed failure
  counts to the existing `ratelimit/store.Store` (reuse the Lua-script infra) and reads a
  shared state gauge, with local fast-path caching to avoid a Redis hop per call. Keep
  eventual consistency; local breaker remains the source of truth for rejection.
- **Risks/tradeoffs:** Adds latency/Redis dependency to the hot path; must degrade to local
  on store failure (mirror the fail-open pattern in `store/redis.go`).
- **References:** resilience4j distributed state, mercari/go-circuitbreaker, Polly.

### 1.5 Rate-limit-by-cost / weight  
> ✅ **Implemented & merged** (item 7)
- **Category:** Algorithms / API · **Priority:** P1 · **Effort:** M
- **Rationale:** Real APIs charge different costs (a bulk write ≠ a health check). GitHub,
  Stripe, and Shopify all bill in weighted "points". `AllowN(n)` technically models this but
  only as an integer token count and is not surfaced as a first-class "cost" concept in
  middleware or results.
- **What exists today:** `AllowN(ctx, key, n int)` (`ratelimit/limiter.go:29`) consumes n
  tokens all-or-nothing. GCRA/token-bucket support it, but middleware
  (`ratelimit/middleware/http.go`) always uses cost 1 — there is no `CostFunc(r)` hook.
- **Proposed approach:** Add `WithCost(func(*http.Request) int)` to HTTP/gRPC middleware and
  document the cost model; optionally a `float64` cost path for token bucket (which already
  stores fractional tokens, `tokenbucket.go:37`). Surface consumed cost in `Result.Metadata`.
- **Risks/tradeoffs:** Float costs re-introduce the precision concerns (§5.4); keep integer
  as the default, float opt-in.
- **References:** Stripe rate-limit blog, Shopify GraphQL cost-based limiting, GitHub API points.

### 1.6 Hierarchical / tiered limits
- **Category:** Algorithms · **Priority:** P2 · **Effort:** M
- **Rationale:** Common real requirement: "10 req/s per user **and** 1000 req/s per org **and**
  100k req/s global". Users must currently hand-wire multiple composites and keys.
- **What exists today:** `ratelimit/composite/composite.go` supports flat AND/OR only
  (`composite.go:59` serializes AND for atomicity) — no hierarchy, no per-level key
  derivation, no "which level tripped" attribution in the result.
- **Proposed approach:** `ratelimit/tiered` that takes an ordered list of
  `(keyFunc, limiter)` tiers, checks tightest-first, and reports the tripping tier in
  `Result.Metadata["tier"]`. Reuse the composite AND two-phase reserve/commit to keep it
  atomic across tiers.
- **Risks/tradeoffs:** Multi-tier atomicity across a distributed store needs a single Lua
  script per check to avoid partial consumption; start with local-only.
- **References:** Cloudflare tiered limits, Kong/Envoy multi-descriptor rate limiting.

### 1.7 Priority queue for `Wait`/leaky bucket
- **Category:** Algorithms · **Priority:** P3 · **Effort:** M
- **Rationale:** Under contention, high-priority callers should acquire tokens/slots first.
- **What exists today:** `Wait`/`WaitN` and the leaky bucket queue are FIFO
  (`leakybucket.go:50` buffered channel; `bulkhead.go` semaphore is unordered).
- **Proposed approach:** Optional priority-aware `Wait` using a heap of waiters keyed by a
  context-carried priority; leaky bucket variant with a priority queue instead of a channel.
- **Risks/tradeoffs:** Priority inversion / starvation; needs aging. Higher complexity than
  a channel — keep it opt-in.
- **References:** Java `PriorityBlockingQueue`, WFQ scheduling.

### 1.8 Distributed leaky bucket & adaptive limiter
- **Category:** Algorithms / Distributed · **Priority:** P2 · **Effort:** M
- **Rationale:** Five of eight algorithms have Redis backends (TB, GCRA, FW, SWL, SWC), but
  **leaky bucket and adaptive do not** (`leakybucket/` and `adaptive/` have no `distributed.go`).
  This is an inconsistency users will hit.
- **What exists today:** Confirmed absent — no `distributed.go` in `ratelimit/leakybucket` or
  `ratelimit/adaptive`.
- **Proposed approach:** Leaky bucket maps cleanly to GCRA math; provide a distributed leaky
  bucket via a Lua script analogous to `GCRAScript` (`store/redis.go:431`). Adaptive is harder
  (signals are node-local) — document it as intentionally node-local, or share only the
  computed limit via the store.
- **Risks/tradeoffs:** Adaptive with shared limit needs a leader/aggregation model; may not be
  worth it — a doc note may suffice.
- **References:** Stripe GCRA writeup (leaky-bucket-as-GCRA duality).

### 1.9 Debounce / throttle primitives
- **Category:** Algorithms · **Priority:** P3 · **Effort:** S
- **Rationale:** Frequently-requested lightweight primitives (coalesce bursts, trailing-edge
  throttle) that pair naturally with a resilience toolkit.
- **What exists today:** None.
- **Proposed approach:** Small `throttle/` package: `Debounce(d)` and `Throttle(d)` wrappers
  around a `func()`, clock-injected for deterministic tests (reuse `internal/clock`).
- **Risks/tradeoffs:** Scope creep — keep minimal; clearly distinct from rate limiting.
- **References:** Lodash debounce/throttle semantics.

### 1.10 Cache-aside / stale-fallback pattern  
> ✅ **Implemented & merged** (P2)
- **Category:** Algorithms · **Priority:** P2 · **Effort:** M
- **Rationale:** The most common real fallback is "serve last-known-good value on failure",
  which the current fallback package cannot express without user plumbing.
- **What exists today:** `fallback/fallback.go:30-36` requires an explicit fallback *function*;
  no built-in cache, no stale-while-error, no static-value fallback.
- **Proposed approach:** `fallback.Cached[T]` that records successful results with a TTL and
  serves stale values on primary failure (single-flight the refresh). Add
  `fallback.Static(v)` convenience.
- **Risks/tradeoffs:** Introduces state/memory and staleness semantics; generics keep it
  type-safe. Keep the cache pluggable.
- **References:** Polly fallback + caching, Hystrix request cache, `golang.org/x/sync/singleflight`.

### 1.11 Half-open probe strategies (gradual ramp / probe budget)
- **Category:** Algorithms · **Priority:** P2 · **Effort:** M
- **Rationale:** A single-probe half-open recovers slowly; a fixed concurrent-probe count can
  slam a still-fragile dependency. Gradual ramp (1→2→4…) recovers faster *and* safer.
- **What exists today:** `HalfOpenMaxRequests` is a **fixed** concurrent-probe cap
  (`config.go:51-53`, enforced by CAS at `circuitbreaker.go:205-214`) with a consecutive-
  success close threshold (`SuccessThreshold`, `config.go:55-57`). No ramp, no probe budget,
  no slow-start after close.
- **Proposed approach:** Add `HalfOpenRampStrategy` (fixed | linear | exponential) that grows
  the probe allowance as consecutive successes accrue, plus an optional post-close "slow start"
  that limits throughput briefly after re-closing.
- **Risks/tradeoffs:** More state and config surface; must remain backward compatible
  (default = current fixed behavior).
- **References:** resilience4j half-open, Polly, Hystrix rolling recovery.

### 1.12 Per-attempt deadline budgeting in the pipeline
- **Category:** Algorithms · **Priority:** P2 · **Effort:** M
- **Rationale:** A single shared deadline across all retries can spend the whole budget on
  attempt 1; per-attempt budgeting (or deadline propagation with reserve) gives every attempt
  a fair, shrinking slice.
- **What exists today:** `timeout/timeout.go:47-105` propagates the context deadline correctly,
  and the pipeline shares one timeout across retries (`pipeline/order_test.go:40-67`) — but
  there is **no per-attempt sub-budget** and no deadline-aware retry that stops early when the
  remaining budget can't fit another attempt.
- **Proposed approach:** `retry.WithDeadlineBudget` that computes a per-attempt timeout from
  the remaining context deadline and refuses a retry it can't complete in time.
- **Risks/tradeoffs:** Interacts with backoff (backoff must fit the budget); needs clear
  precedence rules.
- **References:** gRPC deadline propagation, Google SRE deadlines chapter.

---

## 2. API & Ergonomics

### 2.1 Rate-limiter event hooks (`OnAllowed` / `OnDenied` / `OnWait`)  
> ✅ **Implemented & merged** (item 9)
- **Category:** API / Observability · **Priority:** P1 · **Effort:** S
- **Rationale:** The circuit breaker exposes rich callbacks (`OnStateChange`, `OnSuccess`,
  `OnFailure`, `OnRejected`, `config.go:73-83`) but **rate limiters expose none** — consumers
  can only inspect the returned `Result`. Hooks are the cleanest metrics/logging integration
  point (and unblock §4.1).
- **What exists today:** No callback fields anywhere in the algorithm packages; `Result`/`State`
  (`ratelimit/limiter.go:47-101`) are the only observability surface.
- **Proposed approach:** Add optional functional-option hooks to every limiter constructor:
  `WithOnDecision(func(key string, r Result))`. Fire after each Allow/AllowN. Keep nil-cheap.
- **Risks/tradeoffs:** Hooks on the hot path must be branch-predicted-cheap when nil; document
  that they run synchronously.
- **References:** gobreaker `OnStateChange`, resilience4j event publisher.

### 2.2 Functional-options constructor parity + a unified builder
- **Category:** API · **Priority:** P2 · **Effort:** M
- **Rationale:** Constructors are inconsistent: `tokenbucket.New(cap, rate, opts...)`,
  `gcra.New(limit, burst, window, opts...)`, `slidingwindow.NewLog(limit, window, opts...)`,
  `adaptive.New(init, min, max, signals, opts...)`. Positional args differ in count/meaning,
  which hurts discoverability and makes swapping algorithms harder than the "one-line change"
  the README promises.
- **What exists today:** Each package has its own `options.go` with a subset of options
  (`WithClock`, `WithIdleCleanup`, `WithBurst`…); no shared option vocabulary; several
  constructors **panic** on bad input rather than returning an error (e.g. `tokenbucket.New`).
- **Proposed approach:** Introduce a `ratelimit.Builder` / `ratelimit.New(algo, opts...)`
  facade returning `(Limiter, error)`, and normalize shared options (`WithClock`,
  `WithIdleCleanup`, `WithStore`, `WithOnDecision`) into one place. Keep the concrete
  constructors for power users.
- **Risks/tradeoffs:** A facade adds an interface indirection; keep the direct constructors.
  Changing panic→error is a breaking change — do it pre-1.0.
- **References:** `x/time/rate` (single `NewLimiter`), Dave Cheney functional options.

### 2.3 Generic `Execute[T]` on the core resilience paths
- **Category:** API · **Priority:** P2 · **Effort:** M
- **Rationale:** Only `retry`, `timeout`, and `fallback` have `DoWithResult[T]`; the circuit
  breaker, bulkhead, and pipeline `Execute` take `func() error` and force callers to capture
  results via closures. A typed `Execute[T]` is the modern idiom (Go 1.24 module floor already
  supports generics).
- **What exists today:** `circuitbreaker.Execute(ctx, fn func() error)`, `bulkhead.Execute`,
  `pipeline.Execute` are all `error`-only; generics appear only in the three helper packages.
- **Proposed approach:** Add `ExecuteResult[T]` (or a package-level `Do[T](cb, ...)`) across
  circuit breaker, bulkhead, and pipeline, mirroring the retry helper signature.
- **Risks/tradeoffs:** Methods can't have type parameters in Go, so these must be package-level
  functions — a small naming asymmetry. Document it.
- **References:** samber/mo, resilience4j `Supplier<T>`.

### 2.4 Framework middleware: chi, gin, echo, fiber, connect  
> ✅ **Implemented & merged** (item 8)
- **Category:** API · **Priority:** P1 · **Effort:** M
- **Rationale:** Middleware exists only for stdlib `net/http` (`ratelimit/middleware/http.go`,
  `circuitbreaker/middleware/http.go`) and gRPC. The majority of Go HTTP services use chi/gin/
  echo/fiber; connect-go is the modern RPC choice. Missing adapters is a direct adoption
  blocker — people won't reach for a library that doesn't drop into their router.
- **What exists today:** stdlib `func(http.Handler) http.Handler` + gRPC unary/stream
  interceptors only. `KeyByIP/KeyByHeader/KeyByParam` extractors exist (`http.go:20-49`).
- **Proposed approach:** Thin adapters in `ratelimit/middleware/{chi,gin,echo,fiber,connect}`
  (and the same for circuit breaker) that reuse the existing extractor + response-header logic.
  Chi is trivial (already `http.Handler`). Keep each behind its own module tag / build so the
  core stays zero-dep (mirror the gRPC pattern).
- **Risks/tradeoffs:** Each adds an optional dependency — must not leak into the core module
  (CI `verify-zero-deps` guards this). Consider a separate `/contrib` module.
- **References:** `ulule/limiter` (which ships many framework stores/middlewares), `didip/tollbooth`.

### 2.5 Error taxonomy: typed circuit-breaker & bulkhead errors
- **Category:** API · **Priority:** P2 · **Effort:** S
- **Rationale:** Rate limiting has a rich `RateLimitError` with `Unwrap`/`Is`
  (`ratelimit/errors.go:29-60`), but resilience packages are thinner. Callers need to
  distinguish "circuit open" from "downstream error" from "bulkhead full" via `errors.Is`.
- **What exists today:** `circuitbreaker/errors.go` and `bulkhead` sentinels exist
  (`ErrTooManyRequests`, `ErrOpen`, rejection errors) but there is no rich typed error
  carrying breaker name/state or bulkhead inflight/queue context.
- **Proposed approach:** Introduce `CircuitBreakerError{Name, State, Err}` and
  `BulkheadError{Name, Inflight, Rejected}` with `Unwrap`/`Is`, mirroring `RateLimitError`.
- **Risks/tradeoffs:** Breaking if callers switch on concrete sentinels; keep sentinels as the
  `Is` target.
- **References:** `ratelimit/errors.go` (as the internal template), resilience4j exceptions.

### 2.6 Interface segregation: a read-only `Peeker` / `Observer`
- **Category:** API · **Priority:** P3 · **Effort:** S
- **Rationale:** The `Limiter` interface bundles mutating (`Allow`, `Reset`) and read-only
  (`Peek`) methods (`ratelimit/limiter.go:23-45`). Dashboards/exporters that only observe are
  forced to depend on the full interface.
- **What exists today:** Single fat `Limiter` interface; no smaller `Peeker`.
- **Proposed approach:** Extract `Peeker interface { Peek(...) State }` and have `Limiter`
  embed it. Non-breaking (embedding).
- **Risks/tradeoffs:** Minor; mostly upside.
- **References:** Go proverb "the bigger the interface, the weaker the abstraction".

### 2.7 Zero-value usability for config structs
- **Category:** API · **Priority:** P3 · **Effort:** S
- **Rationale:** Circuit breaker `Config` already applies defaults via `defaults()`
  (`config.go:90-124`) — good. But limiter constructors panic on zero/invalid input rather
  than degrading. A usable zero value or a `Default*()` helper lowers the barrier.
- **What exists today:** `circuitbreaker.Config` zero value is usable via `defaults()`; rate
  limiters require explicit valid args and panic otherwise.
- **Proposed approach:** Provide `tokenbucket.Default()`, etc., with sane presets; document
  which zero values are legal.
- **Risks/tradeoffs:** Presets can mislead; document clearly.
- **References:** stdlib `http.Server{}` zero-value usability.

### 2.8 Context-based cost & priority propagation helpers
- **Category:** API · **Priority:** P2 · **Effort:** S
- **Rationale:** §1.5 (cost) and §1.7/§1.2 (priority) both need a call-site convention.
  Provide typed context helpers so middleware and pipeline stages share one vocabulary.
- **What exists today:** None; context only carries request IDs on the server side
  (`server/logger/logger.go:38-49`).
- **Proposed approach:** `resilience.WithCost(ctx, n)` / `resilience.WithPriority(ctx, p)` and
  matching getters, consumed by middleware, load shedder, and tiered limiter.
- **Risks/tradeoffs:** Context values are stringly-typed footguns; use unexported key types.
- **References:** gRPC metadata conventions.

---

## 3. Performance

### 3.1 Shard hot per-key maps to cut global-lock contention
- **Category:** Performance · **Priority:** P2 · **Effort:** M
- **Rationale:** Every algorithm guards its key map with a single global `sync.RWMutex`
  (e.g. token bucket `buckets` map at `tokenbucket.go:51`, GCRA `entries` at `gcra.go:57`,
  fixed window at `fixedwindow.go:49`). Under high key cardinality + concurrency this is the
  dominant contention point. Sharded maps (N stripes by key hash) are the standard fix.
- **What exists today:** Single RWMutex per limiter over the whole key map; no sharding, no
  `sync.Map` in the algorithms (only the Redis store uses `sync.Map`, `store/memory.go:30`).
- **Proposed approach:** Introduce an internal sharded map (`internal/shardmap`) with, say,
  256 stripes keyed by `xxhash(key)` (xxhash is already a transitive dep), and use it in all
  local limiters. Benchmark contention before/after with `-cpu=1,4,16`.
- **Risks/tradeoffs:** Sharding complicates cleanup/iteration (must sweep all shards); more
  memory for many small maps. Validate with the existing chaos harness.
- **References:** `orcaman/concurrent-map`, Go runtime map sharding patterns.

### 3.2 `sync.Pool` for transient per-call allocations
- **Category:** Performance · **Priority:** P3 · **Effort:** S
- **Rationale:** Core Allow paths are already 0-alloc (per CHANGELOG), but `Result.Metadata`
  is a `map[string]any` (`ratelimit/limiter.go:77`) and distributed paths format floats to
  strings (`store/redis.go` uses `strconv.FormatFloat`) — both allocate. Pooling or lazy
  metadata avoids garbage on the distributed hot path.
- **What exists today:** No `sync.Pool` anywhere; metadata maps allocated eagerly.
- **Proposed approach:** Make `Metadata` lazy (only populated when a hook/observer is
  attached), and pool the Redis arg slices / `[]interface{}` used per Eval.
- **Risks/tradeoffs:** Pooled objects must be reset carefully; lazy metadata changes an
  observable field's population semantics (document it).
- **References:** `sync.Pool` best practices, `bytebufferpool`.

### 3.3 Escape-analysis & allocation profiling pass on distributed paths
- **Category:** Performance · **Priority:** P2 · **Effort:** M
- **Rationale:** Benchmarks cover local algorithms well (`*_bench_test.go` in tokenbucket,
  gcra, slidingwindow, leakybucket, circuitbreaker) but there are **no distributed/Redis
  benchmarks** and no `-gcflags=-m` escape analysis in CI. The Redis path (arg marshalling,
  float formatting, script Eval) is where allocations actually hurt.
- **What exists today:** No `distributed_*_bench_test.go`; `make bench` runs local benches
  with `-benchmem -count=5`.
- **Proposed approach:** Add distributed benchmarks (against a testcontainer/miniredis), run
  `go build -gcflags=-m` on hot packages in CI, and profile with `-memprofile`.
- **Risks/tradeoffs:** Redis benches need infra; use miniredis for CI speed and real Redis for
  nightly.
- **References:** `benchstat`, `pprof`, dgraph benchmarking posts.

### 3.4 Atomic fast-path audit for time-window circuit breaker
- **Category:** Performance · **Priority:** P3 · **Effort:** S
- **Rationale:** The count-window breaker uses atomics well, but the time-window breaker's
  bucket slide (`circuitbreaker/metrics.go:161-180`) takes locks on every request under a
  full mutex. A `RLock` fast-path for the common "no slide needed" case reduces contention.
- **What exists today:** Time window uses precomputed counters but slides under a write path
  each call.
- **Proposed approach:** Split read (check current bucket) from write (slide) with a
  double-checked RLock→Lock upgrade, or make the newest-bucket index atomic.
- **Risks/tradeoffs:** Lock upgrades are subtle; keep the CAS-based count window as the default
  recommendation.
- **References:** `sony/gobreaker` counter design.

### 3.5 Benchmark coverage gaps (middleware, composite, pipeline, adaptive)
- **Category:** Performance · **Priority:** P3 · **Effort:** S
- **Rationale:** No benchmarks exist for the HTTP/gRPC middleware wrappers, the composite
  limiter's two-phase AND path (`composite.go:59`), the full pipeline, or the adaptive
  adjustment loop — all of which add overhead users care about.
- **What exists today:** Benchmarks only in tokenbucket, gcra, slidingwindow, leakybucket,
  circuitbreaker.
- **Proposed approach:** Add `*_bench_test.go` for middleware, composite, pipeline, adaptive;
  include them in the regression gate (§6.5).
- **Risks/tradeoffs:** More CI time; gate only on the hot ones.
- **References:** standard Go benchmarking guidance.

---

## 4. Observability

### 4.1 Wire real metrics into the library core via a `Recorder` interface  
> ✅ **Implemented & merged** (item 1)
- **Category:** Observability · **Priority:** **P0** · **Effort:** L
- **Rationale:** This is the #1 credibility gap. The library core emits **zero** metrics:
  Prometheus counters are declared in `server/metrics/prometheus.go:10-60` but the rate-limit
  counters (`RateLimitTotal/Allowed/Denied`) and most CB counters are **never `.Inc()`'d** —
  only the demo server's HTTP middleware records anything (`server/api/middleware.go:129-132`).
  A "production-grade" resilience library that you can't observe is not adoptable.
- **What exists today:** Metrics live only in the demo server, disconnected from the library.
  CB has callbacks (`config.go:73-83`) a user *could* wire, but rate limiters have no hooks at
  all (§2.1).
- **Proposed approach:** Define a small `metrics.Recorder` interface in a new `observability/`
  (or `metric/`) package — `IncAllowed(algo, key)`, `IncDenied(...)`, `ObserveDecision(dur)`,
  `SetActiveKeys(n)`, `RecordCBState(name, state)` — with a Prometheus adapter and an OTel
  meter adapter. Fire it from the (new) limiter hooks (§2.1) and existing CB callbacks. Keep a
  no-op default so the core stays zero-dep.
- **Risks/tradeoffs:** Per-key label cardinality is a DoS/cost risk — expose bounded label sets
  and document not to label by raw key (the demo already normalizes method labels,
  `middleware.go:105-147`; apply the same discipline).
- **References:** OTel Go Meter API, Prometheus client_golang, RED/USE methodology.

### 4.2 Real OpenTelemetry tracing (replace the stub)  
> ✅ **Implemented & merged** (item 3)
- **Category:** Observability · **Priority:** **P0** · **Effort:** M
- **Rationale:** `server/telemetry/tracer.go` is an explicit no-op stub (all spans do nothing,
  `tracer.go:16-49`; `Shutdown` returns nil, line 49). There is no real span creation, no
  context propagation, no exporter. Distributed tracing is table-stakes for modern services.
- **What exists today:** Stub only, gated on `OTEL_ENABLED` but wired to nothing.
- **Proposed approach:** Provide a real OTel path: a configured `TracerProvider` (OTLP
  exporter) behind a build tag or optional module, spans around rate-limit decisions, breaker
  Execute, bulkhead acquire, and retries, with trace context propagated through the pipeline.
  Emit **exemplars** linking Prometheus histograms to trace IDs.
- **Risks/tradeoffs:** OTel SDK is a heavy dependency — keep it in an optional module so the
  core stays zero-dep.
- **References:** OpenTelemetry Go SDK, OTel semantic conventions, exemplars spec.

### 4.3 Fix the Grafana dashboard ↔ emitted-metric mismatch  
> ✅ **Implemented & merged** (item 4)
- **Category:** Observability · **Priority:** **P0** · **Effort:** S
- **Rationale:** The shipped dashboard (`deploy/grafana/dashboards/resilience.json`) queries
  ~10 series that are **never emitted**: `resilience_ratelimit_decision_duration_seconds_bucket`,
  `resilience_ratelimit_active_keys`, `resilience_circuitbreaker_state`,
  `resilience_circuitbreaker_requests_total`, `..._state_transitions_total`,
  `..._execution_duration_seconds_bucket`. A demo that ships broken panels actively damages
  credibility.
- **What exists today:** Dashboard panels reference metrics with names/shapes that don't match
  `server/metrics/prometheus.go` (which uses `resilience_cb_*` and `resilience_ratelimit_*_total`,
  and defines no histograms/gauges for RL/CB).
- **Proposed approach:** After §4.1, emit exactly the series the dashboard expects (or update
  the dashboard to match). Add a CI check that greps dashboard queries against registered
  metric names.
- **Risks/tradeoffs:** None significant; mostly a correctness/consistency fix.
- **References:** Grafana dashboard-as-code, promtool.

### 4.4 Structured-logging hooks for library consumers
- **Category:** Observability · **Priority:** P2 · **Effort:** S
- **Rationale:** `server/logger/logger.go` is `slog`-based but **server-internal only**; the
  library packages have no logging integration. Consumers want to plug their own `slog.Logger`
  for decisions/state changes.
- **What exists today:** Logger is a server concern; no `WithLogger` on any library type.
- **Proposed approach:** Accept an optional `*slog.Logger` via functional option on breakers/
  limiters/pipeline; log at debug for decisions, info for state changes. Nil = silent.
- **Risks/tradeoffs:** Logging on the hot path must be guarded by level checks.
- **References:** stdlib `log/slog`, `slog.LevelVar` gating.

### 4.5 Prometheus histograms with exemplars + native histograms
- **Category:** Observability · **Priority:** P2 · **Effort:** M
- **Rationale:** The only histogram (`resilience_http_request_duration_seconds`,
  `prometheus.go:58`) uses default buckets and no exemplars. Decision latency, breaker Execute
  latency, and bulkhead wait time should be histograms with exemplars (link to traces) and,
  ideally, Prometheus native histograms.
- **What exists today:** One default-bucket HTTP histogram; no RL/CB latency histograms.
- **Proposed approach:** Add tuned-bucket histograms for decision/execute/wait; use
  `ExemplarObserver` to attach trace IDs; opt into native histograms.
- **Risks/tradeoffs:** Exemplars require a tracing context; native histograms need a recent
  Prometheus server.
- **References:** Prometheus exemplars, native histograms.

### 4.6 Bulkhead queue/wait metrics (RED/USE saturation)  
> ✅ **Implemented & merged** (P2)
- **Category:** Observability · **Priority:** P2 · **Effort:** S
- **Rationale:** Bulkhead exposes only `Inflight()` and `Rejected()` (`bulkhead.go:86-95`) — no
  **queue depth**, no **wait-time** distribution. Saturation is the "U" in USE and the key
  early-warning signal for concurrency exhaustion.
- **What exists today:** Two counters; the thread-pool variant (`threadpool.go`) has a queue but
  no depth metric.
- **Proposed approach:** Track waiting count, queue depth, and a wait-time histogram; expose via
  the new Recorder (§4.1).
- **References:** Hystrix bulkhead metrics, resilience4j bulkhead.

### 4.7 Event-stream abstraction for the playground and consumers
- **Category:** Observability · **Priority:** P3 · **Effort:** M
- **Rationale:** The demo streams `rate_limit_result` / `cb_state_change` / `sim_stats` over
  WebSockets from the *server* (`server/api/hub.go`), reimplementing what a library-level event
  bus could provide once. A generic event sink would let any consumer subscribe.
- **What exists today:** WebSocket hub is server-only; no library event bus.
- **Proposed approach:** Library-level `Events()` channel/subscription built on the hooks
  (§2.1/§4.1); the demo server subscribes instead of hand-instrumenting handlers.
- **References:** resilience4j `EventPublisher`.

---

## 5. Distributed Correctness

### 5.1 Redis clock-skew mitigation (server `TIME`, skew detection)  
> ✅ **Implemented & merged** (item 10)
- **Category:** Distributed · **Priority:** P1 · **Effort:** M
- **Rationale:** **Every** Lua script uses **client-supplied `now`** (token bucket
  `redis.go:369`, GCRA `redis.go:436`, sliding-window-log `redis.go:516`). If application
  clocks drift, distributed limits become inaccurate (forward skew can wrongly evict/deny;
  backward skew under-counts). Multi-region fleets always have some skew.
- **What exists today:** No use of Redis `TIME`, no skew detection, no monotonic-source note.
  The fail-open fallback (`store/redis.go:54-64`) is good but orthogonal.
- **Proposed approach:** Offer a mode that reads Redis `TIME` inside the Lua script (single
  authoritative clock), or clamp client time to `[server_time-ε, server_time+ε]` and emit a
  skew metric. Document the tradeoff (server TIME adds a call / reduces script portability).
- **Risks/tradeoffs:** `redis.call('TIME')` is non-deterministic and disallowed in replicated
  scripts unless `redis.replicate_commands()` is used — must handle Redis-version semantics.
- **References:** Redis `TIME` + `replicate_commands`, Stripe/Shopify GCRA writeups, Google TrueTime.

### 5.2 Additional backends: Memcached, DynamoDB, and gossip/in-cluster
- **Category:** Distributed · **Priority:** P2 · **Effort:** L
- **Rationale:** Redis is the only distributed backend (`ratelimit/store/redis.go`). Teams on
  Memcached, DynamoDB, or those wanting a no-external-dependency in-cluster limiter (gossip)
  can't use distributed limiting. The `Store` interface (`store/store.go:14-48`) already
  abstracts this cleanly.
- **What exists today:** `Store` interface with Memory + Redis implementations only.
- **Proposed approach:** Add a DynamoDB store (conditional-write atomicity via `UpdateItem`),
  a Memcached store (CAS-based), and an experimental gossip-based approximate limiter
  (`hashicorp/memberlist`) for zero-external-dependency deployments.
- **Risks/tradeoffs:** DynamoDB/Memcached can't run arbitrary Lua — atomicity must be
  re-expressed per backend (conditional writes / CAS loops), so not all algorithms port
  cleanly. Gossip is approximate by nature.
- **References:** AWS DynamoDB conditional writes, `bradfitz/gomemcache`, SWIM/memberlist.

### 5.3 Key TTL/GC audit and hot-key protection
- **Category:** Distributed · **Priority:** P2 · **Effort:** S
- **Rationale:** Distributed keys rely on Redis TTL for GC (e.g. token bucket TTL =
  fill-time, `redis.go:366-373`). Worth auditing that every script sets a TTL on *every*
  create path (sliding-window-log ZSET can grow unbounded if TTL isn't refreshed on each
  add), and adding a max-key guard mirroring the memory store's `maxKeys`
  (`store/memory.go:39-40,102-111`).
- **What exists today:** TTLs are set, but there's no distributed max-key guard and no explicit
  test that a churning key set doesn't leak in Redis.
- **Proposed approach:** Verify/refresh TTL on every write in each script; add an integration
  test that asserts key count stays bounded under key churn; document ZSET growth bounds for
  the sliding-window-log.
- **References:** Redis key eviction policies, the existing `docker-compose.yml` `allkeys-lru`.

### 5.4 First-class fix (or documented guarantee) for the float64/256ns snapping
- **Category:** Distributed · **Priority:** P2 · **Effort:** M
- **Rationale:** Nanosecond TAT/scores (~1.78e18) exceed float64's 2^53 exact-integer ceiling,
  snapping to ~256ns granularity when Redis evaluates them as Lua doubles — carefully documented
  in `scripts_memory.go:242-268` and faithfully emulated. It's correct-but-lossy; a first-class
  fix removes an entire class of edge cases and a documentation footnote.
- **What exists today:** The snapping is documented and the in-memory emulation matches Redis
  exactly (good testing discipline), but the imprecision itself remains.
- **Proposed approach:** Store TAT/scores as split hi/lo integers or as microseconds (fits in
  float64 exactly for centuries) inside the scripts; or use Redis 7+ integer-preserving paths.
  If not fixed, promote the note to a documented **accuracy guarantee** ("≤256ns error").
- **Risks/tradeoffs:** Changing the on-Redis representation is a data-format migration; version
  the key schema.
- **References:** IEEE-754 double precision, Redis Lua number semantics, GCRA writeups.

### 5.5 Consistency-guarantee documentation matrix
- **Category:** Distributed · **Priority:** P2 · **Effort:** S
- **Rationale:** Users must know exactly what "distributed" guarantees under Redis failover,
  cluster resharding, and the fail-open fallback (which silently degrades to **per-instance ×N**
  limits, `store/redis.go:54-64`). This is currently prose in `docs/distributed.md` but not a
  crisp guarantee table.
- **What exists today:** Good narrative docs; no formal matrix of (failure mode → guarantee).
- **Proposed approach:** A table: single-Redis vs cluster vs sentinel, behavior on partition,
  fail-open vs fail-closed per algorithm, and the accuracy bound from §5.4.
- **References:** Jepsen-style guarantee docs, Redis Cluster consistency notes.

### 5.6 Multi-region / active-active strategy note or CRDT counter
- **Category:** Distributed · **Priority:** P3 · **Effort:** L
- **Rationale:** Global limits across regions need either a single authoritative store (latency)
  or approximate CRDT counters (eventual). Neither is addressed.
- **What exists today:** Single-region Redis assumed.
- **Proposed approach:** At minimum a design note; optionally an approximate G-Counter-based
  cross-region limiter with a documented over-admission bound.
- **References:** CRDT counters, DynamoDB Global Tables, Cloudflare multi-region limiting.

---

## 6. Testing & Quality

### 6.1 Property-based testing (rapid/gopter)  
> ✅ **Implemented & merged** (P2)
- **Category:** Testing · **Priority:** P2 · **Effort:** M
- **Rationale:** There is native fuzzing (`ratelimit/tokenbucket/fuzz_test.go`,
  `ratelimit/gcra/fuzz_test.go`) and a chaos harness (`internal/testutil/chaos.go`), but **no
  property-based tests** asserting algebraic invariants (e.g. "over any schedule, admitted ≤
  limit×windows + burst"). Property tests catch classes of bugs fuzzing samples miss.
- **What exists today:** Go fuzz (2 targets), chaos concurrency harness, `ManualClock`
  determinism, leak checker.
- **Proposed approach:** Add `pgregory.net/rapid` tests over the `Limiter` interface asserting
  the admission bound and monotonicity across random operation/time schedules, reusing
  `internal/clock.ManualClock`.
- **Risks/tradeoffs:** Adds a test-only dependency (fine — not in core module runtime).
- **References:** `pgregory.net/rapid`, `leanovate/gopter`, Hypothesis.

### 6.2 Expand fuzzing to all algorithms + the Redis Lua scripts
- **Category:** Testing · **Priority:** P2 · **Effort:** S
- **Rationale:** Only token bucket and GCRA are fuzzed. Fixed window, sliding-window (log &
  counter), leaky bucket, composite, and — critically — the **script emulations vs real Redis**
  parity (`store/scripts_memory.go` vs `store/redis.go`) deserve fuzzing/differential testing.
- **What exists today:** 2 fuzz targets; a `parity_integration_test.go` exists in `store/` but
  isn't fuzz-driven.
- **Proposed approach:** Add fuzz targets for the remaining algorithms and a **differential
  fuzzer** that runs the same random op sequence against the memory emulation and real Redis,
  asserting identical decisions.
- **References:** Go fuzzing, differential testing.

### 6.3 Testcontainers for Redis (deterministic integration)
- **Category:** Testing · **Priority:** P2 · **Effort:** S
- **Rationale:** Integration tests use a **CI service container** (`.github/workflows/ci.yml`
  Redis 7-alpine) and fall back to `localhost:6379`, which makes local runs environment-
  dependent. `testcontainers-go` (or `miniredis` for unit speed) gives hermetic, one-command
  local integration.
- **What exists today:** `//go:build integration` tests need an externally-provided Redis.
- **Proposed approach:** Use `miniredis` for fast unit-level Lua tests and `testcontainers-go`
  for real-Redis integration that spins the container up in-process.
- **Risks/tradeoffs:** testcontainers needs Docker locally; keep the env-var fallback.
- **References:** `testcontainers-go`, `alicebob/miniredis`.

### 6.4 Frontend tests in CI (unit + Playwright e2e)
- **Category:** Testing · **Priority:** **P0** (paired with §2 CI) · **Effort:** S
- **Rationale:** The frontend has a Playwright suite (`frontend/e2e/app.spec.ts`,
  `playwright.config.ts`) but **CI never runs it** — no lint, no `tsc`, no e2e. The playground
  is the project's shop window; a silent regression ships to `main`.
- **What exists today:** 3 Playwright e2e cases (SSR, token exhaustion, CB transitions);
  requires both servers running. No component unit tests. Not in `ci.yml`.
- **Proposed approach:** Add a `frontend` CI job: `npm ci`, `eslint`, `tsc --noEmit`, and
  Playwright e2e (spin the Go demo server + Next dev server, or use Playwright's webServer
  config). Add a few component unit tests (Vitest/RTL) for the vizualizations.
- **Risks/tradeoffs:** e2e needs both servers — use Playwright `webServer` to orchestrate.
- **References:** Playwright CI recipe, Vitest.

### 6.5 Benchmark regression gate in CI (benchstat)  
> ✅ **Implemented & merged** (item 14)
- **Category:** Testing / Performance · **Priority:** P1 · **Effort:** M
- **Rationale:** `make bench-compare OLD=main NEW=HEAD` exists and uses `benchstat`, but **CI
  never runs it** — performance regressions can merge unnoticed, undermining the "0 allocs,
  62–82 ns/op" claims in the CHANGELOG.
- **What exists today:** Local `make bench` / `bench-compare` only.
- **Proposed approach:** A CI job (or nightly, given noise) that runs benches on PR base vs
  head and fails or comments on a regression threshold via `benchstat`. Pin CPU / use a
  dedicated runner to reduce noise.
- **Risks/tradeoffs:** Shared CI runners are noisy — use a delta threshold and `-count` to
  reduce false positives, or run nightly rather than per-PR.
- **References:** `golang.org/x/perf/cmd/benchstat`, `benchmark-action/github-action-benchmark`.

### 6.6 Mutation testing
- **Category:** Testing · **Priority:** P3 · **Effort:** M
- **Rationale:** High test *count* doesn't guarantee assertions actually catch faults. Mutation
  testing measures whether tests fail when logic is perturbed.
- **What exists today:** None.
- **Proposed approach:** Run `gremlins` (go-gremlins) on the core algorithm packages nightly;
  triage surviving mutants.
- **Risks/tradeoffs:** Slow; nightly-only.
- **References:** `go-gremlins/gremlins`, Stryker.

### 6.7 Deterministic simulation / latency injection tests
- **Category:** Testing · **Priority:** P2 · **Effort:** M
- **Rationale:** The chaos harness tests concurrency, and `ManualClock` gives time determinism,
  but there's no **fault/latency injection** into downstreams to validate breaker+retry+hedge
  interaction under realistic failure schedules.
- **What exists today:** `internal/testutil/chaos.go`, `internal/clock` — but no fault-injection
  wrapper for the callee.
- **Proposed approach:** A `testutil.FaultyFunc` that injects configurable latency/error rates
  driven by `ManualClock`, and scenario tests asserting pipeline behavior (e.g. hedge fires,
  breaker opens, budget stops retries).
- **References:** FoundationDB deterministic simulation, toxiproxy.

---

## 7. Security & Hardening

### 7.1 `govulncheck` in CI  
> ✅ **Implemented & merged** (item 2)
- **Category:** Security · **Priority:** **P0** · **Effort:** S
- **Rationale:** No dependency vulnerability scanning exists anywhere. The library pulls
  Redis/gRPC/Prometheus transitively (`go.mod`); a CVE in any of them ships silently.
  `govulncheck` is the Go-native, low-false-positive answer.
- **What exists today:** Dependabot (`.github/dependabot.yml`) updates deps but does not scan
  for exploitable calls; no `govulncheck`.
- **Proposed approach:** Add a `govulncheck ./...` CI step (and to the Makefile). Optionally
  `osv-scanner` for the frontend `npm` tree.
- **References:** `golang.org/x/vuln/cmd/govulncheck`, OSV-Scanner.

### 7.2 Supply-chain: SBOM + cosign signing + SLSA provenance  
> ✅ **Implemented & merged** (item 12)
- **Category:** Security / Release · **Priority:** P1 · **Effort:** M
- **Rationale:** `release.yml` builds raw binaries with no SBOM, no signatures, no provenance.
  Regulated/enterprise adopters increasingly require these. It's a differentiator for a
  "production-grade" claim.
- **What exists today:** `release.yml` does a matrix `go build` + `softprops/action-gh-release`;
  no signing/attestation/SBOM.
- **Proposed approach:** Generate SBOMs (`anchore/syft` / cyclonedx), sign artifacts and the
  Docker image with `cosign` (keyless/OIDC), and emit SLSA provenance
  (`slsa-framework/slsa-github-generator`). goreleaser (§9.1) does most of this natively.
- **References:** SLSA framework, sigstore/cosign, syft, goreleaser signing.

### 7.3 Fill the SECURITY.md contact placeholder
- **Category:** Security · **Priority:** P1 · **Effort:** S
- **Rationale:** `SECURITY.md` has a `[PLACEHOLDER]` security-contact email. A disclosure
  policy that can't receive reports is worse than none.
- **What exists today:** Good policy text (3-day ack, coordinated 90-day disclosure) but no
  real contact.
- **Proposed approach:** Add a real reporting channel (private email or GitHub Security
  Advisories) and enable GitHub private vulnerability reporting.
- **References:** GitHub Security Advisories.

### 7.4 Demo server self-protection & DoS surface
- **Category:** Security · **Priority:** P2 · **Effort:** M
- **Rationale:** The demo server has solid basics — constant-time API-key compare
  (`server/api/middleware.go:239-255`), 1 MiB body cap (`middleware.go:218-229`), key
  validation blocking header injection (`server/api/security.go:23-41`), security headers,
  panic recovery — but **the server does not rate-limit itself**. `/metrics`, `/health`, and
  `/simulate` are unauthenticated DoS surfaces, and the simulator can be driven to exhaust
  resources.
- **What exists today:** Auth is *optional*; no global/self rate limit; no per-endpoint quotas.
- **Proposed approach:** Apply the library's own rate limiter + bulkhead to the server
  (dogfooding), bound the simulator's parameters, and add request timeouts. Document that the
  demo is not a hardened public service.
- **References:** OWASP API Security Top 10, the library's own `bulkhead`/`ratelimit`.

### 7.5 Fuzz the server's JSON decoders & the simulator
- **Category:** Security · **Priority:** P3 · **Effort:** S
- **Rationale:** Request bodies are JSON-decoded but not schema-fuzzed. The simulate handler
  (`server/api/simulate_handler.go`) takes numeric params that could be adversarial (huge N,
  negative rates).
- **What exists today:** Zod validation on the frontend, Go-side decoding + key validation, but
  no server-side fuzzing of decoders.
- **Proposed approach:** Add `Fuzz` targets for the handler decoders; clamp/validate simulator
  bounds server-side.
- **References:** Go fuzzing, defensive input validation.

### 7.6 Enable security/quality linters (gosec, revive, depguard)  
> ✅ **Implemented & merged** (P2)
- **Category:** Security / Quality · **Priority:** P2 · **Effort:** S
- **Rationale:** `.golangci.yml` enables only six linters (`errcheck, govet, ineffassign,
  misspell, staticcheck, unused`) with `default: none`. No `gosec` (security), no `depguard`
  (to enforce the zero-dep rule as lint, not just a Make script), no `revive`/`prealloc`.
- **What exists today:** Minimal linter set; zero-dep enforced by `make verify-deps` shell logic.
- **Proposed approach:** Add `gosec`, `depguard` (codify the import allow-list), and selectively
  `prealloc`/`unconvert`/`revive`. Keep the noise low with targeted config.
- **References:** securego/gosec, golangci-lint depguard.

---

## 8. DX & Docs

### 8.1 Migration guides (from `x/time/rate` and `gobreaker`)  
> ✅ **Implemented & merged** (item 15)
- **Category:** Docs · **Priority:** P1 · **Effort:** M
- **Rationale:** Adoption is a switching cost. The two libraries people migrate *from* are
  `golang.org/x/time/rate` (rate limiting) and `sony/gobreaker` (breaker). A side-by-side
  "here's your code, here's the equivalent" guide removes the biggest barrier.
- **What exists today:** README has a feature matrix but **no comparison vs `x/time/rate`/
  `gobreaker`** and no migration section; `docs/comparison.md` compares algorithms, not libraries.
- **Proposed approach:** `docs/migration.md` with before/after snippets and an API-mapping table
  (e.g. `rate.NewLimiter(r, b)` → `tokenbucket.New(b, float64(r))`; `gobreaker.NewCircuitBreaker`
  → `circuitbreaker.New(Config{...})`).
- **References:** how ent/gorm/zap document migrations.

### 8.2 Recipe cookbook (framework + scenario recipes)  
> ✅ **Implemented & merged** (item 15)
- **Category:** Docs / DX · **Priority:** P1 · **Effort:** M
- **Rationale:** Users copy recipes, not APIs. "Rate limit per IP in Gin", "per-tenant quotas",
  "protect a flaky downstream with breaker+retry+hedge", "distributed limit with Redis
  fail-open" — these drive adoption.
- **What exists today:** Four runnable `examples/` (http, grpc, distributed, pipeline) — good but
  not framework-specific and not scenario-organized.
- **Proposed approach:** `docs/cookbook/` (or `/recipes`) with copy-pasteable snippets, one per
  scenario, cross-linked from the README. Pairs naturally with §2.4 framework middleware.
- **References:** Polly docs, resilience4j getting-started recipes.

### 8.3 Hosted docs site
- **Category:** Docs · **Priority:** P2 · **Effort:** M
- **Rationale:** Docs are markdown-in-repo (`docs/algorithms.md`, `comparison.md`,
  `distributed.md`) plus in-playground pages. A searchable hosted site (with the algorithm
  visualizations embedded) is far more discoverable and looks professional.
- **What exists today:** Repo markdown + Next.js `/docs` route reading `frontend/content/docs`.
- **Proposed approach:** Publish the existing `frontend/content/docs` as a real docs site (the
  Next app already renders them) on Vercel/GitHub Pages, or use Docusaurus/Starlight. Embed the
  live playground visualizations.
- **References:** Starlight, Docusaurus.

### 8.4 Published head-to-head benchmarks
- **Category:** Docs / Community · **Priority:** P1 · **Effort:** M
- **Rationale:** The CHANGELOG cites internal ns/op numbers, but there are **no published
  comparisons** vs `x/time/rate`, `uber/ratelimit`, `gobreaker`. Benchmarks are the most
  persuasive adoption artifact for a performance-sensitive library.
- **What exists today:** Internal `*_bench_test.go`; no comparative benchmark harness or
  published results.
- **Proposed approach:** A `benchmarks/` module that benches this library against the
  incumbents on identical workloads, with a `benchstat` table checked into the README.
- **Risks/tradeoffs:** Benchmarks invite "cherry-picking" accusations — publish the harness and
  methodology.
- **References:** `x/perf`, published Go library benchmark comparisons.

### 8.5 API stability policy & versioned guarantees
- **Category:** Docs · **Priority:** P2 · **Effort:** S
- **Rationale:** Pre-1.0 SemVer is noted, but there's no statement of *which* surface is stable
  vs experimental (e.g. `Limiter` interface stable; `internal/` and adaptive experimental). This
  matters for anyone building on top.
- **What exists today:** SemVer note in README/CHANGELOG; no stability tiers.
- **Proposed approach:** A `STABILITY.md` (or README section) marking each package as
  stable/beta/experimental, with a deprecation policy.
- **References:** Go module compatibility guidelines, k8s API stability tiers.

### 8.6 Architecture Decision Records (ADRs)
- **Category:** Docs · **Priority:** P3 · **Effort:** S
- **Rationale:** Key decisions (fixed pipeline order `pipeline.go:6-20`, fail-open Redis,
  panic-on-bad-input, zero-dep core, client-time in Lua) are captured only in code comments and
  audit docs. ADRs make the *why* durable and reviewable.
- **What exists today:** `SPEC.md`, `AUDIT_REPORT.md` — comprehensive but not ADR-structured.
- **Proposed approach:** `docs/adr/` with short numbered ADRs for the load-bearing decisions.
- **References:** Michael Nygard ADR template.

### 8.7 `doc.go` package overviews & pkg.go.dev polish
- **Category:** Docs · **Priority:** P3 · **Effort:** S
- **Rationale:** Package docs and `example_test.go` are strong (18 example files), but there are
  no dedicated `doc.go` overview files, which give pkg.go.dev a cleaner package landing page for
  multi-file packages.
- **What exists today:** Package comment on the primary file per package; no `doc.go`.
- **Proposed approach:** Add `doc.go` to the larger packages with a short overview + link to
  examples.
- **References:** pkg.go.dev rendering conventions.

---

## 9. Operability & Release

### 9.1 Adopt goreleaser  
> ✅ **Implemented & merged** (item 13)
- **Category:** Release · **Priority:** P1 · **Effort:** M
- **Rationale:** `release.yml` hand-rolls a 4-platform build matrix + `gh-release`. goreleaser
  gives multi-arch binaries, checksums, SBOMs, cosign signing, Docker manifests, Homebrew taps,
  and changelog generation from one config — replacing most of §7.2, §9.2, §9.3, §9.5 at once.
- **What exists today:** Manual matrix (`linux/darwin` × `amd64/arm64`), symbol-stripped, version
  injected; no checksums/signing/Docker push.
- **Proposed approach:** `.goreleaser.yaml` producing signed multi-arch archives + checksums +
  SBOM, plus Docker manifest push and a Homebrew tap.
- **References:** goreleaser docs.

### 9.2 Multi-arch Docker image build + registry push  
> ✅ **Implemented & merged** (item 13)
- **Category:** Release · **Priority:** P1 · **Effort:** S
- **Rationale:** The `Dockerfile` is already well-hardened (distroless nonroot, `CGO_ENABLED=0`,
  healthcheck) but **no CI publishes it** — release ships binaries only. Users can't `docker
  pull`.
- **What exists today:** Good Dockerfile + `frontend/Dockerfile.frontend`; `docker-compose.yml`;
  no image publication in `release.yml`.
- **Proposed approach:** `docker buildx` multi-arch (amd64/arm64) push to GHCR on release; sign
  with cosign (§7.2).
- **References:** docker buildx, GHCR.

### 9.3 Helm chart  
> ✅ **Implemented & merged** (item 13)
- **Category:** Release · **Priority:** P2 · **Effort:** M
- **Rationale:** `deploy/kubernetes/` has raw manifests (deployment/service/hpa/pdb/configmap,
  well-hardened: nonroot, read-only rootfs, dropped caps) but **no Helm chart**, so it isn't
  parameterizable or versioned for real cluster adoption.
- **What exists today:** Static K8s YAML.
- **Proposed approach:** A `charts/resilience-demo` Helm chart wrapping the existing manifests
  with values for image tag, replicas, resources, Redis, and Prometheus scraping.
- **References:** Helm best practices.

### 9.4 CHANGELOG automation
- **Category:** Release · **Priority:** P2 · **Effort:** S
- **Rationale:** `CHANGELOG.md` (Keep-a-Changelog format) is manually curated. Automating it
  from Conventional Commits removes toil and drift.
- **What exists today:** Manual CHANGELOG; `CONTRIBUTING.md` mentions commit conventions.
- **Proposed approach:** git-cliff / release-please / goreleaser changelog from Conventional
  Commits, with a CI check that PRs update the changelog.
- **References:** git-cliff, release-please.

### 9.5 Homebrew tap (and `go install` one-liner) for the demo server
- **Category:** Release · **Priority:** P3 · **Effort:** S
- **Rationale:** The demo server is a useful standalone tool; a `brew install` / documented
  `go install …/server@latest` lowers the try-it barrier.
- **What exists today:** Binaries attached to releases only.
- **Proposed approach:** goreleaser Homebrew tap + a documented `go install` path.
- **References:** goreleaser brew.

### 9.6 Prometheus alerting & recording rules
- **Category:** Operability · **Priority:** P3 · **Effort:** S
- **Rationale:** `deploy/prometheus/prometheus.yml` scrapes but ships **no alert rules** (e.g.
  "breaker open", "denial rate spike") and no recording rules. Dashboards without alerts are
  passive.
- **What exists today:** Scrape config only.
- **Proposed approach:** Ship example alert rules (breaker-open, high-denial, latency SLO burn)
  and recording rules; depends on §4.1/§4.3 emitting the metrics.
- **References:** Prometheus alerting, SRE burn-rate alerts.

---

## 10. Frontend / Demo

### 10.1 Frontend accessibility pass
- **Category:** Frontend · **Priority:** P2 · **Effort:** M
- **Rationale:** The playground has only two `aria-label`s (on `TokenBucketViz` and
  `StateMachineViz`); no ARIA live regions for streaming updates, no keyboard navigation, no
  chart alt-text/table fallbacks. A public demo should be accessible.
- **What exists today:** React 19 / Next 16 / Tailwind v4 / Radix UI (accessible primitives are
  available but under-used); 2 aria-labels total.
- **Proposed approach:** Add ARIA live regions for the WS-driven metric tiles, keyboard controls
  for the simulator, `role`/labels on SVG visualizations, and axe-core checks in Playwright.
- **References:** WCAG 2.2, `@axe-core/playwright`.

### 10.2 Richer WS-driven charts & missing visualizations
- **Category:** Frontend · **Priority:** P3 · **Effort:** M
- **Rationale:** WS streaming works (`frontend/lib/ws/manager.ts` with reconnect/backoff; store
  keeps 200-item history), but visualization coverage is uneven — token bucket has an animated
  SVG, but GCRA/sliding-window/leaky-bucket/adaptive lack equivalent algorithm-specific
  animations, and there's no live latency-histogram or breaker-window heatmap.
- **What exists today:** `TokenBucketViz`, `StateMachineViz`, Recharts, a compare page.
- **Proposed approach:** Per-algorithm visualizations (GCRA TAT timeline, sliding-window ring,
  leaky-bucket drain), a live latency histogram, and a breaker rolling-window heatmap fed by
  the WS stream.
- **References:** the existing Recharts setup, observable-style algorithm animations.

### 10.3 Split the demo server from the library (repo/module hygiene)
- **Category:** Frontend / DX · **Priority:** P2 · **Effort:** M
- **Rationale:** The demo server (`server/`) and frontend live in the library repo. While the
  frontend is already separable (talks over REST/WS) and the core enforces zero deps, the
  `server/` module pulls gorilla/websocket, prometheus, etc. into the same go.mod, muddying the
  "zero-dep core" story on pkg.go.dev.
- **What exists today:** One module; `verify-zero-deps` keeps *core packages* clean, but `server/`
  deps are in the same `go.mod`.
- **Proposed approach:** Move `server/` (and maybe `examples/`) into a nested `demo/` module (own
  go.mod) or a sibling repo, so the library's dependency graph is provably minimal. Keep the
  frontend as-is (already decoupled).
- **Risks/tradeoffs:** Multi-module repos add release complexity; weigh against the marketing
  value of a truly dependency-free library module.
- **References:** Go multi-module repos, k8s `staging/` pattern.

### 10.4 Frontend Vercel deploy + preview environments
- **Category:** Frontend / Release · **Priority:** P3 · **Effort:** S
- **Rationale:** `next.config.ts` uses `output: "standalone"` (Docker-ready) but there's **no
  `vercel.json`** and no deployed public playground — a live demo is a huge adoption driver.
- **What exists today:** Standalone Docker build; `.env.example` with `NEXT_PUBLIC_API_URL`.
- **Proposed approach:** Deploy the playground to Vercel with the demo server on a small host
  (or mock the API for a static demo); add PR preview deployments.
- **References:** Vercel Next.js deploys.

### 10.5 Frontend component unit tests
- **Category:** Frontend / Testing · **Priority:** P3 · **Effort:** S
- **Rationale:** Only 3 Playwright e2e cases exist; no component-level tests for the store
  reducers, WS manager reconnect logic, or visualizations.
- **What exists today:** `frontend/e2e/app.spec.ts` only.
- **Proposed approach:** Vitest + React Testing Library for the Zustand store, `usePoll`/
  `useWebSocket` hooks, and the reconnect/backoff logic in `lib/ws/manager.ts`.
- **References:** Vitest, RTL.

---

## 11. Community & Adoption

### 11.1 Library-vs-alternatives comparison table in the README
- **Category:** Community · **Priority:** P1 · **Effort:** S
- **Rationale:** The README compares *algorithms* internally but never positions the library vs
  `x/time/rate` / `uber/ratelimit` / `gobreaker` / `slok/goresilience` / `resilience4j`. New
  users decide in the first 30 seconds whether this beats what they know.
- **What exists today:** Internal feature matrix (README) and `docs/comparison.md` (algorithm
  comparison) — but no library-level positioning table.
- **Proposed approach:** A concise "Why this over X" table (distributed? zero-dep? breaker+RL+
  pipeline in one? adaptive? observability?) high in the README.
- **References:** how zap/zerolog position vs each other.

### 11.2 Good-first-issue set & contributor on-ramp
- **Category:** Community · **Priority:** P2 · **Effort:** S
- **Rationale:** `CONTRIBUTING.md`, issue templates, and PR template exist, but there's no
  curated `good first issue` backlog. Many items in this roadmap (framework middleware §2.4,
  recipes §8.2, debounce/throttle §1.9, benchmark additions §3.5) are ideal first issues.
- **What exists today:** Templates and contribution docs; no labeled starter issues.
- **Proposed approach:** Seed 10–15 `good-first-issue` tickets from this roadmap; enable GitHub
  Discussions.
- **References:** up-for-grabs.net, CNCF contributor ladders.

### 11.3 Blog/talk material & an examples showcase
- **Category:** Community · **Priority:** P3 · **Effort:** M
- **Rationale:** The GCRA float64 note (§5.4), the fixed pipeline-order design, the deterministic
  `ManualClock`, and the 59-issue adversarial audit are genuinely interesting write-ups that
  build credibility and inbound interest.
- **What exists today:** Rich internal audit docs (`ADVERSARIAL_AUDIT.md`, `SPEC.md`) that could
  become public posts.
- **Proposed approach:** A short blog series (design of the pipeline, distributed atomicity with
  Lua, testing with a fake clock) and a conference-talk outline; link from the README.
- **References:** the project's own audit trail.

### 11.4 Dedicated examples repo / runnable playground links
- **Category:** Community · **Priority:** P3 · **Effort:** S
- **Rationale:** `examples/` is in-repo; a discoverable set of runnable examples (Go
  playground links where feasible, plus the framework recipes) lowers the trial barrier.
- **What exists today:** `examples/{http,grpc,distributed,pipeline}`.
- **Proposed approach:** Cross-link examples from the README table, add a "run it" section, and
  consider Go-playground-compatible snippets for the local (non-Redis) algorithms.
- **References:** gobyexample-style layouts.

---

## Appendix A: What Already Exists (Strengths)

To keep the roadmap honest, these are already strong and should **not** be "enhanced" away:

- **Eight algorithms behind one interface** (`ratelimit/limiter.go:23`) with consistent
  `Allow/AllowN/Wait/WaitN/Peek/Reset/Close`.
- **Atomic distributed backends** for 5 algorithms via Lua (`ratelimit/store/redis.go:351-541`),
  with **in-memory emulations** that match Redis semantics exactly for testing
  (`store/scripts_memory.go`).
- **Deterministic time** via `internal/clock.ManualClock` (lock-order-safe timers/tickers).
- **Fail-open Redis fallback** with an explicit tradeoff note (`store/redis.go:54-64`).
- **Rich circuit-breaker** with count- and time-based windows, CAS-guarded half-open probing
  (`circuitbreaker.go:205-214`), and full callbacks (`config.go:73-83`).
- **AWS-style backoff suite** (constant/exponential/full/equal/decorrelated jitter,
  `retry/backoff/`).
- **Hedging** including N-way with goroutine-leak-safe draining (`fallback/fallback.go:158-253`).
- **Fixed, correct pipeline order** with stable sort regardless of builder call order
  (`pipeline/pipeline.go:6-20,175-188`).
- **Hardened demo/deploy**: distroless nonroot Dockerfile, K8s manifests with read-only rootfs +
  dropped caps, constant-time API-key compare, header-injection-safe key validation.
- **Strong test culture**: fuzzing, chaos harness, leak checker, `-race -count=3`, integration
  tests with a real Redis, 18 runnable `example_test.go`.
- **Exceptional audit trail**: `SPEC.md`, `ADVERSARIAL_AUDIT.md`, `ISSUES.md`, `AUDIT_REPORT.md`.

The roadmap builds *on top of* these; it does not relitigate them.

---

## Appendix B: Enhancement Count by Category & Priority

**Total enhancements: 72**

| # | Category | P0 | P1 | P2 | P3 | Total |
|---|----------|----|----|----|----|-------|
| 1 | Missing Algorithms & Patterns | 0 | 4 | 5 | 3 | 12 |
| 2 | API & Ergonomics | 0 | 2 | 4 | 2 | 8 |
| 3 | Performance | 0 | 0 | 2 | 3 | 5 |
| 4 | Observability | 3 | 1 | 2 | 1 | 7 |
| 5 | Distributed Correctness | 0 | 1 | 4 | 1 | 6 |
| 6 | Testing & Quality | 1 | 1 | 4 | 1 | 7 |
| 7 | Security & Hardening | 1 | 2 | 2 | 1 | 6 |
| 8 | DX & Docs | 0 | 3 | 2 | 2 | 7 |
| 9 | Operability & Release | 0 | 2 | 2 | 2 | 6 |
| 10 | Frontend / Demo | 0 | 0 | 2 | 3 | 5 |
| 11 | Community & Adoption | 0 | 1 | 1 | 2 | 4 |
| **Total** | | **5** | **17** | **30** | **20** | **72** |

**Priority totals:** P0 = 5 · P1 = 17 · P2 = 30 · P3 = 20 → **72**.

**Suggested sequencing:**
1. **Sprint 1 (P0 credibility):** §4.1 real metrics, §4.2 real OTel, §4.3 fix dashboards,
   §6.4 frontend CI, §7.1 govulncheck. Closes the "production-grade but unobservable/untested-
   frontend" gap.
2. **Sprint 2 (P1 competitiveness):** §1.3 retry budget, §1.5 cost/weight, §2.1 RL hooks,
   §2.4 framework middleware, §5.1 clock skew, §8.1 migration guides, §11.1 comparison table.
3. **Sprint 3 (P1 flagship + release):** §1.1 concurrency limiter, §1.11 load shedder,
   §7.2 supply-chain, §9.1 goreleaser, §6.5 benchmark gate, §8.4 published benchmarks.
4. **Backlog (P2/P3):** the remainder, seeded as `good-first-issue` (§11.2).
