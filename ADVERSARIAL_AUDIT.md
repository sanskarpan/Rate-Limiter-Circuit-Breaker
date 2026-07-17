# Adversarial Codebase Audit — Rate Limiter + Circuit Breaker

**Date:** 2026-07-16
**Method:** 7 parallel adversarial auditors, one per domain, each verifying against `SPEC.md` invariants rather than trusting code comments or the prior `AUDIT_REPORT.md`. Key-severity claims spot-verified by hand.
**Ground truth:** `go build ./...` succeeds. `go vet ./...` is clean. `go test -race ./...` **passes on every package.** Every defect below survives a green test suite — the tests systematically avoid the concurrency and edge-case paths where the bugs live. This is the central theme: **passing tests here are false confidence.**

**Meta-finding:** The existing `AUDIT_REPORT.md` concludes "production-ready." That conclusion is overstated. Its security/test-existence claims are largely truthful, but its headline "atomic" fix (CRIT-1) is itself buggy under concurrency (see H-3 / AUDIT-1), and the frontend↔backend contract is broken in three independent places it never caught.

Tallies: **6 Critical · 20 High · ~18 Medium · ~15 Low.** IDs in brackets map to the per-domain auditor findings.

---

## CRITICAL — unauthenticated or silent correctness failure, broad blast radius

### C-1 · Security · No authentication on any server route [SRV-1]
**Location:** `server/api/router.go:60-77`
The actual middleware chain is `Recovery → SecurityHeaders → LimitRequestBody → RequestID → Logger → CORS → mux`. There is **no auth middleware anywhere in the codebase** — none defined, none wired. State-mutating endpoints (`POST /api/v1/cb/{name}/force-open`, `force-close`, `execute`, `/api/v1/simulate/*`) are reachable by any unauthenticated caller.
**Reproduction:** `curl -X POST http://host/api/v1/cb/primary/force-open` trips the "primary" breaker → denial of the protected dependency for all users.
**Fix:** Add an API-key/bearer middleware using `crypto/subtle.ConstantTimeCompare`; register it in `NewRouter`; at minimum gate the `force-*`/`execute`/`simulate` routes.

### C-2 · Security · `/metrics` exposed unauthenticated [SRV-2]
**Location:** `server/api/router.go:65`
`GET /metrics` serves `promhttp.Handler()` with no auth or IP allow-list. Fix: bind to a separate internal listener or require auth.

### C-3 · Race/Logic · Circuit-breaker probe counter leaks on panic → circuit wedged forever [CB-1]
**Location:** `circuitbreaker/circuitbreaker.go:73-97`
`Execute` calls `err = fn(ctx)` then `afterExecute(...)` with **no `defer`**. If `fn` panics, `afterExecute` never runs, so `halfOpenInflight` (incremented in `beforeExecute`) is never decremented. Once it reaches `HalfOpenMaxRequests`, every future probe returns `ErrTooManyRequests` permanently — the breaker is stuck HalfOpen and can never recover.
**Reproduction (verified by auditor):** trip Open → advance past `OpenTimeout` → run one probe whose `fn` panics (recovered upstream) → next legitimate probe returns `ErrTooManyRequests`, state stays half-open forever.
**Fix:** `defer` the outcome handling / counter release so a panic is converted to a failure and the slot is always freed.

### C-4 · Data · Memory store `maxKeys` counter never decremented → self-inflicted DoS [STORE-1]
**Location:** `ratelimit/store/memory.go` — increments at 100/129/193; **no** decrement in `Del` (234), `cleanup` (264), or lazy expiry in `Get` (88). (The `Add(-1)` calls at 102/131/195 are create-check rollbacks, not deletions.)
With `WithMaxKeys(N)`, a store that churns keys climbs monotonically to N and then **rejects all new keys forever** despite holding zero live keys.
**Reproduction (verified):** `NewMemory(WithMaxKeys(3))`; loop 10×{Set unique key; Del it} → 4th distinct key rejected though 0 live keys.
**Fix:** Decrement `keyCount` on every removal path (Del, lazy expiry, cleanup GC).

### C-5 · Race/Logic · Composite AND mode leaks tokens / double-consumes under concurrency [COMP-1]
**Location:** `ratelimit/composite/composite.go:103-115`
Package godoc claims AND mode is leak-free ("if A allows but B denies, A's token is NOT consumed"). True only serially. Under concurrency many goroutines pass the phase-1 `Peek` gate, all consume `limiter[0]`, but only the bottleneck's capacity wins `limiter[1]`; the rest are denied overall yet permanently drained `limiter[0]`.
**Reproduction (verified):** 3000 concurrent `AllowN(1)`, A=cap100000 / B=cap50 → overall allowed=50 but A consumed **723** → 673 tokens leaked.
**Fix:** Make phase-2 atomic (refund consumed limiters on any downstream deny, or 2-phase reservation). The Limiter interface currently lacks a Release primitive — add one. At minimum, delete the false "no token loss" guarantee.

### C-6 · Logic · Adaptive limiter can never recover upward for limits < 34 (one-way ratchet) [AD-1]
**Location:** `ratelimit/adaptive/adaptive.go:244`
`newLimit := int(float64(current)*0.7 + target*0.3)` with `target=current*1.1` → `int(current*1.03)`. Integer truncation makes this equal `current` for all `current ≤ 33` (the mild-healthy branch is a no-op up to 66), and the `if newLimit == current { return }` guard turns it into a permanent no-op. Decreases round down and always move. Net: any adaptive limiter driven to a small limit under stress can **never increase again**.
**Fix:** `math.Round` (or enforce a ±1 minimum step in the adjustment direction).

---

## HIGH — over-admits, data races, resource leaks, broken contracts

### Distributed limiters (all over-admit; all effectively untested — see H-20)
- **H-1 · [SWL-D1]** `slidingwindow/distributed_log.go:45-53` + `store/redis.go:374,384`: `AllowN` ignores `n` — Lua script denies on `count >= limit` (not `count+n`) and does a single `ZADD`. `AllowN(key,5)` consumes 1 slot → 5× over-admit.
- **H-2 · [SWL-D2]** `distributed_log.go:49`: ZSET member `"<ns>-<n>"` collides for same-nanosecond same-n requests; `ZADD` updates instead of inserting → `ZCARD` under-counts → over-admit under burst. Fix: unique suffix per request.
- **H-3 · [SWC-D1]** `slidingwindow/distributed_counter.go:64-104`: check-then-`IncrBy` is **non-atomic** (spec mandates Lua). Concurrent callers all read the same count, all pass, all increment → admits ≫ limit.
- **H-4 · [FW-D1]** `fixedwindow/distributed.go:69`: `AllowN(n>1)` does `IncrBy(n)` then denies on `count>limit` with **no rollback** → the rejected batch permanently poisons the window; all later requests in the window are denied though capacity was never consumed (in-window DoS).
- **H-5 · [FW-D2 / STORE-6]** `store/redis.go:222-237`: Redis `IncrBy` issues `EXPIRE` on **every** call, contradicting the `Store` interface contract ("TTL only on creation") which the memory store honors. Fixed/sliding windows get a sliding TTL and effectively never reset; memory vs Redis backends diverge for identical inputs. Fix: only `EXPIRE` when the INCR created the key (returned value == delta).

### In-memory limiter correctness
- **H-6 · [LB-1 / AUDIT-1]** `ratelimit/leakybucket/leakybucket.go:171-193` **(cross-confirmed by 2 auditors)**: `AllowN` releases `q.mu` before the enqueue loop, so it is **not atomic**. On partial enqueue it returns Denied but leaves the already-pushed tokens in `q.ch` (the "drain the already-queued" comment describes code that doesn't exist) — a denied `AllowN` still consumes 1..n-1 slots. Also the `break` at :179 breaks the `select`, not the `for`. This is the prior audit's "CRIT-1 fixed/atomic" claim — the fix is itself buggy. Fix: hold `q.mu` across check+enqueue.
- **H-7 · [TB-1 / GCRA-1]** `tokenbucket/distributed.go:47` & `gcra/distributed.go:46`: distributed `AllowN`/`WaitN` skip the `ValidateKey`/`ValidateN`(/`n>burst`) guards their in-memory twins enforce. `n=0` → script trivially "allows" and consumes nothing (silent allow); `n<0` refunds tokens; unvalidated keys carry `\r\n`/null-byte injection into Redis.

### Circuit breaker (the "critical" probe-limit invariant is broken)
- **H-8 · [CB-2]** `circuitbreaker.go:166-187` vs `190-223`: `beforeExecute` and `afterExecute` each independently re-read `State()` to decide whether to inc/dec `halfOpenInflight`. The two reads (with `fn` between) can disagree → negative counter (lets > `HalfOpenMaxRequests` probes through) or leaked slot (wedges circuit). Fix: record "acquired a slot" in a local bool, decrement iff true.
- **H-9 · [CB-3]** `circuitbreaker.go:321/336/353`: on transition, `halfOpenInflight.Store(0)` while other probes (with `HalfOpenMaxRequests>1`) are still in flight; their later `Add(-1)` drives the counter negative, defeating the probe limit on the next cycle.
- **H-10 · [CB-4]** `metrics.go:95-146`: time-based window evicts on `now.After(oldest.start + numBuckets*bucketWidth)`, so entries persist for `windowDuration + bucketWidth`. Verified: 5 failures at t=0 with a 3s window still count at exactly t=3s, clearing only at t>4s → breaker trips on failures older than the configured window.

### Adaptive limiter (diverges from spec, destroys state)
- **H-11 · [AD-2]** `adaptive.go:225-268`: implementation is a weighted-score model with thresholds/steps/smoothing **entirely different** from the spec's threshold rules. Consequence: error-rate alone can never trigger a decrease (needs score>0.6, unreachable from ErrorRate at cpu=p99=0). Spec and code are different limiters.
- **H-12 · [AD-3]** `adaptive.go:257-267`: every adjustment constructs a **brand-new** `tokenbucket.New(...)` and closes the old one, discarding all per-key token state — every client gets a fresh full burst on every adjust, precisely when the system is stressed and adjusting often. In-flight `Wait` callers block on a bucket closed out from under them. Fix: add `TokenBucket.Resize` and mutate in place.

### Concurrency / data races (not caught by `-race` because tests are single-goroutine here)
- **H-13 · [BACKOFF-1]** `retry/backoff/jitter.go:29,57` & `decorrelated.go:53`: `*math/rand.Rand` is not safe for concurrent use, yet a backoff instance lives in a shared `*retry.Policy`. Two goroutines running the same policy race on rand state. Verified: 8 goroutines → DATA RACE every run. Fix: mutex-guard `Next`, or use `math/rand/v2`.
- **H-14 · [BACKOFF-2]** `decorrelated.go:47,59`: `d.prev` read/written with no lock — same shared-policy race, corrupts the feedback chain.
- **H-15 · [CLK-1]** `internal/clock/clock.go:230-272` vs `132-140`: lock-order inversion — `Advance` takes `c.mu`→`tick.mu`; `manualTicker.Reset` takes `tick.mu`→`c.mu`. Concurrent `Reset`+`Advance` can deadlock. Fix: drop `c.mu` before `tick.mu` in Advance's ticker path (mirror the timer path).

### Resource leaks / liveness
- **H-16 · [TPOOL-1]** `bulkhead/threadpool.go:89-99`: `Close` never closes `queue`; a `Submit` after Close with buffer space returns `(ch, nil)` but no worker consumes it → caller blocks forever on `<-ch`. The `!ok` branches in `run` are dead code. Fix: track closed state in Submit, return `ErrClosed`.
- **H-17 · [SRV-4]** `server/api/hub.go:42-52` + `websocket.go:105-108`: on hub `Stop()`, clients are removed and `conn.Close()`d but `c.send` is **not** closed → each connection's `writePump` (`range c.send`) blocks forever. Goroutine leak per connection on every shutdown. Fix: close each `c.send` in the `done` branch.
- **H-18 · [FB-1]** `fallback/fallback.go:186-220`: `HedgeN` counts only *outstanding* attempts; if an attempt fails faster than `hedgeDelay`, `fired` hits 0 and it returns the failure **without firing the remaining budget**. `HedgeN(ctx,5ms,4,fn)` with attempt 1 failing in 3ms makes exactly 1 call. Speculative execution collapses to a single try whenever failures are fast.

### Broken frontend↔backend contract
- **H-19 · [SRV-6 / FE-1]** `server/api/hub.go` (`Broadcast` never called anywhere) + `frontend/hooks/useWebSocket.ts:25-41`: the three `/ws/...` endpoints send only a `{"type":"connected"}` welcome and then stream **nothing**. The frontend subscribes to `rate_limit_result|cb_state_change|sim_result|sim_stats` that the server never emits. The advertised real-time streaming is dead end-to-end; the UI updates only via polling, while each dead WS connection holds a goroutine pair + 64-slot buffer.
- **H-20 · [FE-2]** `frontend/lib/api/client.ts:36-45`: `executeCB` POSTs `{simulate:'success'|'failure'|'timeout'}` but the handler decodes `{simulate_failure bool, latency_ms int}` (`circuitbreaker_handlers.go:14-19`). The `simulate` field is dropped → **every** execute is treated as success; the breaker never trips from the UI. Response is read as `{cb_state}` but server returns `{state,executed,snapshot}` → state column always `undefined`.

### Test-infrastructure gap that hides all distributed bugs
- **H-21 · [T-1 / TB-5]** The in-memory store registers **no handler** for `SlidingWindowLogScript` (and distributed tests are `//go:build integration` + `t.Skip` without live Redis), so `DistributedSlidingWindowLog`, distributed counter, and distributed fixed-window have **zero CI coverage**. This is why H-1…H-5 shipped.

---

## MEDIUM

- **M-1 · [COMP-2]** `composite.go:258-262`: `computeRetryAfter` is a stub returning `0` unconditionally → AND-mode deny always reports `RetryAfter=0`, and `WaitN` falls to a 1ms busy-retry loop instead of backing off. Spec requires longest RetryAfter.
- **M-2 · [SWC-D2]** `distributed_counter.go:89`: `math.Ceil(estimated)+n > limit` rounds the approximation up → denies earlier than the in-memory counter; behavior diverges across backends.
- **M-3 · [SWC-D3]** `distributed_counter.go:104`: TTL `window*2` from last write can expire the "previous" window early → `prevCount` reads 0 → under-count → over-admit.
- **M-4 · [TB-3 / GCRA-2 / LB-4]** `WaitN` with `n>capacity` (TB), `n>burst` (GCRA), or `leakRate=0` (LB) retries forever with `context.Background()` — no impossible-request pre-check. Constructors also accept `limit/window/rate<=0` leading to divide-by-zero panics (see M-5).
- **M-5 · [SWL-1 / SWC-1 / FW-1 / GCRA-3]** No constructor validates `window>0` / `limit>0` / `rate>0`. `NewCounter(10,0)`, `New(5,0)` (fixed window), `slidingwindow.NewLog(10,0)`, `gcra.New(0,1,s)` all panic (NewTicker(0) or integer divide-by-zero) — a config typo crashes the process.
- **M-6 · [CB-5]** `state.go:6-16`: enum order is `Closed=0,Open=1,HalfOpen=2`; spec (and the intended Prometheus gauge `1=half-open,2=open`) require `Closed=0,HalfOpen=1,Open=2`. Latent today (no numeric mapping in non-test code) but a landmine.
- **M-7 · [CB-7]** `circuitbreaker/middleware/grpc.go:103`: `codes.DeadlineExceeded` counted as a CB failure, contradicting its own doc and the core library (which does not count caller deadlines) → breaker trips on caller-imposed timeouts.
- **M-8 · [CB-6]** `circuitbreaker.go:311-318`: dead branch (`current==Open && current==HalfOpen`); the "re-open refreshes openedAt" the comment promises never executes.
- **M-9 · [PIPE-1]** `pipeline/pipeline.go:3-18`: godoc claims a "fixed, non-configurable" stage order; the Builder actually honors call order. `.Retry().Timeout()` puts Retry outermost → retry re-runs the whole timeout stage. Either enforce canonical order in `Build()` or fix the doc.
- **M-10 · [TIMEOUT-1]** `timeout/timeout.go:22-39`: `Do` is a bare `context.WithTimeout` wrapper — if `fn` ignores ctx it blocks past the deadline forever; the doc's "returns DeadlineExceeded" and the spec's goroutine+`*TimeoutError` design are not implemented (no `TimeoutError` type; `errors.As` impossible).
- **M-11 · [FB-2]** `fallback/fallback.go:78`: `Hedge`/`HedgeN` with `hedgeDelay<=0` fire the backup immediately, doubling downstream load for zero benefit.
- **M-12 · [BACKOFF-3]** `retry/backoff/exponential.go:26-30`: the overflow guard `if d<=0 {return max}` also catches `base<=0`, so `Exponential(0, 1h).Next(0)` returns 1h. Full/Equal jitter inherit it.
- **M-13 · [STORE-2]** `store/memory.go:216`: `IncrBy` has no overflow check; near `MaxInt64` it wraps negative (a counter that suddenly allows everything). Redis errors on overflow.
- **M-14 · [STORE-3]** `store/memory.go:125-135,186-201`: `SetNX`/`IncrBy` `LoadOrStore` before the maxKeys check, briefly publishing an entry that's about to be rejected (concurrent readers can observe it; a racing `Del` can be lost).
- **M-15 · [STORE-5]** `store/redis.go:199-219`: `GetSet` via `TxPipeline` relies on GET-before-SET ordering and muddled per-command error handling; fragile. Use `SET ... GET` (6.2+).
- **M-16 · [STORE-7]** `store/redis.go:88-90`: on Redis outage with `Fallback==nil`, silently routes to a fresh per-process in-memory store (fail-open, per-instance divergence, limits effectively reset), and `isConnectionError` misses `ECONNREFUSED` (non-timeout) → inconsistent fallback.
- **M-17 · [CLK-2/3]** `internal/clock/clock.go:248-267`: fired timer/ticker values are dropped (buffered-1 + `default`) so `Advance(5*interval)` may deliver 1 tick, not 5 (adaptive misses cycles); `Reset` after `Stop` never re-registers → ticker never fires again.
- **M-18 · [SRV-9]** `circuitbreaker_handlers.go:146-159`: `force-half-open` actually calls `forceTransitionOpen` and reports OPEN — the endpoint lies about what it did.
- **M-19 · [SRV-10]** `simulate_handler.go` + `simulation/engine.go`: per-request caps exist but there's **no global** cap on concurrent `/simulate` calls → N concurrent requests × 500 workers each = amplification DoS (compounded by C-1 no-auth).
- **M-20 · [SRV-7]** `websocket.go:41-51`: `CheckOrigin` returns true for `Origin==""` and, if `CORS_ORIGINS=*`, for **every** origin → CSWSH. Never allow `*` for WS origin.
- **M-21 · [SRV-12]** `handlers.go:33-36`, `circuitbreaker_handlers.go:74-76`: JSON decode errors are swallowed and treated as an empty default request (silent 200 instead of 400); inconsistent with `HandleSimulate`. `RequireJSON` is defined (`middleware.go:172`) but never wired.
- **M-22 · [SRV-3]** `main.go:86-93`: `http.Server` sets Read/Write/Idle timeouts but not `ReadHeaderTimeout` (Slowloris); WS upgrades bypass `WriteTimeout` and there is no ping ticker [SRV-8], so live-but-idle clients drop at 60s and stuck writes zombie the goroutine.

---

## LOW (condensed)

- **[TB-2]** `tokenbucket/distributed.go:48`: `ttlMs` computed then discarded; Lua PEXPIRE lacks the Go-side safety margin.
- **[TB-4 / SWC-3]** `Remaining` is `int(float)` truncation; `Peek.Remaining` can disagree with the actual allow decision at fractional boundaries.
- **[SWC-2]** `counter.go:241`: `RetryAfter` can exceed a full window / underestimate when `current.count` alone exceeds limit; cap `neededFrac` to [0,1].
- **[COMP-3]** OR-mode deny returns `shortestRetry`, contradicting the "most restrictive" spec (decide + document).
- **[COMP-4]** `composite.go:264-309`: dead `parallelAllowOR` (`//nolint:unused`), untested.
- **[AD-4]** `adaptive/signals.go:135-150`: `CPUPercent` is a meaningless proxy (cumulative `GCCPUFraction` + goroutine-count/1000) and calls STW `ReadMemStats` every 1s.
- **[AD-5]** `signals.go`: EMA seeds from the first sample (first-adjustment bias); power-of-2 latency histogram is coarser than the p99 thresholds.
- **[LB-2/3/5]** leaky bucket: ctx-cancel leaves a token stuck in the queue; `WaitN` loops single `Wait`s (non-atomic partial consume); `len(chan)`-derived metrics are racy estimates.
- **[GCRA-4]** `gcra/fuzz_test.go:52`: `advanceNs` computed but never applied — the fuzzer only ever tests first-request-always-allowed.
- **[CB-9]** `metrics.go:96`: `numBuckets = windowDuration/bucketWidth` panics if `bucketWidth==0` (guarded only by `defaults()`); unbounded slice for tiny bucketWidth.
- **[CB-10]** `registry.go:59-69`: `Reset` is Range+Delete, not atomic; concurrent `GetOrCreate` can re-insert. (GetOrCreate itself is race-free.)
- **[SRV-11]** WS `HandleLimiter`/`HandleCB` store unvalidated `{algorithm}`/`{name}` path params per connection (no `validateKey`).
- **[SRV-13]** `server/metrics/prometheus.go` is entirely dead code (never wired) — no HTTP/RL/CB metrics recorded despite `/metrics` being served.
- **[SRV-15]** `config.go:58-61`: `SERVER_ADDR=host:port` yields bind addr `host:port:8080` (broken legacy branch).
- **[FE-6]** `frontend/lib/ws/manager.ts`: backoff is 0.5s/1s/2s (spec 1s/2s/4s), no jitter (thundering herd), no event buffering during reconnect, singleton `disconnect()` wedges forever.
- **[FE-5]** `usePoll.ts`: no in-flight guard / no `AbortController` → overlapping requests, no abort on unmount.
- **[FE-7]** `pipeline/page.tsx:316`: two `react/no-unescaped-entities` ESLint **errors** → `next build` fails under default lint-on-build (contradicts prior audit's "build OK").
- **[ATOM-1]** `internal/atomicx/float64.go`: bit-exact float CAS is NaN-fragile (theoretical).

---

## Test-quality findings (false confidence)

- **[T-1/H-21]** Distributed limiters have zero non-integration coverage (no memory-store script handler) — masks H-1…H-5.
- **[CB-11]** `TestCB_HalfOpen_ProbeSucceeds_CloseCircuit` never asserts the circuit closes (only `t.Log`s); `TestCB_SuccessThreshold_RequiresConsecutiveSuccesses` contains `t.Skip(...)` + `t.Logf("Not fatal")` — real regressions pass silently.
- **[SRV-16/17/18]** Handler tests call handlers directly, bypassing (and never asserting) the middleware chain, so C-1/C-2 pass the suite. `TestHandleSimulate_ConcurrencyCapped` asserts only "finishes in 5s", not that the cap held. No WebSocket tests at all → H-17 unexercised.
- **[COMP-5]** Composite "no token loss" test only covers the serial path (C-5 proves the concurrent path leaks); invalid-key tests pass only because the underlying limiter validates — composite itself has no validation.
- **[T-2/3/4]** Sliding-window tests fire all requests at one instant (no clock advance) → exercise no sliding behavior; the 2+-window stale-previous case (the key counter invariant) is never tested.
- **[LB-6]** Leaky-bucket tests use real `time.Sleep` not the ManualClock and accept "either nil or error" — assert nothing about correctness.

---

## Prior-audit trust findings [AUDIT-1..3]

- **AUDIT-1 (High):** `AUDIT_REPORT.md` CRIT-1 claims `LeakyBucket.AllowN` is now atomic. It is not (H-6) — the fix is buggy under concurrency and its tests never hit the partial-enqueue path.
- **AUDIT-2 (Medium):** "npm run build OK (0 errors)" — tsc is clean but ESLint has 2 errors that fail `next build` (FE-7).
- **AUDIT-3 (Low):** Benchmark numbers are copy-forwarded ("from previous runs"), internally inconsistent (Fixed Window 73 ns/op in CHANGELOG vs 45 ns/op in the audit table), and the "real-time WebSocket" production-ready claim is contradicted by the dead WS path (H-19).

---

## Recommended fix order (by blast radius)

1. **C-1/C-2** — add auth; close the open control plane. (Unauthenticated DoS.)
2. **H-1…H-5** — distributed limiters over-admit; make them atomic (Lua), fix TTL/`n` handling. (These are the library's core promise.)
3. **C-3/H-8/H-9** — circuit-breaker probe accounting; fix the leak-on-panic and the inc/dec desync.
4. **C-5/H-6/M-1** — composite + leaky-bucket atomicity; remove the false "no token loss" guarantees.
5. **C-4/H-13/H-14/H-15/H-16/H-17** — leaks & data races (memory store counter, jitter/decorrelated rand, clock deadlock, threadpool, WS shutdown).
6. **C-6/H-11/H-12** — adaptive limiter (recover-upward, spec conformance, state preservation).
7. **H-19/H-20** — frontend↔backend contract (dead WS, CB execute body).
8. **M-4/M-5** — add constructor validation to stop panics on bad config.
9. Backfill tests that exercise concurrency, distributed paths, the middleware chain, and multi-window time progression (the suite currently passes over every bug above).
