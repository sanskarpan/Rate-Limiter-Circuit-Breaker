# Consistency Guarantees

This page states, precisely and honestly, what consistency each limiter and the
circuit breaker provide across backends and failure modes. It is the crisp
counterpart to the narrative in [docs/distributed.md](distributed.md).

Terminology used below:

- **Strong / exact** — the limit is enforced exactly; two concurrent checks never
  both admit past the limit.
- **Per-key atomic** — each individual key's check-and-update is atomic, but there
  is no cross-key transaction: two *different* keys are independent.
- **Eventual (across keys / instances)** — different keys or instances converge
  over time but are not coordinated within a single operation.
- **Fail-open** — on a backend error the component degrades to allowing traffic
  with weaker enforcement (see [ADR-0004](adr/0004-fail-open-resilience-philosophy.md)),
  rather than denying it.

## Rate limiters

| Backend / mode | Scope of the guarantee | Consistency | Atomicity | Clock source | On backend failure |
| --- | --- | --- | --- | --- | --- |
| **In-memory (single instance)** | One process | **Strong / exact** for that process | Mutex-guarded per key | Injected `clock.Clock` (`RealClock` in prod) — see [ADR-0002](adr/0002-clock-interface-for-determinism.md) | N/A (no external store) |
| **Redis-backed (client time)** | Whole fleet, shared key space | **Per-key atomic**, exact under normal operation; **eventual across keys** | Single atomic Lua script per check ([ADR-0003](adr/0003-store-interface-and-lua-for-distributed.md)) | Client-supplied `now` — **susceptible to fleet clock skew** | **Fail-open** to fallback store → per-instance ×N over-admission |
| **Redis-backed (`UseServerTime`)** | Whole fleet, shared key space | **Per-key atomic**, exact; skew-resistant | Single atomic Lua script per check | **Redis server `TIME`** read in-script (one authoritative clock) | **Fail-open** to fallback store → per-instance ×N over-admission |

Notes:

- **"Per-key atomic, eventual across keys"** is the honest description of the
  distributed algorithms. Each key (`user:123`, `api-key:xyz`, …) is decided by
  one atomic Lua script, so a single key's limit is enforced correctly under
  concurrency across the whole fleet. There is **no** cross-key transaction: a
  composite/AND check over several keys is only as atomic as the script that
  covers it.
- **Multiple instances** sharing one Redis all see the same authoritative counter
  per key, so the *global* limit holds — **as long as Redis is reachable.**
- **`UseServerTime`** is the mitigation for clock skew: with skewed application
  clocks, client-time mode can corrupt a time-based decision, whereas server-time
  mode pins every instance to the single Redis clock. It is **off by default** for
  backward compatibility and costs a tiny in-script `TIME` read.

## Circuit breaker

| Mode | Scope | Consistency | On backend failure |
| --- | --- | --- | --- |
| **Local (`circuitbreaker.New`)** | One process | **Strong** for that process; each instance learns independently | N/A |
| **Distributed (`circuitbreaker.NewDistributed`)** | Whole fleet | State shared via the store; transitions are atomic per breaker key; **eventual** convergence across instances | **Fail-open** — runs `fn` and records best-effort ([ADR-0004](adr/0004-fail-open-resilience-philosophy.md)) |

The distributed breaker packs its state into a single store key and runs every
allow/reject decision plus every transition inside an atomic Lua script, so when
one instance trips the breaker the whole fleet observes `Open` quickly instead of
each instance learning independently. Its Open→HalfOpen timeout is evaluated
**against the Redis server clock in-script**, so app-fleet clock skew cannot open
the breaker early or late (`circuitbreaker/distributed.go`).

## Fail-open degradation (the important caveat)

For **all** distributed components, a store outage triggers **fail-open**
degradation, by design ([ADR-0004](adr/0004-fail-open-resilience-philosophy.md)):

- **Rate limiters**: the Redis store transparently routes to a fallback store on
  unreachable-Redis errors. With the default per-process in-memory fallback, each
  instance then limits against its own local counters with no shared state, so
  the **effective global limit is multiplied by the number of instances** — a
  temporary over-admission, not a denial. Supply an explicit `Fallback` (including
  a deny-all fail-closed store) if that trade-off is unacceptable
  (`RedisOptions.Fallback`, `ratelimit/store/redis.go`).
- **Circuit breaker**: a store error degrades the breaker to "always closed" (no
  protection) rather than "always open" (total outage); state re-converges once
  the store recovers.

The choice is availability over strict enforcement. It is the correct default for
a resilience library, but it means "distributed" does **not** mean "exact under
all failure modes."

## Accuracy caveat: float64 / ~256 ns snapping

The time-based distributed algorithms (GCRA, token bucket, sliding-window-log)
store nanosecond timestamps/TATs that Redis evaluates as IEEE-754 **doubles**.
Values around `1.78e18` ns exceed float64's 2^53 exact-integer ceiling, so the
in-script arithmetic **snaps to roughly 256 ns granularity**. The in-memory
emulation performs the *same* float64 arithmetic on purpose, so it stays faithful
to what Redis actually computes (`ratelimit/store/scripts_memory.go`; see also
ENHANCEMENTS §5.4 and [ADR-0003](adr/0003-store-interface-and-lua-for-distributed.md)).

Practical impact: timing decisions on these algorithms carry a bounded error of
**≤ ~256 ns**. This is immaterial for real rate limits (which operate on
milliseconds-and-up windows) but is documented here as a precise guarantee rather
than left as a surprise.

## Summary

- **In-memory, single instance** → strong/exact, no external failure mode.
- **Redis, per key** → atomic and exact while Redis is up; eventual across keys;
  clock-skew-resistant only with `UseServerTime`.
- **Any distributed component, Redis down** → fail-open, weaker enforcement,
  service stays available.
- **Time-based distributed algorithms** → ≤ ~256 ns accuracy floor from float64
  snapping.
