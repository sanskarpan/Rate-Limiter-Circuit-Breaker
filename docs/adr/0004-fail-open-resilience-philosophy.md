# ADR-0004: Fail-open on store errors

## Status

Accepted.

## Context

Distributed rate limiters and the distributed circuit breaker depend on an
external store (typically Redis) to hold shared state. That store can become
unreachable: a network partition, a failover, a restart, a saturated connection
pool.

When the store is down, the limiter/breaker faces a binary choice on every call:

- **Fail closed** — deny the request (or hold the breaker open) because it cannot
  confirm the shared state. Safe for the downstream, but it converts a Redis
  outage into a **full outage of the protected service**: the very failure the
  library exists to survive is now amplified by the library itself.
- **Fail open** — allow the request and fall back to local enforcement, accepting
  temporarily weaker limiting. The service stays up; enforcement degrades rather
  than availability.

For a resilience library, turning a dependency's partial outage into a total
outage of the caller is the worst possible behaviour.

## Decision

**On store errors, the distributed components fail open.**

- **Distributed rate limiters.** The Redis store detects "Redis is unreachable"
  errors — connection-refused / reset / broken-pipe syscall errors and net
  timeouts (`ratelimit/store/redis.go`, the fallback-classification logic) — and
  **transparently routes the operation to a fallback store** instead of returning
  an error. If the caller supplies no explicit `Fallback`, `NewRedis` installs a
  fresh per-process in-memory store as the fallback.

  The consequence is documented candidly on `RedisOptions.Fallback`: with the
  default per-process fallback, each instance rate-limits against its own local
  counters with no shared state, so **the effective global limit is multiplied by
  the number of instances** ("fail-open / per-instance divergence"). This
  preserves availability during a Redis outage at the cost of enforcement
  accuracy. Callers who cannot accept that trade-off can supply an explicit
  fallback (including a fail-closed deny-all store).

- **Distributed circuit breaker.** `DistributedCircuitBreaker.Execute`
  (`circuitbreaker/distributed.go`) fails open when the store errors on the
  acquire step: it **runs `fn` anyway** rather than wedging traffic, and records
  the outcome best-effort so shared state re-converges once the store recovers. A
  store outage therefore degrades the breaker to "always closed" (no protection)
  rather than "always open" (total outage). Read-side methods (`State`,
  `Snapshot`) likewise fail open, reporting `Closed`/empty on a store error.

Fail-open is the **default**. Fail-closed is available as an explicit opt-in for
the rate limiters (supply a deny-all fallback store).

## Consequences

**Positive:**

- A Redis outage degrades **enforcement accuracy**, not **service availability** —
  the library never turns its own dependency's failure into a caller outage.
- Behaviour is uniform and predictable across both the distributed rate limiters
  and the distributed circuit breaker, and it is documented at the point of use
  rather than hidden.
- Callers with stricter requirements retain control via an explicit fallback
  store.

**Negative / trade-offs:**

- During a store outage, distributed limits silently weaken to **per-instance ×N**
  (over-admission), and the distributed breaker loses its fleet-wide protection —
  operators must understand this is the deliberate default, not a bug.
- The "correct" choice is deployment-specific; a security-sensitive quota may
  genuinely prefer fail-closed, which requires opting out of the default.
- Best-effort convergence means there is a brief window after recovery where
  shared state is catching up.
