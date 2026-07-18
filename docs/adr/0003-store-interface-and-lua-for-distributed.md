# ADR-0003: `Store` interface + Lua scripts for distributed limiting

## Status

Accepted.

## Context

Single-instance rate limiting is easy: hold the counter in memory behind a mutex.
Distributed rate limiting across a fleet is not — every instance must agree on a
shared counter, and each check-and-decrement must be **atomic** across concurrent
instances, or two nodes will both read "1 token left" and both admit, breaking
the limit.

The classic Redis approaches to this atomicity are `WATCH`/`MULTI`/`EXEC`
optimistic transactions (which require retry loops on contention and multiple
round-trips) or server-side Lua scripts (which run atomically in a single
round-trip). We also wanted to keep the core algorithm packages free of any Redis
dependency (see [ADR-0001](0001-zero-dependency-core.md)), and to keep the
distributed algorithms testable without a live Redis.

A further subtlety: the limiter decision depends on **time**. If every instance
supplies its own `now`, clock skew across the fleet corrupts a shared decision.

## Decision

**Distributed backends sit behind a narrow `store.Store` interface**
(`ratelimit/store/store.go`), and each distributed algorithm's atomicity is
expressed as a **named Lua script** executed through `Store.Eval`.

- `Store` exposes `Get`/`Set`/`SetNX`/`GetSet`/`IncrBy`/`Eval`/`Del`/`Ping`/`Close`,
  all required to be safe for concurrent use. Algorithms depend only on this
  interface, never on a concrete Redis client.
- The Redis adapter (`ratelimit/store/redis.go`) implements each algorithm as an
  atomic Lua script — `TokenBucketScript`, `GCRAScript`, `FixedWindowScript`,
  `SlidingWindowCounterScript`, `SlidingWindowLogScript`, and the circuit-breaker
  `CircuitBreakerAcquireScript` / `CircuitBreakerRecordScript` /
  `CircuitBreakerReadScript`. Each script does the full read-decide-write in one
  server-side, atomic step — **no `WATCH`/`MULTI`/`EXEC` round-trips.**
- The in-memory store (`ratelimit/store/memory.go`) registers Go handlers under
  the same script names and **emulates each Lua script faithfully**
  (`ratelimit/store/scripts_memory.go`), including reproducing Redis's float64
  arithmetic quirks (see below). This gives the distributed algorithms a
  dependency-free, deterministic backend for unit tests, with a
  `parity_integration_test.go` asserting the emulation matches real Redis.

**Clock source — server `TIME`.** To defend against fleet clock skew, the
time-sensitive scripts support a **server-time mode** (`RedisOptions.UseServerTime`).
When enabled, the script reads the Redis server's own clock via the in-script
`TIME` command (guarded by `redis.replicate_commands()` so the non-deterministic
call replicates safely) and uses that as the authoritative `now`, instead of the
client-supplied timestamp. Every instance is then pinned to a single clock — the
Redis server's — regardless of how skewed the calling hosts are. The distributed
circuit breaker likewise evaluates its Open→HalfOpen timeout against the Redis
server clock in-script (`circuitbreaker/distributed.go`).

**Fail-open on store errors.** The Redis store degrades to a fallback store when
Redis is unreachable rather than returning errors up the stack. This is important
enough to have its own record — see
[ADR-0004](0004-fail-open-resilience-philosophy.md).

### The float64 / ~256ns snapping caveat

Redis evaluates Lua numbers as IEEE-754 doubles. Nanosecond timestamps and TATs
(~1.78e18) exceed float64's 2^53 exact-integer ceiling, so the arithmetic
**snaps to roughly 256 ns granularity**. Rather than pretend otherwise, the
in-memory emulation performs the **same** float64 arithmetic so it stays faithful
to what Redis actually computes (documented in `ratelimit/store/scripts_memory.go`,
around the GCRA handler). The practical effect is a bounded accuracy error on
sub-microsecond timing, documented as a guarantee in
[docs/consistency-guarantees.md](../consistency-guarantees.md).

## Consequences

**Positive:**

- **Atomic** distributed decisions in a single round-trip per check; no optimistic
  retry loops or multi-command transactions.
- The core stays Redis-free: algorithms depend on `store.Store` only, upholding
  [ADR-0001](0001-zero-dependency-core.md).
- The in-memory emulation makes distributed logic **fast and deterministic to
  test**, and parity tests keep the emulation honest against real Redis.
- Server-time mode gives a principled answer to fleet clock skew.

**Negative / trade-offs:**

- Every distributed algorithm needs **two** implementations kept in lock-step: the
  Lua script and its Go emulation. Parity is a maintenance burden (mitigated by
  the parity integration test).
- Distributed atomicity is expressed in Lua, which is harder to read and debug
  than Go and ties the design to a backend that can run scripts. Backends that
  can't run arbitrary Lua (Memcached, DynamoDB) would need each algorithm
  re-expressed via CAS / conditional writes.
- Server-time mode costs a tiny extra in-script `TIME` read and forces effects
  replication for those scripts; it is therefore **off by default** for backward
  compatibility.
- The float64 snapping imposes a documented ~256 ns accuracy floor on the
  time-based distributed algorithms.
