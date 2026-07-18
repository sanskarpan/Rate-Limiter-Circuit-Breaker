# ADR-0001: Zero-dependency core; dependencies isolated in adapters

## Status

Accepted.

## Context

A resilience library is infrastructure: it sits on the hot path of every request
in the services that adopt it. Every transitive dependency it pulls in becomes a
dependency of *those* services — a larger attack surface, more CVE exposure, more
version-conflict friction, and a heavier `go.mod` graph. Many teams reject
infrastructure libraries purely on their dependency footprint.

The library nonetheless needs to integrate with external systems for its optional
features: Redis for distributed rate limiting, gRPC/HTTP frameworks for
middleware, and Prometheus/OpenTelemetry for observability. Those integrations
require real third-party packages.

## Decision

**The core algorithm and resilience packages have zero external runtime
dependencies.** External dependencies are permitted only in clearly isolated
adapter layers:

- **Distributed backend** lives behind `ratelimit/store`. The Redis adapter
  (`ratelimit/store/redis.go`) is the only place that imports
  `github.com/redis/go-redis/v9`; the core algorithms talk to the `store.Store`
  interface (`ratelimit/store/store.go`), never to Redis directly.
- **Framework middleware** (chi, gin, echo, fiber, connect) lives in a
  **separate `contrib` module** (`contrib/go.mod`) so those framework
  dependencies never enter the core module's graph. See
  [docs/STABILITY.md](../STABILITY.md).
- **Observability** adapters (`metric/prometheus`, `observability/otel`,
  `observability/otelhttp`) isolate the Prometheus / OpenTelemetry dependencies;
  the core emits through a small no-op-default `metric.Recorder` interface
  (`metric/metric.go`) rather than importing a metrics SDK.

The rule is **enforced in CI**, not just by convention. The `verify-deps` Make
target (`Makefile`, target `verify-deps`) runs `go list` over an explicit list of
core packages — `ratelimit` and its algorithm subpackages, `circuitbreaker`,
`bulkhead`, `retry`, `retry/backoff`, `timeout`, `fallback`, `pipeline`,
`loadshed`, `concurrency`, `metric`, and `internal/clock` / `internal/atomicx` —
and **fails the build if any of them imports a non-stdlib, non-internal package.**
The CI `verify-zero-deps` job (`.github/workflows/ci.yml`) invokes it on every
push.

## Consequences

**Positive:**

- Adopters who use only the local (in-memory) algorithms pull in **no external
  runtime dependencies** — the marketing claim on pkg.go.dev is provable, not
  aspirational.
- The zero-dep boundary is a *tested invariant*: a PR that accidentally imports
  Redis into a core algorithm package fails CI, so the guarantee can't silently
  erode.
- Redis, gRPC, and metrics SDKs are opt-in — you pay for them only if you import
  the adapter that needs them.

**Negative / trade-offs:**

- The abstraction has a cost: distributed atomicity must be expressed through the
  narrow `store.Store` interface and Lua scripts (see
  [ADR-0003](0003-store-interface-and-lua-for-distributed.md)) rather than by
  reaching for a rich Redis client API directly.
- Observability is indirect: the core fires through the `metric.Recorder`
  interface instead of calling a Prometheus client inline, which adds a small
  layer of indirection.
- Multi-module layout (`contrib` as its own module) adds release and versioning
  complexity — `contrib` is tagged and versioned independently of the core.
