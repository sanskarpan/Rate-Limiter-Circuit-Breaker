# Building a zero-dependency Go resilience library

A resilience library is infrastructure: it sits on the hot path of every request
in the services that adopt it. That gives its dependency graph outsized weight —
every transitive dependency the library pulls in becomes a dependency of *those*
services, expanding their attack surface, their CVE exposure, and their
version-conflict friction. Plenty of teams reject an infrastructure library on
its `go.mod` graph alone, before ever reading the code.

So this library takes a hard line, captured in
[ADR-0001](../adr/0001-zero-dependency-core.md): **the core algorithm and
resilience packages have zero external runtime dependencies.** This post is about
what that actually buys you, why "zero-dep" is worthless unless it's *enforced*,
and how the optional integrations that genuinely need third-party code are kept
out of the core.

## The core is provably dependency-free

Import only the local (in-memory) algorithms — token bucket, GCRA, the sliding
windows, leaky bucket — or the resilience primitives — circuit breaker, retry,
timeout, bulkhead, fallback, the pipeline — and you pull in **no external runtime
dependencies at all**. The eight rate-limiting algorithms sit behind one uniform
`ratelimit.Limiter` interface; the resilience patterns compose through a
fixed-order `pipeline`. None of them import anything outside the standard library
and the module's own `internal/` packages.

That is the marketing claim on pkg.go.dev, and the point of this design is that
the claim is *provable*, not aspirational.

## "Zero-dep" only means something if CI enforces it

A dependency boundary that lives in a README erodes the first time someone adds a
convenient import in a hurry. So the rule is a **tested invariant**.

The `verify-deps` Make target runs `go list` over an explicit allow-list of core
packages — `ratelimit` and its algorithm subpackages, `circuitbreaker`,
`bulkhead`, `retry`, `retry/backoff`, `timeout`, `fallback`, `pipeline`,
`loadshed`, `concurrency`, `metric`, and `internal/clock` / `internal/atomicx` —
and **fails the build if any of them imports a non-stdlib, non-internal
package.** The CI `verify-zero-deps` job invokes it on every push
(`.github/workflows/ci.yml`).

The consequence is that a pull request which accidentally imports, say, the Redis
client into a core algorithm package doesn't just get a reviewer's frown — it
fails CI. The guarantee can't silently rot.

## Where the real dependencies live

The library still integrates with the outside world; it just quarantines those
integrations behind clearly isolated layers:

- **Distributed backend** lives behind the `ratelimit/store` interface. The Redis
  adapter (`ratelimit/store/redis.go`) is the *only* place that imports
  `github.com/redis/go-redis/v9`. The core algorithms talk to the `store.Store`
  interface (`ratelimit/store/store.go`), never to Redis directly — which is also
  what makes them testable against a dependency-free in-memory store that
  faithfully emulates each Lua script (see
  [ADR-0003](../adr/0003-store-interface-and-lua-for-distributed.md)).

- **Framework middleware** — chi, gin, echo, Fiber, connect — lives in a
  **separate `contrib` module** with its own `go.mod`. Those framework
  dependencies never enter the core module's graph. You opt into a framework
  adapter by depending on `contrib`, and only then do its dependencies appear.

- **Observability** adapters (`metric/prometheus`, `observability/otel`) isolate
  the Prometheus and OpenTelemetry SDKs. The core emits through a small,
  no-op-by-default `metric.Recorder` interface (`metric/metric.go`) rather than
  importing a metrics SDK. If you never wire a recorder, the hot path is a single
  nil check and no metrics dependency is linked.

The unifying rule: **you pay for a dependency only if you import the adapter that
needs it.** Redis, gRPC, and metrics SDKs are all opt-in.

## Why bother?

Three payoffs, in order of how often they matter:

1. **Adoptability.** The teams most likely to want a resilience library are the
   ones most careful about their dependency footprint. A dependency-free core
   clears their gate.
2. **Blast radius.** A CVE in `go-redis` affects only services that opted into
   distributed limiting. A CVE in the OTel SDK affects only services that wired a
   tracer. The core is out of the line of fire.
3. **Honesty.** Because the boundary is CI-enforced, "zero-dependency core" is a
   fact about the current commit, not a stale aspiration from the first release.

The cost is real but modest: some integrations that would be one import in a
looser design become an interface plus an adapter, and the `contrib` split means
framework users add a second module. For infrastructure that lands on everyone's
hot path, that's a trade worth making — and one the CI gate makes permanent.
