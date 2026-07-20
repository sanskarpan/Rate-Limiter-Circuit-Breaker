# Resilience

A correctness-first, **zero-dependency-core** resilience toolkit for Go:
eight rate-limiting algorithms behind one uniform `ratelimit.Limiter` interface,
five with atomic Redis-Lua distributed backends, plus circuit breaker, retry,
timeout, bulkhead, fallback/hedging, load shedding and adaptive concurrency —
all composable through a fixed-order pipeline.

Module path: `github.com/sanskarpan/Rate-Limiter-Circuit-Breaker`

## Why this exists

Most Go services eventually need to cap request rate, stop cascading failures,
bound concurrency, and degrade gracefully under load. Those primitives usually
arrive as a pile of unrelated dependencies with inconsistent APIs and untestable,
wall-clock-driven time. This library ships them together, behind uniform
interfaces, with **deterministic time** (`internal/clock.ManualClock`) so every
time-dependent behaviour is testable without `time.Sleep`.

The **core** algorithm and resilience packages have **zero external runtime
dependencies** — a rule enforced in CI, not just documented. Redis, framework
middleware, and Prometheus/OpenTelemetry live behind isolated adapter layers and
a separate `contrib` module, so you pay for them only if you import them
(see [ADR-0001](adr/0001-zero-dependency-core.md)).

## Start here

- **[Algorithms](algorithms.md)** — per-algorithm theory, formulas, and properties
- **[Comparison & decision guide](comparison.md)** — trade-offs and which algorithm to pick
- **[Distributed limiting](distributed.md)** — Redis-backed limiters, fail-open, cluster notes
- **[Examples](examples.md)** — runnable programs under `examples/` and how to run them
- **[Cookbook](cookbook/index.md)** — copy-pasteable, API-accurate recipes
- **[Migration guide](migration.md)** — from `golang.org/x/time/rate` and `sony/gobreaker`

## Design & guarantees

- **[Architecture Decision Records](adr/README.md)** — the load-bearing "why" decisions
- **[Consistency guarantees](consistency-guarantees.md)** — what "distributed" promises per failure mode
- **[Redis key lifecycle](redis-key-lifecycle.md)** — TTL/GC audit and hot-key protection
- **[Multi-region strategy](multi-region.md)** — active-active and cross-region notes
- **[Stability policy](STABILITY.md)** — which packages are stable / beta / experimental
- **[Benchmarks](benchmarks.md)** — reproducible microbenchmarks (ns/op, B/op, allocs/op)

## Deep dives

- **[Blog & talk material](blog/index.md)** — design write-ups and a conference-talk outline

## Contributing

- **[Good first issues](good-first-issues.md)** — scoped starter tasks for new contributors

> This site is generated with [MkDocs](https://www.mkdocs.org/) from the markdown
> already in `docs/`. Every page here is a real file in the repository.
