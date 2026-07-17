# Recipe cookbook

Copy-pasteable, API-accurate recipes for common resilience scenarios. Every
snippet uses the real public API of
`github.com/sanskarpan/Rate-Limiter-Circuit-Breaker`.

Migrating from another library? Start with the
[migration guide](../migration.md).

## Rate limiting

- [Rate limit per IP (Gin, chi, echo, Fiber)](ratelimit-per-ip-frameworks.md) —
  drop-in middleware for the four supported HTTP frameworks.
- [Per-tenant / per-API-key quotas](per-tenant-quotas.md) — keyed limiters plus
  a composite `AND` for stacked global + per-tenant tiers.
- [Cost / weight-based limiting](cost-weighted-limiting.md) — charge expensive
  endpoints more tokens with `WithCost` / `AllowN`.
- [Distributed rate limit with Redis + fail-open](distributed-redis-failopen.md) —
  fleet-wide limits, server-time clock-skew mitigation, graceful degradation.

## Resilience

- [Protect a flaky downstream](flaky-downstream-cb-retry-hedge.md) — circuit
  breaker + retry (with a retry budget) + hedge/fallback via the pipeline.
- [Adaptive concurrency + load shedding](adaptive-concurrency-loadshed.md) —
  self-tuning concurrency limits and CoDel-style shedding under overload.

## Observability

- [Prometheus metrics + OpenTelemetry tracing](observability-prometheus-otel.md) —
  wire the `metric.Recorder` adapter and OTel spans.
</content>
