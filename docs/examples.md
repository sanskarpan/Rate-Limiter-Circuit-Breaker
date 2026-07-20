# Examples

Runnable example programs live under [`examples/`](https://github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/tree/main/examples).
They are all `package main` programs **in the main module** (no separate
`go.mod`), so they compile and run against the library exactly as it ships.

Build them all at once:

```bash
go build ./examples/...
```

Run a specific example:

```bash
go run ./examples/<name>
```

## Catalog

| Example | Demonstrates | Run |
|---------|--------------|-----|
| [`http-server`](https://github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/tree/main/examples/http-server) | Token-bucket rate-limit middleware (`KeyByIP`) + a circuit breaker around a downstream, wired into `net/http`. | `go run ./examples/http-server` then `curl localhost:8080/api/test` |
| [`grpc-server`](https://github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/tree/main/examples/grpc-server) | Rate limiting and circuit breaking as gRPC unary interceptors. | `go run ./examples/grpc-server` |
| [`pipeline`](https://github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/tree/main/examples/pipeline) | The `pipeline.Builder` composing rate limit → bulkhead → timeout → circuit breaker → retry in the fixed canonical order. | `go run ./examples/pipeline` |
| [`distributed`](https://github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/tree/main/examples/distributed) | A Redis-backed distributed limiter with the fail-open fallback path. Needs a reachable Redis (falls back gracefully if absent). | `go run ./examples/distributed` |
| [`resilience-stack`](https://github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/tree/main/examples/resilience-stack) | The `resilience.Builder` facade: fluent composition of every layer plus a retry budget, generic `Execute[T]`, and `ExecuteWithFallback[T]`. | `go run ./examples/resilience-stack` |
| [`tiered`](https://github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/tree/main/examples/tiered) | Hierarchical limits with `ratelimit/tiered` (per-user AND per-org AND global), all-or-nothing accounting, and the denying-tier surfaced in `Result.Metadata`. | `go run ./examples/tiered` |
| [`debounce`](https://github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/tree/main/examples/debounce) | The `debounce` package: trailing-edge `Debouncer` and leading-edge `Throttler`. | `go run ./examples/debounce` |

## What each new example shows

### `resilience-stack`

Builds a full stack — rate limiter, bulkhead, timeout, circuit breaker, retry
(with a `retry.Budget`) and an outer fallback — with `resilience.New()`. The
builder methods are called in an arbitrary order on purpose; `Build()` sorts the
layers into the fixed canonical order (fallback → rate limit → bulkhead → timeout
→ circuit breaker → retry → operation), which the example prints via
`Builder.Layers()`. It then exercises three entry points:

- `Stack.Execute` — the error-only path, where the retry layer absorbs a
  transient failure;
- `resilience.Execute[T]` — the generic, value-returning path;
- `resilience.ExecuteWithFallback[T]` — a typed fallback that supplies a default
  value when the primary operation fails.

### `tiered`

Enforces "3 req per user **and** 5 req per org **and** 8 req globally" as an
ordered `tiered.TieredLimiter`. Request keys look like `org:user`; each tier
derives its own key (`tiered.Prefix(":")` for the org tier, `tiered.Constant`
for the global tier). Because accounting is all-or-nothing, a deny never leaves a
higher tier debited, and the denying tier is reported under
`Result.Metadata["denied_tier"]`.

### `debounce`

Shows the two coalescing primitives. `Debouncer` collapses a burst of rapid calls
into a single trailing-edge invocation of the most recent function ("save once the
user stops typing"). `Throttler` runs at most once per interval with a leading
edge and one coalesced trailing invocation ("refresh, but no more than once per
interval"). Both are concurrency-safe and accept an injectable clock for
deterministic tests.

## Framework recipes

For drop-in middleware against chi, gin, echo, Fiber and connect (which live in
the separate `contrib` module), see the
[cookbook](cookbook/index.md) — in particular
[per-IP limiting across frameworks](cookbook/ratelimit-per-ip-frameworks.md).
