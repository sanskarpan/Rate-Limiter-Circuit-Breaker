# integration/ — container-based integration tests (ENHANCEMENTS §6.3)

This is a **separate nested Go module** whose only purpose is to house heavy,
container-based integration tests. It exists so the large
[`testcontainers-go`](https://golang.testcontainers.org/) dependency graph
**never touches the zero-dependency core module** (the repo-root `go.mod`).

The core library is pulled in via a local `replace` directive
(`replace github.com/sanskarpan/Rate-Limiter-Circuit-Breaker => ../`), mirroring
`contrib/go.mod`, so the tests always run against the working-tree source.

## What it tests

The tests spin up a **real Redis 7 container in-process** and exercise the
**real Redis Lua scripts** (not the in-memory emulation):

| Test | Asserts |
| ---- | ------- |
| `TestContainer_DistributedCircuitBreaker_SharedState` | Two independent `DistributedCircuitBreaker` instances share one Redis-backed state; A's failures trip it, B observes Open without ever failing. |
| `TestContainer_DistributedTokenBucket_Limit` | A distributed token bucket of capacity N admits exactly N immediate requests, denies the rest. |
| `TestContainer_DistributedTokenBucket_ParityAcrossInstances` | Two handles on the same prefix+key share the bucket via Redis. |

All tests are gated behind the `integration` build tag.

## Running (Docker required)

```sh
cd integration
go test -tags=integration ./...
```

Requirements: a working Docker daemon (or a compatible runtime such as
Podman/Colima with `DOCKER_HOST` set). `testcontainers-go` starts and tears the
container down automatically — no `docker-compose`, no port juggling, no
pre-provisioned Redis.

## Fallback without Docker (docker-compose)

If Docker-in-testcontainers is unavailable in your environment, the same
assertions run against a Redis started via the repo's `docker-compose.yml` and
the **core module's own** integration tests (which use the `REDIS_ADDR`
env-var):

```sh
# from the repo root
docker compose -f docker-compose.yml up -d redis
REDIS_ADDR=localhost:6379 go test -tags=integration ./circuitbreaker/ ./ratelimit/...
docker compose -f docker-compose.yml down
```

## Why a nested module (not build tags in the root module)

`testcontainers-go` transitively pulls in Docker, containerd, gRPC, gopsutil,
and dozens of other packages. Adding it to the root `go.mod` — even as a
test-only dependency — would pollute `go.sum`, slow `go mod download` for every
consumer, and break the project's "core is zero-dependency" guarantee
(`make verify-deps`). A nested module with its own `go.mod`/`go.sum` keeps that
weight fully isolated: consumers of the core library never see it.
