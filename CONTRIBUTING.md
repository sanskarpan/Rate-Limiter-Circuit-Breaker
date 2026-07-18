# Contributing

Thanks for your interest in contributing to this project — a production-grade
Go resilience library (rate limiters, circuit breakers, bulkheads, retries,
timeouts, fallbacks, and pipelines) plus a Next.js visualization frontend.

This document explains how to build, test, and submit changes. By participating
you agree to abide by our [Code of Conduct](CODE_OF_CONDUCT.md).

## Table of contents

- [Contributor on-ramp](#contributor-on-ramp)
- [Prerequisites](#prerequisites)
- [Development workflow](#development-workflow)
- [Building and testing](#building-and-testing)
- [Integration tests (Redis)](#integration-tests-redis)
- [Running the demo server and frontend](#running-the-demo-server-and-frontend)
- [Code style](#code-style)
- [The zero-dependency rule](#the-zero-dependency-rule)
- [Commit messages](#commit-messages)
- [Pull requests](#pull-requests)

## Contributor on-ramp

New to the project? Here is the fast path from clone to merged PR.

**1. Set up your environment**

```bash
git clone https://github.com/<you>/Rate-Limiter-Circuit-Breaker.git
cd Rate-Limiter-Circuit-Breaker
make help          # list every Makefile target
make test          # unit tests + coverage — confirms the toolchain works
```

Key Makefile targets you'll use most:

| Target | When |
| --- | --- |
| `make test-race` | **Before every PR** — race detector, `-count=3`. |
| `make lint` | `golangci-lint run` — must be clean. |
| `make verify-deps` | Enforces the zero-dependency rule (below). |
| `make bench` / `make bench-ci` | Run benchmarks (see [docs/benchmarks.md](docs/benchmarks.md)). |
| `make test-integration` | Redis-backed distributed tests (needs Docker). |

**2. Know the one rule that will fail your PR if broken**

The **[zero-dependency rule](#the-zero-dependency-rule)**: core algorithm
packages import only the standard library and other core packages. Third-party
imports belong in adapter packages (`contrib/`, `ratelimit/store`,
`ratelimit/middleware`, `observability/`, `metric/`) or test files. `make
verify-deps` enforces this in CI.

**3. Pick something to work on**

- Browse [docs/good-first-issues.md](docs/good-first-issues.md) — 12 scoped
  starter tasks with files and acceptance criteria, each drawn from an unchecked
  item in [ENHANCEMENTS.md](ENHANCEMENTS.md).
- Comment on the tracking issue to claim it before you start.

**4. Pre-PR checklist**

Run these three and make sure they pass before opening a PR:

```bash
make test-race      # go test -race -count=3 ./...
golangci-lint run   # or: make lint
make verify-deps    # core packages must stay dependency-free
```

Then: add/update tests for any behaviour change, update godoc for exported
symbols, add a `## [Unreleased]` note to [CHANGELOG.md](CHANGELOG.md) for
user-facing changes, and fill out the [PR template](.github/PULL_REQUEST_TEMPLATE.md).
See [Pull requests](#pull-requests) for the full list.

## Prerequisites

- **Go 1.24+** (see the `go` directive in `go.mod`).
- **golangci-lint** for linting: <https://golangci-lint.run/welcome/install/>.
- **Docker** (optional) for `make test-integration` and the full stack.
- **Redis** at `localhost:6379` (optional) for running integration tests
  directly on the host.
- **Node.js 20+** and **npm** if you plan to work on the `frontend/` app.

## Development workflow

We use the standard GitHub fork-and-pull model:

1. **Fork** the repository on GitHub.
2. **Clone** your fork and add the upstream remote:
   ```bash
   git clone https://github.com/<you>/Rate-Limiter-Circuit-Breaker.git
   cd Rate-Limiter-Circuit-Breaker
   git remote add upstream https://github.com/sanskarpan/Rate-Limiter-Circuit-Breaker.git
   ```
3. **Branch** off `main` with a descriptive name, e.g.
   `git checkout -b feat/adaptive-limiter-tuning` or `git checkout -b fix/breaker-deadlock`.
4. **Make your change**, keeping commits focused and following the
   [commit-message convention](#commit-messages).
5. **Verify locally** (see below) — `go test -race ./...` and
   `golangci-lint run` must pass.
6. **Push** to your fork and **open a pull request** against `main`. Fill out
   the PR template and reference any related issue.

## Building and testing

All common tasks are exposed as Makefile targets. Run `make help` to list them.

| Target | What it does |
| --- | --- |
| `make test` | Run all unit tests with coverage; prints the total coverage line. |
| `make test-race` | Run tests with the race detector (`-race -count=3`). **Required before every PR.** |
| `make test-unit` | Fast unit tests only (`-short`, no integration). |
| `make lint` | Run `golangci-lint run` with a 5m timeout. |
| `make bench` | Run all benchmarks with `-benchmem`. |
| `make verify-deps` | Assert that core algorithm packages have zero external runtime dependencies. |
| `make build-go` | Build the demo server binary into `bin/demo-server`. |

Before opening a PR, at minimum run:

```bash
go test -race ./...     # or: make test-race
golangci-lint run       # or: make lint
make verify-deps        # core packages must stay dependency-free
```

Coverage is produced by `make test` (writing `coverage.out`); inspect it with
`go tool cover -html=coverage.out`.

## Integration tests (Redis)

The Redis-backed distributed limiters have integration tests guarded by the
`integration` build tag. They require a Redis instance reachable at
`localhost:6379` (override with the `REDIS_ADDR` environment variable).

Start Redis and run them directly:

```bash
docker run --rm -p 6379:6379 redis:7-alpine   # in a separate terminal
go test -tags=integration ./...
```

Or use the Makefile target, which drives Docker for you:

```bash
make test-integration
```

Integration tests are excluded from the default `go test ./...` run, so unit
tests never depend on a running Redis.

## Running the demo server and frontend

The demo server exposes the resilience primitives over HTTP/WebSocket with
Prometheus metrics; the Next.js frontend renders live visualizations.

**Backend (demo server):**

```bash
make build-go
./bin/demo-server
# or run directly:
go run ./server/
```

**Frontend:**

```bash
cd frontend
npm install
npm run dev        # Next.js dev server (default http://localhost:3000)
```

**Full stack via Docker Compose** (server + frontend + Redis + Prometheus + Grafana):

```bash
make docker-run    # docker-compose up --build
```

## Code style

- **Formatting:** all Go code must be `gofmt`-clean. Run `gofmt -l .` (or let
  `golangci-lint`/your editor handle it) before committing. CI rejects
  unformatted code.
- **Linting:** `golangci-lint run` must pass with no new issues. Configuration
  lives in `.golangci.yml`.
- **Documentation:** every exported symbol (types, functions, methods,
  constants) needs a godoc comment that starts with the symbol name. Preview
  docs locally with `make godoc`.
- **Errors:** wrap with `%w` where callers may want to unwrap; prefer typed
  errors (e.g. the timeout package's typed error) over string matching.
- **Concurrency:** the race detector must stay green. Never call `close()` on a
  shutdown channel while holding a mutex (see the deadlock fix in the
  changelog for context).

## The zero-dependency rule

Core packages must have **zero external runtime dependencies** — they may only
import the standard library and other internal core packages. This is enforced
by `make verify-deps` and in CI.

Core packages include the rate-limiter algorithms (`ratelimit/tokenbucket`,
`ratelimit/gcra`, `ratelimit/fixedwindow`, `ratelimit/slidingwindow`,
`ratelimit/leakybucket`, `ratelimit/adaptive`, `ratelimit/composite`),
`circuitbreaker`, `bulkhead`, `retry`, `retry/backoff`, `timeout`, `fallback`,
`pipeline`, and the `internal/` helpers.

**Adapter packages are exempt** and may import third-party libraries:
middleware/interceptors may import gRPC, and the Redis store and distributed
limiters may import `github.com/redis/go-redis`. If you add a dependency to a
core package, `make verify-deps` will fail — move the code into an adapter
package instead.

## Commit messages

We follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<optional scope>): <short summary>
```

Common types: `feat`, `fix`, `docs`, `test`, `refactor`, `perf`, `chore`,
`ci`, `build`. Examples:

```
feat(ratelimit/gcra): add distributed CAS retry budget
fix(circuitbreaker): release mutex before signalling shutdown
docs(contributing): document integration test setup
```

Keep the summary in the imperative mood and under ~72 characters. Use the body
to explain the *why* when it is not obvious.

### How commits become the changelog

Commit types drive release notes automatically. On every `v*` tag, **goreleaser**
generates the GitHub Release notes from the commits since the previous tag (see
the `changelog:` section of [`.goreleaser.yaml`](.goreleaser.yaml)):

- `feat:` → **Features**, `fix:` → **Bug fixes**, `perf:` → **Performance**;
  anything else lands under **Others**.
- `docs:`, `test:`, `chore:`, `ci:`, and merge commits are filtered out as noise.

That is why the commit type matters: a well-typed history produces a clean,
categorized release page with zero extra effort. [CHANGELOG.md](CHANGELOG.md)
remains a hand-curated, human-readable companion in Keep-a-Changelog form — see
its header for the full flow.

## Pull requests

- Fill out the PR template completely and link the issue you are addressing.
- Ensure `go test -race ./...`, `golangci-lint run`, and `make verify-deps`
  all pass.
- Add or update tests for any behavior change; update godoc and relevant docs.
- Add a note to the `## [Unreleased]` section of [CHANGELOG.md](CHANGELOG.md)
  for user-facing changes.
- Keep PRs focused; large unrelated changes are harder to review.

Thanks for contributing!
