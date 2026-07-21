# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## How this changelog is produced

Commits follow [Conventional Commits](https://www.conventionalcommits.org/)
(`feat`, `fix`, `perf`, `docs`, `test`, `chore`, `ci`, `build`; see
[CONTRIBUTING.md](CONTRIBUTING.md#commit-messages)). On every `v*` tag,
**goreleaser** generates the GitHub Release notes automatically from those
commits (see the `changelog:` section of [`.goreleaser.yaml`](.goreleaser.yaml)):

- `feat:` → **Features**, `fix:` → **Bug fixes**, `perf:` → **Performance**;
  everything else falls under **Others**.
- `docs:`, `test:`, `chore:`, `ci:`, and merge commits are filtered out as noise.

So the **GitHub Release page is generated** from commit history, while **this
file is curated** in Keep-a-Changelog form for a human-readable, categorized
narrative. When cutting a release: move the `## [Unreleased]` entries into a new
dated version section, then tag — goreleaser fills in the machine-generated
notes from the commits since the previous tag.

## [Unreleased]

### Added

### Changed

### Fixed

## [1.0.0] - 2026-07-21

### Added

- Prometheus recording & alerting rules under `deploy/prometheus/`
  (`rules.yml`, `alerts.yml`) covering rate-limit denial ratio, circuit-breaker
  stuck-open, bulkhead saturation, and decision-latency SLOs, plus a `README.md`
  documenting how to load them and wire Alertmanager.

### Changed

- The public API is now stable under [Semantic Versioning](https://semver.org/) v1.0.0.
  No breaking changes will be made within the v1.x major version.

## [0.1.0] - 2026-07-17

### Added

- Token Bucket rate limiter with lazy refill and burst support
- Leaky Bucket rate limiter with queue-based constant output rate
- Fixed Window Counter rate limiter
- Sliding Window Log rate limiter (exact)
- Sliding Window Counter rate limiter (approximate, memory efficient)
- GCRA (Generic Cell Rate Algorithm) rate limiter
- Adaptive Rate Limiter with runtime signal feedback
- Composite/Chained Limiter with AND/OR semantics
- Circuit Breaker with count-based and time-based windows
- Circuit Breaker Registry for named breakers
- Bulkhead concurrency limiter (semaphore + thread pool)
- Retry with 5 backoff strategies (constant, exponential, full jitter, equal jitter, decorrelated jitter)
- Timeout wrapper with typed error
- Fallback and Hedge request patterns (including `HedgeN` speculative execution)
- Resilience Pipeline builder
- In-memory Store with TTL eviction
- HTTP middleware with full RFC rate limit headers
- gRPC unary and streaming interceptors
- Redis-backed distributed rate limiter (Lua CAS scripts)
- Demo server with WebSocket streaming and Prometheus metrics
- Next.js visualization frontend with real-time charts
- Multi-stage Docker build (Alpine builder + distroless runtime)
- Docker Compose stack (server + frontend + Redis + Prometheus + Grafana)
- Kubernetes manifests (Deployment, Service, ConfigMap, HPA, PDB)
- Grafana dashboard for rate limiter + circuit breaker observability
- GitHub Actions CI (race detector, zero-dep verification, lint, fuzz)
- Structured slog-based logger with request ID propagation
- Security headers middleware

### Fixed

- Deadlock in `Close()` across all 5 rate limiter packages: `close(done)` was
  called while holding `mu.Lock()`, causing cleanup goroutines to starve under
  CPU contention. Fixed by releasing the mutex before signalling the shutdown
  channel.
- `go vet` context leak in `fallback.Hedge()`: restructured to use
  `defer backupCancel()` after the hedge timer fires, ensuring cancel is always
  called.

### Performance

- Token Bucket: 62 ns/op, 0 allocs
- GCRA: 67 ns/op, 0 allocs
- Fixed Window: 73 ns/op, 0 allocs
- Sliding Window Counter: 71 ns/op, 0 allocs
- Circuit Breaker (closed): 82 ns/op, 0 allocs
- All core packages: zero external runtime dependencies

[Unreleased]: https://github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/compare/v0.1.0...v1.0.0
[0.1.0]: https://github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/releases/tag/v0.1.0
