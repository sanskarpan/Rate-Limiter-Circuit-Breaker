# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-02-23

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
- Deadlock in `Close()` across all 5 rate limiter packages: `close(done)` was called
  while holding `mu.Lock()`, causing cleanup goroutines to starve under CPU contention.
  Fixed by releasing mutex before signalling shutdown channel.
- `go vet` context leak in `fallback.Hedge()`: restructured to use `defer backupCancel()`
  after the hedge timer fires, ensuring cancel is always called.

### Performance
- Token Bucket: 62 ns/op, 0 allocs
- GCRA: 67 ns/op, 0 allocs
- Fixed Window: 73 ns/op, 0 allocs
- Sliding Window Counter: 71 ns/op, 0 allocs
- Circuit Breaker (closed): 82 ns/op, 0 allocs
- All core packages: zero external runtime dependencies
