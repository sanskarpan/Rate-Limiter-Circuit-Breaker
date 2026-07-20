// Package integration hosts container-based integration tests for the
// resilience library (ENHANCEMENTS §6.3). It is a SEPARATE Go module so that
// the heavy testcontainers-go dependency graph never touches the zero-dependency
// core module (root go.mod).
//
// The tests spin up a real Redis container in-process via testcontainers-go and
// run the distributed circuit-breaker and distributed token-bucket parity
// checks against it — the same shared-state paths used in production, but
// hermetic and one-command, with no reliance on an externally-provided Redis or
// a CI service container.
//
// # Running
//
//	cd integration
//	go test -tags=integration ./...
//
// Requirements: a working Docker daemon (or a compatible runtime such as
// Podman/Colima with DOCKER_HOST set). The build tag keeps the container tests
// from running (and from requiring the heavy deps to be present) unless
// explicitly requested.
//
// # Fallback without Docker
//
// If Docker is unavailable, the same parity assertions can be run against a
// Redis started via the repo's docker-compose.yml and the core module's own
// integration tests:
//
//	docker compose -f ../docker-compose.yml up -d redis
//	cd ..
//	REDIS_ADDR=localhost:6379 go test -tags=integration ./circuitbreaker/ ./ratelimit/...
//
// See ../docs/mutation-testing.md and this module's README-style comments for
// details.
package integration
