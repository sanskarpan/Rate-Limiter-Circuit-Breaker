# Multi-stage Dockerfile for the resilience demo server.
# Stage 1: build
# Stage 2: distroless runtime (minimal attack surface)

# ─── Stage 1: build ───────────────────────────────────────────────────────────
FROM golang:1.25-alpine AS builder

WORKDIR /build

# The demo server lives in its own nested Go module (server/) whose go.mod has
# `replace github.com/sanskarpan/Rate-Limiter-Circuit-Breaker => ../`, so the
# parent (library) module MUST also be present in the image for the build to
# resolve. Copy both modules' dependency manifests first for layer caching, then
# download the server module's deps from within server/.
COPY go.mod go.sum ./
COPY server/go.mod server/go.sum ./server/
RUN cd server && go mod download

# Copy the full source tree (parent library module + nested server module).
COPY . .

# Build the binary from the server module (WORKDIR /build/server); the replace
# directive resolves the library at ../. The version package import path is
# unchanged by the module split.
ARG VERSION=dev
RUN cd server && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build \
    -ldflags="-s -w -X github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/server/version.Version=${VERSION}" \
    -o /build/demo-server \
    .

# ─── Stage 2: distroless runtime ─────────────────────────────────────────────
FROM gcr.io/distroless/static:nonroot AS runtime

LABEL org.opencontainers.image.title="resilience-demo" \
      org.opencontainers.image.description="Rate Limiter & Circuit Breaker demo server" \
      org.opencontainers.image.source="https://github.com/sanskarpan/Rate-Limiter-Circuit-Breaker"

# Copy binary from builder
COPY --from=builder /build/demo-server /demo-server

# Run as nonroot (uid=65532)
USER nonroot:nonroot

EXPOSE 8080 9090

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD ["/demo-server", "-healthcheck"]

ENTRYPOINT ["/demo-server"]
