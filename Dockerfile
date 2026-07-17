# Multi-stage Dockerfile for the resilience demo server.
# Stage 1: build
# Stage 2: distroless runtime (minimal attack surface)

# ─── Stage 1: build ───────────────────────────────────────────────────────────
FROM golang:1.24-alpine AS builder

WORKDIR /build

# Copy dependency files first for layer caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the binary with CGO disabled and stripped symbols
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build \
    -ldflags="-s -w -X github.com/sanskarpan/resilience/server/version.Version=${VERSION}" \
    -o /build/demo-server \
    ./server/

# ─── Stage 2: distroless runtime ─────────────────────────────────────────────
FROM gcr.io/distroless/static:nonroot AS runtime

LABEL org.opencontainers.image.title="resilience-demo" \
      org.opencontainers.image.description="Rate Limiter & Circuit Breaker demo server" \
      org.opencontainers.image.source="https://github.com/sanskarpan/resilience"

# Copy binary from builder
COPY --from=builder /build/demo-server /demo-server

# Run as nonroot (uid=65532)
USER nonroot:nonroot

EXPOSE 8080 9090

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD ["/demo-server", "-healthcheck"]

ENTRYPOINT ["/demo-server"]
