// Package telemetry provides OpenTelemetry-compatible tracing stubs for the demo server.
// When OTEL_ENABLED=true is set, wire in a real OTel SDK by replacing the Tracer variable
// with a configured TracerProvider.Tracer() call during server initialisation.
// By default all operations are no-ops with zero overhead.
package telemetry

import (
	"context"
	"net/http"
	"os"
)

// Enabled reports whether tracing is active.
var Enabled = os.Getenv("OTEL_ENABLED") == "true"

// Span is a no-op span used when OTel is not configured.
type Span struct {
	name string
}

// End is a no-op.
func (s *Span) End() {}

// SetAttribute is a no-op.
func (s *Span) SetAttribute(key string, value any) {}

// StartSpan starts a named trace span. Returns ctx unchanged and a no-op Span
// when OTEL_ENABLED is false.
func StartSpan(ctx context.Context, name string) (context.Context, *Span) {
	return ctx, &Span{name: name}
}

// StartHTTPSpan starts a span for an incoming HTTP request.
func StartHTTPSpan(ctx context.Context, r *http.Request) (context.Context, *Span) {
	return StartSpan(ctx, r.Method+" "+r.URL.Path)
}

// StartRateLimitSpan starts a span for a rate limiter Allow call.
func StartRateLimitSpan(ctx context.Context, algorithm, key string) (context.Context, *Span) {
	return StartSpan(ctx, "ratelimit.Allow/"+algorithm)
}

// StartCBSpan starts a span for a circuit breaker Execute call.
func StartCBSpan(ctx context.Context, name string) (context.Context, *Span) {
	return StartSpan(ctx, "circuitbreaker.Execute/"+name)
}

// Shutdown is a no-op shutdown function. Replace with real tp.Shutdown when OTel is wired in.
func Shutdown(_ context.Context) error { return nil }
