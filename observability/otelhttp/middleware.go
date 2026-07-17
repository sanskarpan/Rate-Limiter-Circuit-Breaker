// Package otelhttp provides a minimal, dependency-light net/http tracing
// middleware built directly on the OpenTelemetry trace and propagation APIs. It
// deliberately does NOT depend on the otelhttp contrib module: it extracts the
// incoming W3C TraceContext, starts a server span per request, records
// method/route/status attributes, and re-injects the active span context into
// the request so downstream handlers propagate it.
package otelhttp

import (
	"bufio"
	"net"
	"net/http"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// statusRecorder wraps http.ResponseWriter to capture the response status code.
// It defaults to 200 because a handler that writes a body without an explicit
// WriteHeader implies 200 OK.
type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

// Hijack lets WebSocket upgrades (and other hijacking handlers) work when this
// middleware wraps the ResponseWriter. Without it, wrapping breaks conn hijack.
func (r *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := r.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}
	return nil, nil, http.ErrNotSupported
}

// Flush proxies http.Flusher so streaming/SSE handlers keep working.
func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (r *statusRecorder) WriteHeader(code int) {
	if !r.wroteHeader {
		r.status = code
		r.wroteHeader = true
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if !r.wroteHeader {
		r.status = http.StatusOK
		r.wroteHeader = true
	}
	return r.ResponseWriter.Write(b)
}

// Middleware returns net/http middleware that traces each request under a server
// span. tracerName is the instrumentation scope; empty falls back to a package
// default. The tracer and propagator are resolved from the global OTel provider
// installed by observability/otel.New, so no wiring is required beyond that.
func Middleware(tracerName string) func(http.Handler) http.Handler {
	if tracerName == "" {
		tracerName = "github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/observability/otelhttp"
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			propagator := otel.GetTextMapPropagator()
			tracer := otel.Tracer(tracerName)

			// Extract any incoming W3C trace context from the request headers.
			ctx := propagator.Extract(r.Context(), propagation.HeaderCarrier(r.Header))

			spanName := r.Method + " " + r.URL.Path
			ctx, span := tracer.Start(ctx, spanName,
				trace.WithSpanKind(trace.SpanKindServer),
				trace.WithAttributes(
					semconv.HTTPRequestMethodKey.String(r.Method),
					semconv.URLPath(r.URL.Path),
				),
			)
			defer span.End()

			// Inject the active (server) span context back into the request
			// headers so it propagates to any outbound calls the handler makes.
			r = r.WithContext(ctx)
			propagator.Inject(ctx, propagation.HeaderCarrier(r.Header))

			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rec, r)

			span.SetAttributes(
				semconv.HTTPResponseStatusCode(rec.status),
			)
			if rec.status >= http.StatusInternalServerError {
				span.SetStatus(codes.Error, http.StatusText(rec.status))
			}
		})
	}
}
