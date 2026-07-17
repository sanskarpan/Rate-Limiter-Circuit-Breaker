package otelhttp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// setup installs a recording TracerProvider and a W3C propagator globally, so
// the middleware (which resolves both from the globals) is observable and
// deterministic. No network is involved.
func setup(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(sr),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	return sr
}

func TestMiddlewareProducesServerSpan(t *testing.T) {
	sr := setup(t)

	h := Middleware("http-test")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}))

	req := httptest.NewRequest(http.MethodPost, "/v1/allow", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	ended := sr.Ended()
	if len(ended) != 1 {
		t.Fatalf("want 1 span, got %d", len(ended))
	}
	s := ended[0]
	if s.SpanKind() != trace.SpanKindServer {
		t.Fatalf("span kind = %v, want Server", s.SpanKind())
	}
	if s.Name() != "POST /v1/allow" {
		t.Fatalf("span name = %q", s.Name())
	}
	set := attribute.NewSet(s.Attributes()...)
	if v, ok := set.Value("http.request.method"); !ok || v.AsString() != "POST" {
		t.Fatalf("method attr = %v (ok=%v)", v, ok)
	}
	if v, ok := set.Value("url.path"); !ok || v.AsString() != "/v1/allow" {
		t.Fatalf("url.path attr = %v (ok=%v)", v, ok)
	}
	if v, ok := set.Value("http.response.status_code"); !ok || v.AsInt64() != int64(http.StatusTeapot) {
		t.Fatalf("status attr = %v (ok=%v)", v, ok)
	}
}

func TestMiddlewareErrorStatusSetsErrorCode(t *testing.T) {
	sr := setup(t)
	h := Middleware("")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/boom", nil))

	s := sr.Ended()[0]
	if s.Status().Code != codes.Error {
		t.Fatalf("status code = %v, want Error", s.Status().Code)
	}
}

func TestMiddlewareDefaultStatus200(t *testing.T) {
	sr := setup(t)
	h := Middleware("")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok")) // no explicit WriteHeader -> 200
	}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))

	s := sr.Ended()[0]
	set := attribute.NewSet(s.Attributes()...)
	if v, _ := set.Value("http.response.status_code"); v.AsInt64() != 200 {
		t.Fatalf("default status = %v, want 200", v.AsInt64())
	}
	if s.Status().Code == codes.Error {
		t.Fatalf("2xx should not be error status")
	}
}

// TestMiddlewareExtractsIncomingContext verifies the middleware joins an
// incoming W3C trace: the server span must be a child of the injected remote
// trace ID (extract side of the round-trip).
func TestMiddlewareExtractsIncomingContext(t *testing.T) {
	sr := setup(t)

	// Build a remote parent span context and inject it into request headers.
	traceID, _ := trace.TraceIDFromHex("0123456789abcdef0123456789abcdef")
	spanID, _ := trace.SpanIDFromHex("0123456789abcdef")
	remote := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
	carrierCtx := trace.ContextWithSpanContext(context.Background(), remote)

	req := httptest.NewRequest(http.MethodGet, "/downstream", nil)
	otel.GetTextMapPropagator().Inject(carrierCtx, propagation.HeaderCarrier(req.Header))

	var childTraceID trace.TraceID
	h := Middleware("")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		childTraceID = trace.SpanFromContext(r.Context()).SpanContext().TraceID()
	}))
	h.ServeHTTP(httptest.NewRecorder(), req)

	if childTraceID != traceID {
		t.Fatalf("server span trace ID = %s, want inherited %s", childTraceID, traceID)
	}
	s := sr.Ended()[0]
	if s.Parent().SpanID() != spanID {
		t.Fatalf("server span parent = %s, want remote %s", s.Parent().SpanID(), spanID)
	}
}

// TestInjectExtractRoundTrip exercises the propagator round-trip independently of
// the middleware: inject a context into headers, extract it back, and confirm the
// trace ID survives.
func TestInjectExtractRoundTrip(t *testing.T) {
	setup(t)
	prop := otel.GetTextMapPropagator()

	traceID, _ := trace.TraceIDFromHex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	spanID, _ := trace.SpanIDFromHex("bbbbbbbbbbbbbbbb")
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	h := http.Header{}
	prop.Inject(ctx, propagation.HeaderCarrier(h))
	if h.Get("traceparent") == "" {
		t.Fatal("expected traceparent header after Inject")
	}

	got := trace.SpanContextFromContext(prop.Extract(context.Background(), propagation.HeaderCarrier(h)))
	if got.TraceID() != traceID {
		t.Fatalf("round-trip trace ID = %s, want %s", got.TraceID(), traceID)
	}
	if got.SpanID() != spanID {
		t.Fatalf("round-trip span ID = %s, want %s", got.SpanID(), spanID)
	}
}
