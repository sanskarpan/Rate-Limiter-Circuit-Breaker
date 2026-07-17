package otel

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// newRecorded builds a provider with the "none" exporter, then attaches a
// tracetest recorder as an additional span processor and installs it globally so
// the package-level span helpers (which resolve the global tracer) are observed.
// It returns the recorder and a cleanup that restores nothing global-specific
// beyond shutting the provider down (tests run in the same package serially).
func newRecorded(t *testing.T) (*tracetest.SpanRecorder, *Provider) {
	t.Helper()
	p, err := New(context.Background(), Config{
		ServiceName:    "test-svc",
		ServiceVersion: "9.9.9",
		Exporter:       ExporterNone,
		SampleRatio:    1,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	sr := tracetest.NewSpanRecorder()
	p.tp.RegisterSpanProcessor(sr)
	// Ensure the global tracer resolves to this provider (New already did it,
	// but be explicit so ordering across tests cannot matter).
	otel.SetTracerProvider(p.tp)
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return sr, p
}

func TestNewRecordsSpansWithExporterNone(t *testing.T) {
	sr, p := newRecorded(t)

	tr := p.Tracer("unit")
	_, span := tr.Start(context.Background(), "hello")
	span.End()

	ended := sr.Ended()
	if len(ended) != 1 {
		t.Fatalf("want 1 ended span, got %d", len(ended))
	}
	if got := ended[0].Name(); got != "hello" {
		t.Fatalf("span name = %q, want hello", got)
	}
	// Resource carries the service attributes even with the none exporter.
	attrs := attribute.NewSet(ended[0].Resource().Attributes()...)
	if v, ok := attrs.Value("service.name"); !ok || v.AsString() != "test-svc" {
		t.Fatalf("service.name resource attr = %v (ok=%v), want test-svc", v, ok)
	}
	if v, ok := attrs.Value("service.version"); !ok || v.AsString() != "9.9.9" {
		t.Fatalf("service.version resource attr = %v (ok=%v), want 9.9.9", v, ok)
	}
}

func TestStartRateLimitAttributes(t *testing.T) {
	sr, _ := newRecorded(t)

	_, span := StartRateLimit(context.Background(), "token_bucket", "user:42")
	EndDecision(span, true)

	ended := sr.Ended()
	if len(ended) != 1 {
		t.Fatalf("want 1 span, got %d", len(ended))
	}
	s := ended[0]
	if s.Name() != "ratelimit.Allow" {
		t.Fatalf("name = %q", s.Name())
	}
	want := map[attribute.Key]attribute.Value{
		AttrRateLimitAlgorithm: attribute.StringValue("token_bucket"),
		AttrRateLimitKey:       attribute.StringValue("user:42"),
		AttrDecisionAllowed:    attribute.BoolValue(true),
		AttrOutcome:            attribute.StringValue(OutcomeAllowed),
	}
	assertAttrs(t, s.Attributes(), want)
}

func TestEndDecisionRejected(t *testing.T) {
	sr, _ := newRecorded(t)

	_, span := StartCircuitBreaker(context.Background(), "payments")
	EndDecision(span, false)

	s := sr.Ended()[0]
	assertAttrs(t, s.Attributes(), map[attribute.Key]attribute.Value{
		AttrBreakerName:     attribute.StringValue("payments"),
		AttrDecisionAllowed: attribute.BoolValue(false),
		AttrOutcome:         attribute.StringValue(OutcomeRejected),
	})
}

func TestStartBulkheadAndEndError(t *testing.T) {
	sr, _ := newRecorded(t)

	_, span := StartBulkhead(context.Background(), "db-pool")
	EndError(span, errors.New("saturated"))

	s := sr.Ended()[0]
	if s.Name() != "bulkhead.Acquire" {
		t.Fatalf("name = %q", s.Name())
	}
	if s.Status().Code != codes.Error {
		t.Fatalf("status code = %v, want Error", s.Status().Code)
	}
	if s.Status().Description != "saturated" {
		t.Fatalf("status desc = %q", s.Status().Description)
	}
	if len(s.Events()) == 0 {
		t.Fatalf("expected a recorded error event")
	}
	assertAttrs(t, s.Attributes(), map[attribute.Key]attribute.Value{
		AttrBulkheadName: attribute.StringValue("db-pool"),
		AttrOutcome:      attribute.StringValue(OutcomeError),
	})
}

func TestEndErrorNilIsOk(t *testing.T) {
	sr, _ := newRecorded(t)

	_, span := StartBulkhead(context.Background(), "ok-pool")
	EndError(span, nil)

	s := sr.Ended()[0]
	if s.Status().Code != codes.Ok {
		t.Fatalf("status code = %v, want Ok", s.Status().Code)
	}
	if len(s.Events()) != 0 {
		t.Fatalf("expected no error events, got %d", len(s.Events()))
	}
}

func TestSamplerNoneStillRecordsRootLocally(t *testing.T) {
	// With SampleRatio 0 and a parent-based never-sampler, a root span is NOT
	// sampled, so the batch/recorder OnEnd only fires for sampled spans. Assert
	// that behavior explicitly so the sampler wiring is covered.
	p, err := New(context.Background(), Config{Exporter: ExporterNone, SampleRatio: 0})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	sr := tracetest.NewSpanRecorder()
	p.tp.RegisterSpanProcessor(sr)

	_, span := p.Tracer("x").Start(context.Background(), "unsampled")
	span.End()

	if len(sr.Ended()) != 0 {
		t.Fatalf("ratio 0 should not sample root spans, got %d", len(sr.Ended()))
	}
}

func TestShutdownIdempotent(t *testing.T) {
	p, err := New(context.Background(), Config{Exporter: ExporterNone, SampleRatio: 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("first Shutdown: %v", err)
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("second Shutdown: %v", err)
	}
	// Nil receiver is also safe.
	var np *Provider
	if err := np.Shutdown(context.Background()); err != nil {
		t.Fatalf("nil Shutdown: %v", err)
	}
}

func TestUnknownExporterErrors(t *testing.T) {
	if _, err := New(context.Background(), Config{Exporter: "kafka"}); err == nil {
		t.Fatal("expected error for unknown exporter")
	}
}

func TestNewInstallsGlobals(t *testing.T) {
	p, err := New(context.Background(), Config{Exporter: ExporterNone, SampleRatio: 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	if otel.GetTextMapPropagator() == nil {
		t.Fatal("expected a global propagator to be installed")
	}
	fields := p.Propagator().Fields()
	if !contains(fields, "traceparent") {
		t.Fatalf("propagator fields %v missing traceparent", fields)
	}
}

// ensure verify uses a fully-typed sdktrace processor at least once so the
// import is meaningful and the recorder implements SpanProcessor.
var _ sdktrace.SpanProcessor = tracetest.NewSpanRecorder()

func assertAttrs(t *testing.T, got []attribute.KeyValue, want map[attribute.Key]attribute.Value) {
	t.Helper()
	set := attribute.NewSet(got...)
	for k, wv := range want {
		gv, ok := set.Value(k)
		if !ok {
			t.Fatalf("missing attribute %q", k)
		}
		if gv != wv {
			t.Fatalf("attribute %q = %v, want %v", k, gv.AsString(), wv.AsString())
		}
	}
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}
