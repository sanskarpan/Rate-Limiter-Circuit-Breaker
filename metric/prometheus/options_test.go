package prometheus

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"go.opentelemetry.io/otel/trace"
)

// findHistogram gathers reg and returns the raw dto.Histogram for the named
// metric family (first metric with any labels). It fails the test if absent.
func findHistogram(t *testing.T, reg *prometheus.Registry, name string) *dto.Histogram {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if h := m.GetHistogram(); h != nil {
				return h
			}
		}
	}
	t.Fatalf("histogram family %q not found", name)
	return nil
}

// TestNativeHistogramsDisabledByDefault asserts that without the option the
// duration histograms carry classic buckets and no native-histogram schema —
// i.e. behavior is unchanged for existing callers.
func TestNativeHistogramsDisabledByDefault(t *testing.T) {
	reg := prometheus.NewRegistry()
	r := New(reg)
	r.ObserveDecision("token_bucket", 250*time.Microsecond)

	h := findHistogram(t, reg, "resilience_ratelimit_decision_duration_seconds")
	if len(h.GetBucket()) == 0 {
		t.Fatal("expected classic buckets to be present by default")
	}
	// A classic-only histogram exposes no positive/negative sparse spans and a
	// zero schema field (nil pointer).
	if h.Schema != nil {
		t.Errorf("expected no native-histogram schema by default, got schema=%d", h.GetSchema())
	}
}

// TestWithNativeHistograms asserts the option produces a native-histogram-
// enabled collector: the gathered histogram carries a native schema while the
// classic buckets remain for backward compatibility.
func TestWithNativeHistograms(t *testing.T) {
	reg := prometheus.NewRegistry()
	r := New(reg, WithNativeHistograms(1.1))
	r.ObserveDecision("token_bucket", 250*time.Microsecond)
	r.ObserveCBExecution("primary", 10*time.Millisecond)

	for _, name := range []string{
		"resilience_ratelimit_decision_duration_seconds",
		"resilience_circuitbreaker_execution_duration_seconds",
	} {
		h := findHistogram(t, reg, name)
		if h.Schema == nil {
			t.Errorf("%s: expected native-histogram schema to be set", name)
		}
		if len(h.GetBucket()) == 0 {
			t.Errorf("%s: classic buckets must remain for compatibility", name)
		}
	}
}

// TestWithNativeHistogramsBadFactorFallsBack asserts an invalid (≤1) factor is
// replaced by the sane default rather than producing an invalid collector.
func TestWithNativeHistogramsBadFactorFallsBack(t *testing.T) {
	reg := prometheus.NewRegistry()
	r := New(reg, WithNativeHistograms(0.5)) // invalid → default 1.1
	r.ObserveDecision("token_bucket", 1*time.Millisecond)

	h := findHistogram(t, reg, "resilience_ratelimit_decision_duration_seconds")
	if h.Schema == nil {
		t.Fatal("expected native schema even with a fallback factor")
	}
}

// sampledContext returns a context carrying a synthetic, sampled span context
// with the given trace ID, without needing a real SDK/exporter.
func sampledContext(traceHex, spanHex string) context.Context {
	tid, _ := trace.TraceIDFromHex(traceHex)
	sid, _ := trace.SpanIDFromHex(spanHex)
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: trace.FlagsSampled,
	})
	return trace.ContextWithSpanContext(context.Background(), sc)
}

// classicExemplar returns the exemplar attached to any classic bucket of the
// named histogram, or nil if none is present.
func classicExemplar(t *testing.T, reg *prometheus.Registry, name string) *dto.Exemplar {
	t.Helper()
	h := findHistogram(t, reg, name)
	for _, b := range h.GetBucket() {
		if ex := b.GetExemplar(); ex != nil {
			return ex
		}
	}
	return nil
}

// TestExemplarAttachedWhenTracePresent verifies that with WithExemplars and a
// sampled span in context, the Ctx observe methods attach a trace_id exemplar.
func TestExemplarAttachedWhenTracePresent(t *testing.T) {
	reg := prometheus.NewRegistry()
	r := New(reg, WithExemplars(true))

	const traceHex = "4bf92f3577b34da6a3ce929d0e0e4736"
	ctx := sampledContext(traceHex, "00f067aa0ba902b7")
	r.ObserveDecisionCtx(ctx, "token_bucket", 250*time.Microsecond)

	ex := classicExemplar(t, reg, "resilience_ratelimit_decision_duration_seconds")
	if ex == nil {
		t.Fatal("expected an exemplar to be attached when a sampled span is present")
	}
	var gotTrace string
	for _, lp := range ex.GetLabel() {
		if lp.GetName() == "trace_id" {
			gotTrace = lp.GetValue()
		}
	}
	if gotTrace != traceHex {
		t.Errorf("exemplar trace_id = %q, want %q", gotTrace, traceHex)
	}
}

// TestNoExemplarWhenTraceAbsent verifies that no exemplar is attached when the
// context carries no span, even with exemplars enabled.
func TestNoExemplarWhenTraceAbsent(t *testing.T) {
	reg := prometheus.NewRegistry()
	r := New(reg, WithExemplars(true))

	r.ObserveDecisionCtx(context.Background(), "token_bucket", 250*time.Microsecond)

	if ex := classicExemplar(t, reg, "resilience_ratelimit_decision_duration_seconds"); ex != nil {
		t.Errorf("expected no exemplar when context has no span, got %+v", ex)
	}
}

// TestNoExemplarWhenDisabled verifies that even with a sampled span present, no
// exemplar is attached when WithExemplars was not enabled (default behavior).
func TestNoExemplarWhenDisabled(t *testing.T) {
	reg := prometheus.NewRegistry()
	r := New(reg) // exemplars off (default)

	ctx := sampledContext("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7")
	r.ObserveDecisionCtx(ctx, "token_bucket", 250*time.Microsecond)
	r.ObserveCBExecutionCtx(ctx, "primary", 10*time.Millisecond)

	if ex := classicExemplar(t, reg, "resilience_ratelimit_decision_duration_seconds"); ex != nil {
		t.Errorf("expected no exemplar when exemplars disabled, got %+v", ex)
	}
}

// TestCtxObserveStillRecordsSample verifies the Ctx observe methods record the
// sample regardless of exemplar/trace state (they are drop-in replacements).
func TestCtxObserveStillRecordsSample(t *testing.T) {
	reg := prometheus.NewRegistry()
	r := New(reg, WithExemplars(true))

	r.ObserveDecisionCtx(context.Background(), "token_bucket", 1*time.Millisecond)
	r.ObserveCBExecutionCtx(sampledContext("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7"), "primary", 1*time.Millisecond)

	if got := findHistogram(t, reg, "resilience_ratelimit_decision_duration_seconds").GetSampleCount(); got != 1 {
		t.Errorf("decision sample count = %d, want 1", got)
	}
	if got := findHistogram(t, reg, "resilience_circuitbreaker_execution_duration_seconds").GetSampleCount(); got != 1 {
		t.Errorf("cb execution sample count = %d, want 1", got)
	}
}
