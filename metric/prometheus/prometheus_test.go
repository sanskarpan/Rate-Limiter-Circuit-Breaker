package prometheus

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// gathered is a scraped snapshot indexed for easy assertions. It avoids the
// prometheus/testutil helpers, which pull an extra transitive dep not present
// in go.sum.
type gathered struct {
	counters   map[string]float64 // key: name|label=val,...
	gauges     map[string]float64
	histCounts map[string]uint64
	names      map[string]bool
}

func labelKey(name string, m *dto.Metric) string {
	key := name
	for _, lp := range m.GetLabel() {
		key += "|" + lp.GetName() + "=" + lp.GetValue()
	}
	return key
}

func scrape(t *testing.T, reg *prometheus.Registry) gathered {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	g := gathered{
		counters:   map[string]float64{},
		gauges:     map[string]float64{},
		histCounts: map[string]uint64{},
		names:      map[string]bool{},
	}
	for _, mf := range mfs {
		g.names[mf.GetName()] = true
		for _, m := range mf.GetMetric() {
			key := labelKey(mf.GetName(), m)
			switch mf.GetType() {
			case dto.MetricType_COUNTER:
				g.counters[key] = m.GetCounter().GetValue()
			case dto.MetricType_GAUGE:
				g.gauges[key] = m.GetGauge().GetValue()
			case dto.MetricType_HISTOGRAM:
				g.histCounts[key] = m.GetHistogram().GetSampleCount()
			}
		}
	}
	return g
}

// TestRecorderRoundTrip drives every Recorder method against a fresh registry
// and asserts the resulting series, labels, and values scraped back out.
func TestRecorderRoundTrip(t *testing.T) {
	reg := prometheus.NewRegistry()
	r := New(reg)

	// Rate limiter.
	r.IncAllowed("token_bucket")
	r.IncAllowed("token_bucket")
	r.IncDenied("token_bucket")
	r.IncDenied("gcra")
	r.ObserveDecision("token_bucket", 250*time.Microsecond)

	// Circuit breaker.
	r.RecordCBState("primary", "closed")
	r.RecordCBState("primary", "open") // last write wins → gauge should read 2 (open)
	r.IncCBResult("primary", "success")
	r.IncCBResult("primary", "success")
	r.IncCBResult("primary", "failure")
	r.IncCBResult("primary", "rejected")
	r.ObserveCBExecution("primary", 10*time.Millisecond)
	r.IncCBTransition("primary", "closed", "open")

	// Bulkhead.
	r.SetBulkheadInflight("pool", 3)
	r.IncBulkheadRejected("pool")
	r.IncBulkheadRejected("pool")

	g := scrape(t, reg)

	counterChecks := map[string]float64{
		"resilience_ratelimit_requests_total|algorithm=token_bucket|result=allowed":          2,
		"resilience_ratelimit_requests_total|algorithm=token_bucket|result=denied":           1,
		"resilience_ratelimit_requests_total|algorithm=gcra|result=denied":                   1,
		"resilience_circuitbreaker_requests_total|name=primary|result=success":               2,
		"resilience_circuitbreaker_requests_total|name=primary|result=failure":               1,
		"resilience_circuitbreaker_requests_total|name=primary|result=rejected":              1,
		"resilience_circuitbreaker_state_transitions_total|from=closed|name=primary|to=open": 1,
		"resilience_bulkhead_rejected_total|name=pool":                                       2,
	}
	for key, want := range counterChecks {
		if got := g.counters[key]; got != want {
			t.Errorf("counter %s = %v, want %v", key, got, want)
		}
	}

	gaugeChecks := map[string]float64{
		"resilience_circuitbreaker_state|name=primary": stateOpen,
		"resilience_bulkhead_inflight|name=pool":       3,
	}
	for key, want := range gaugeChecks {
		if got := g.gauges[key]; got != want {
			t.Errorf("gauge %s = %v, want %v", key, got, want)
		}
	}

	histChecks := map[string]uint64{
		"resilience_ratelimit_decision_duration_seconds|algorithm=token_bucket": 1,
		"resilience_circuitbreaker_execution_duration_seconds|name=primary":     1,
	}
	for key, want := range histChecks {
		if got := g.histCounts[key]; got != want {
			t.Errorf("histogram %s sample count = %v, want %v", key, got, want)
		}
	}

	// Every documented metric family must be present.
	for _, name := range []string{
		"resilience_ratelimit_requests_total",
		"resilience_ratelimit_decision_duration_seconds",
		"resilience_circuitbreaker_state",
		"resilience_circuitbreaker_requests_total",
		"resilience_circuitbreaker_state_transitions_total",
		"resilience_circuitbreaker_execution_duration_seconds",
		"resilience_bulkhead_inflight",
		"resilience_bulkhead_rejected_total",
	} {
		if !g.names[name] {
			t.Errorf("expected metric family %q not found in gather output", name)
		}
	}
}

// TestNewIdempotentOnSameRegistry verifies that calling New twice against the
// same registry does not panic and rebinds to the already-registered
// collectors so updates remain visible on the shared registry.
func TestNewIdempotentOnSameRegistry(t *testing.T) {
	reg := prometheus.NewRegistry()
	r1 := New(reg)
	r2 := New(reg) // must not panic

	r1.IncAllowed("token_bucket")
	r2.IncAllowed("token_bucket")

	g := scrape(t, reg)
	key := "resilience_ratelimit_requests_total|algorithm=token_bucket|result=allowed"
	if got := g.counters[key]; got != 2 {
		t.Errorf("shared allowed counter = %v, want 2 (New must rebind to existing collector)", got)
	}
}

// TestUnknownCBStateIgnored asserts an unknown state string is a no-op rather
// than corrupting the gauge.
func TestUnknownCBStateIgnored(t *testing.T) {
	reg := prometheus.NewRegistry()
	r := New(reg)
	r.RecordCBState("x", "half-open")
	r.RecordCBState("x", "bogus") // ignored

	g := scrape(t, reg)
	if got := g.gauges["resilience_circuitbreaker_state|name=x"]; got != stateHalfOpen {
		t.Errorf("state{x} = %v, want %v (unknown state must be ignored)", got, stateHalfOpen)
	}
}
