// Package prometheus provides a Prometheus adapter implementing the
// metric.Recorder interface. It bridges the zero-dependency core (which knows
// only about metric.Recorder) to the Prometheus client, translating each
// Recorder event into an update on a bounded-cardinality metric.
//
// Cardinality safety: every label is one of algorithm / name / result / from /
// to — all drawn from a small, fixed set fixed at construction time. No
// per-request key is ever used as a label, so the metric series count stays
// bounded regardless of traffic (a cost/DoS guardrail).
//
// The emitted series names match the shipped Grafana dashboard at
// deploy/grafana/dashboards/resilience.json:
//
//	resilience_ratelimit_requests_total{algorithm,result}
//	resilience_ratelimit_decision_duration_seconds{algorithm}   (histogram)
//	resilience_circuitbreaker_state{name}                       (gauge; 0=closed,1=half-open,2=open)
//	resilience_circuitbreaker_requests_total{name,result}
//	resilience_circuitbreaker_state_transitions_total{name,from,to}
//	resilience_circuitbreaker_execution_duration_seconds{name}  (histogram)
//	resilience_bulkhead_inflight{name}                          (gauge)
//	resilience_bulkhead_rejected_total{name}
package prometheus

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/trace"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/metric"
)

// State gauge encoding. These MUST match the dashboard value mappings and the
// documented contract on metric.Recorder.RecordCBState.
const (
	stateClosed   = 0.0
	stateHalfOpen = 1.0
	stateOpen     = 2.0
)

// Recorder is a metric.Recorder backed by Prometheus metrics registered on a
// caller-supplied registry. It is safe for concurrent use (all Prometheus
// metric types are).
type Recorder struct {
	rlRequests    *prometheus.CounterVec
	rlDecision    *prometheus.HistogramVec
	cbState       *prometheus.GaugeVec
	cbRequests    *prometheus.CounterVec
	cbTransitions *prometheus.CounterVec
	cbExecution   *prometheus.HistogramVec
	bhInflight    *prometheus.GaugeVec
	bhRejected    *prometheus.CounterVec

	// exemplars, when true, causes the Ctx-variant observe methods to attach a
	// trace_id exemplar to histogram samples whenever the supplied context
	// carries a sampled span. It has no effect on the context-free
	// ObserveDecision / ObserveCBExecution methods used by the core.
	exemplars bool
}

// Compile-time assertion that *Recorder satisfies the core interface.
var _ metric.Recorder = (*Recorder)(nil)

// New builds a Recorder and registers its metrics on reg. It panics if any
// metric fails to register (e.g. a duplicate registration on the same
// registry) — consistent with promauto semantics — because a mis-wired metrics
// pipeline is a programming error that should surface immediately at startup.
//
// Pass prometheus.DefaultRegisterer to expose the series through the default
// promhttp handler.
//
// Optional functional options (WithNativeHistograms, WithExemplars) tune the
// duration histograms. With no options the behavior is identical to previous
// releases (classic fixed-bucket histograms, no exemplars), so existing callers
// need no changes.
func New(reg prometheus.Registerer, opts ...Option) *Recorder {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	cfg := defaultConfig()
	for _, o := range opts {
		o(&cfg)
	}
	r := &Recorder{exemplars: cfg.exemplars}
	r.rlRequests = registerCounterVec(reg, prometheus.CounterOpts{
		Name: "resilience_ratelimit_requests_total",
		Help: "Total rate-limit decisions, partitioned by algorithm and result (allowed|denied).",
	}, []string{"algorithm", "result"})

	rlDecisionOpts := prometheus.HistogramOpts{
		Name:    "resilience_ratelimit_decision_duration_seconds",
		Help:    "Rate-limit decision latency in seconds, partitioned by algorithm.",
		Buckets: decisionBuckets,
	}
	cfg.applyNative(&rlDecisionOpts)
	r.rlDecision = registerHistogramVec(reg, rlDecisionOpts, []string{"algorithm"})

	r.cbState = registerGaugeVec(reg, prometheus.GaugeOpts{
		Name: "resilience_circuitbreaker_state",
		Help: "Circuit breaker state gauge (0=closed, 1=half-open, 2=open).",
	}, []string{"name"})

	r.cbRequests = registerCounterVec(reg, prometheus.CounterOpts{
		Name: "resilience_circuitbreaker_requests_total",
		Help: "Total circuit-breaker executions, partitioned by name and result (success|failure|rejected).",
	}, []string{"name", "result"})

	r.cbTransitions = registerCounterVec(reg, prometheus.CounterOpts{
		Name: "resilience_circuitbreaker_state_transitions_total",
		Help: "Total circuit-breaker state transitions, partitioned by name and from/to state.",
	}, []string{"name", "from", "to"})

	cbExecOpts := prometheus.HistogramOpts{
		Name:    "resilience_circuitbreaker_execution_duration_seconds",
		Help:    "Circuit-breaker protected-call latency in seconds, partitioned by name.",
		Buckets: prometheus.DefBuckets,
	}
	cfg.applyNative(&cbExecOpts)
	r.cbExecution = registerHistogramVec(reg, cbExecOpts, []string{"name"})

	r.bhInflight = registerGaugeVec(reg, prometheus.GaugeOpts{
		Name: "resilience_bulkhead_inflight",
		Help: "Current number of in-flight executions per bulkhead.",
	}, []string{"name"})

	r.bhRejected = registerCounterVec(reg, prometheus.CounterOpts{
		Name: "resilience_bulkhead_rejected_total",
		Help: "Total requests rejected by a bulkhead because no slot was available.",
	}, []string{"name"})

	return r
}

// decisionBuckets are tuned for sub-millisecond in-memory rate-limit decisions
// (down to 100ns) while still covering distributed/Redis-backed round trips.
var decisionBuckets = []float64{
	100e-9, 500e-9, // 100ns, 500ns
	1e-6, 5e-6, 10e-6, 50e-6, 100e-6, 500e-6, // µs range
	1e-3, 5e-3, 10e-3, 50e-3, 100e-3, // ms range
}

// The register* helpers add a freshly-built vec to reg, tolerating a duplicate
// registration by rebinding to the collector that already exists. This keeps
// New idempotent across repeated calls against the same registry (common in
// tests / when the demo server router is built more than once) instead of
// panicking on the second call, and — crucially — ensures the returned vec is
// the one actually registered so updates are visible on the shared registry.

func registerCounterVec(reg prometheus.Registerer, opts prometheus.CounterOpts, labels []string) *prometheus.CounterVec {
	c := prometheus.NewCounterVec(opts, labels)
	if err := reg.Register(c); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			return are.ExistingCollector.(*prometheus.CounterVec)
		}
		panic(err)
	}
	return c
}

func registerGaugeVec(reg prometheus.Registerer, opts prometheus.GaugeOpts, labels []string) *prometheus.GaugeVec {
	c := prometheus.NewGaugeVec(opts, labels)
	if err := reg.Register(c); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			return are.ExistingCollector.(*prometheus.GaugeVec)
		}
		panic(err)
	}
	return c
}

func registerHistogramVec(reg prometheus.Registerer, opts prometheus.HistogramOpts, labels []string) *prometheus.HistogramVec {
	c := prometheus.NewHistogramVec(opts, labels)
	if err := reg.Register(c); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			return are.ExistingCollector.(*prometheus.HistogramVec)
		}
		panic(err)
	}
	return c
}

// IncAllowed increments the allowed rate-limit counter for algorithm.
func (r *Recorder) IncAllowed(algorithm string) {
	r.rlRequests.WithLabelValues(algorithm, "allowed").Inc()
}

// IncDenied increments the denied rate-limit counter for algorithm.
func (r *Recorder) IncDenied(algorithm string) {
	r.rlRequests.WithLabelValues(algorithm, "denied").Inc()
}

// ObserveDecision records a rate-limit decision latency for algorithm.
func (r *Recorder) ObserveDecision(algorithm string, d time.Duration) {
	r.rlDecision.WithLabelValues(algorithm).Observe(d.Seconds())
}

// RecordCBState sets the circuit-breaker state gauge. Unknown state strings
// leave the gauge unchanged (defensive; the core only sends the three valid
// values).
func (r *Recorder) RecordCBState(name, state string) {
	var v float64
	switch state {
	case "closed":
		v = stateClosed
	case "half-open":
		v = stateHalfOpen
	case "open":
		v = stateOpen
	default:
		return
	}
	r.cbState.WithLabelValues(name).Set(v)
}

// IncCBResult increments the circuit-breaker result counter.
func (r *Recorder) IncCBResult(name, result string) {
	r.cbRequests.WithLabelValues(name, result).Inc()
}

// ObserveCBExecution records a circuit-breaker protected-call latency.
func (r *Recorder) ObserveCBExecution(name string, d time.Duration) {
	r.cbExecution.WithLabelValues(name).Observe(d.Seconds())
}

// ObserveDecisionCtx records a rate-limit decision latency for algorithm and,
// when the Recorder was built WithExemplars and ctx carries a sampled OTel span,
// attaches a trace_id exemplar linking the histogram sample to the trace.
//
// It is a drop-in replacement for ObserveDecision on code paths that hold a
// context. When no span is present (or exemplars are disabled) it is exactly
// equivalent to ObserveDecision and performs no extra allocation.
func (r *Recorder) ObserveDecisionCtx(ctx context.Context, algorithm string, d time.Duration) {
	r.observe(ctx, r.rlDecision.WithLabelValues(algorithm), d)
}

// ObserveCBExecutionCtx records a circuit-breaker protected-call latency for
// name and, when the Recorder was built WithExemplars and ctx carries a sampled
// OTel span, attaches a trace_id exemplar. See ObserveDecisionCtx.
func (r *Recorder) ObserveCBExecutionCtx(ctx context.Context, name string, d time.Duration) {
	r.observe(ctx, r.cbExecution.WithLabelValues(name), d)
}

// observe records d.Seconds() on obs, attaching a trace_id exemplar when
// exemplars are enabled and ctx carries a sampled span. The exemplar path is
// entirely skipped (zero cost) otherwise.
func (r *Recorder) observe(ctx context.Context, obs prometheus.Observer, d time.Duration) {
	if r.exemplars {
		if sc := trace.SpanContextFromContext(ctx); sc.IsSampled() {
			if eo, ok := obs.(prometheus.ExemplarObserver); ok {
				eo.ObserveWithExemplar(d.Seconds(), prometheus.Labels{
					"trace_id": sc.TraceID().String(),
					"span_id":  sc.SpanID().String(),
				})
				return
			}
		}
	}
	obs.Observe(d.Seconds())
}

// IncCBTransition increments the state-transition counter for name from→to.
func (r *Recorder) IncCBTransition(name, from, to string) {
	r.cbTransitions.WithLabelValues(name, from, to).Inc()
}

// SetBulkheadInflight sets the in-flight gauge for a bulkhead.
func (r *Recorder) SetBulkheadInflight(name string, n int) {
	r.bhInflight.WithLabelValues(name).Set(float64(n))
}

// IncBulkheadRejected increments the rejected counter for a bulkhead.
func (r *Recorder) IncBulkheadRejected(name string) {
	r.bhRejected.WithLabelValues(name).Inc()
}
