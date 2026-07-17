# Observability: Prometheus metrics + OpenTelemetry tracing

The core library is observability-agnostic: every limiter and circuit breaker
talks to a `metric.Recorder` interface, which defaults to a no-op so the hot
path stays allocation-free. Wire a real recorder to export metrics, and add the
OTel HTTP middleware for distributed tracing.

## Prometheus metrics

The `metric/prometheus` adapter implements `metric.Recorder` on top of the
Prometheus client. Construct it with a `prometheus.Registerer` and pass it to
each component via its `WithRecorder` option (limiters) or `Config.WithRecorder`
(circuit breaker).

```go
package main

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
	metricprom "github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/metric/prometheus"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
)

func main() {
	// One recorder registered on the default registry.
	rec := metricprom.New(prometheus.DefaultRegisterer)

	// Wire it into a rate limiter…
	lim := tokenbucket.New(100, 20, tokenbucket.WithRecorder(rec))
	defer lim.Close()

	// …and a circuit breaker.
	cb := circuitbreaker.New(circuitbreaker.Config{
		Name:             "downstream",
		FailureThreshold: 5,
		OpenTimeout:      30 * time.Second,
	}.WithRecorder(rec))
	_ = cb

	// Expose the metrics for scraping.
	http.Handle("/metrics", promhttp.Handler())
	_ = http.ListenAndServe(":8080", nil)
}
```

The adapter emits bounded-cardinality series (labels are drawn from a small
fixed set — never per-request keys), matching the shipped Grafana dashboard:

| Series | Type | Labels |
| --- | --- | --- |
| `resilience_ratelimit_requests_total` | counter | `algorithm`, `result` |
| `resilience_ratelimit_decision_duration_seconds` | histogram | `algorithm` |
| `resilience_circuitbreaker_state` | gauge (0=closed, 1=half-open, 2=open) | `name` |
| `resilience_circuitbreaker_requests_total` | counter | `name`, `result` |
| `resilience_circuitbreaker_state_transitions_total` | counter | `name`, `from`, `to` |
| `resilience_circuitbreaker_execution_duration_seconds` | histogram | `name` |
| `resilience_bulkhead_inflight` | gauge | `name` |

> **Composite limiters:** pass the recorder to the composite with
> `composite.New(...).WithRecorder(rec)` to record the composite's own
> allow/deny decision under `composite_and` / `composite_or`. Underlying
> limiters keep their own (default no-op) recorders, so wiring is opt-in per
> layer.

## OpenTelemetry tracing

Set up a tracer provider once at startup with `observability/otel`. `New`
installs the provider globally (`otel.SetTracerProvider`) and a W3C
TraceContext + Baggage propagator, so context flows across service boundaries.

```go
import (
	"context"

	otelprov "github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/observability/otel"
)

func setupTracing(ctx context.Context) (*otelprov.Provider, error) {
	return otelprov.New(ctx, otelprov.Config{
		ServiceName:    "api",
		ServiceVersion: "1.4.0",
		Exporter:       otelprov.ExporterOTLP, // "otlp", "stdout", or "none"
		Endpoint:       "localhost:4318",      // OTLP-HTTP collector
		SampleRatio:    0.1,                   // sample 10% of root traces
	})
}
```

Remember to flush on shutdown:

```go
prov, err := setupTracing(ctx)
if err != nil { /* handle */ }
defer prov.Shutdown(context.Background())
```

Then wrap your HTTP handlers with the tracing middleware from
`observability/otelhttp`. It extracts the incoming trace context, starts a
server span per request with method/route/status attributes, and re-injects the
active span so downstream calls propagate it:

```go
import otelhttp "github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/observability/otelhttp"

var handler http.Handler = mux
handler = otelhttp.Middleware("api")(handler) // tracer name
_ = http.ListenAndServe(":8080", handler)
```

Stack it with a rate-limit middleware — tracing outermost so limited requests
still produce a span:

```go
handler = chimw.RateLimit(lim)(handler)
handler = otelhttp.Middleware("api")(handler)
```

## See also

- [Protect a flaky downstream](flaky-downstream-cb-retry-hedge.md)
- [Adaptive concurrency + load shedding](adaptive-concurrency-loadshed.md)
- Runnable example: `examples/http-server/main.go`
</content>
