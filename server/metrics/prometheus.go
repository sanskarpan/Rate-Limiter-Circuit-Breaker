// Package metrics provides Prometheus instrumentation for the resilience demo
// server's HTTP layer.
//
// The rate-limiter, circuit-breaker and bulkhead series are NO LONGER declared
// here. They are emitted by the library core via the metric.Recorder interface
// and its Prometheus adapter (metric/prometheus), which the demo server wires
// over the default registry in main.go. This package now owns only the HTTP
// server middleware metrics, avoiding a duplicate-registration conflict on the
// shared registry (the adapter and this package would otherwise both try to
// register resilience_ratelimit_requests_total).
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds the HTTP-layer Prometheus metrics for the server.
type Metrics struct {
	HTTPRequestsTotal *prometheus.CounterVec
	HTTPDurationSecs  *prometheus.HistogramVec
}

// New creates and registers the HTTP server metrics.
func New(reg prometheus.Registerer) *Metrics {
	factory := promauto.With(reg)

	return &Metrics{
		HTTPRequestsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "resilience_http_requests_total",
			Help: "Total number of HTTP requests.",
		}, []string{"method", "path", "status"}),

		HTTPDurationSecs: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "resilience_http_request_duration_seconds",
			Help:    "HTTP request latency in seconds.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "path"}),
	}
}

// NewDefault creates metrics using the default Prometheus registry.
func NewDefault() *Metrics {
	return New(prometheus.DefaultRegisterer)
}
