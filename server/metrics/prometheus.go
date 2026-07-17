// Package metrics provides Prometheus instrumentation for the resilience demo server.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds all Prometheus metrics for the server.
type Metrics struct {
	RateLimitTotal    *prometheus.CounterVec
	RateLimitAllowed  *prometheus.CounterVec
	RateLimitDenied   *prometheus.CounterVec
	CBExecuteTotal    *prometheus.CounterVec
	CBStateChanges    *prometheus.CounterVec
	HTTPRequestsTotal *prometheus.CounterVec
	HTTPDurationSecs  *prometheus.HistogramVec
}

// New creates and registers all Prometheus metrics.
func New(reg prometheus.Registerer) *Metrics {
	factory := promauto.With(reg)

	return &Metrics{
		RateLimitTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "resilience_ratelimit_requests_total",
			Help: "Total number of rate limit check requests.",
		}, []string{"algorithm"}),

		RateLimitAllowed: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "resilience_ratelimit_allowed_total",
			Help: "Total number of allowed rate limit requests.",
		}, []string{"algorithm"}),

		RateLimitDenied: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "resilience_ratelimit_denied_total",
			Help: "Total number of denied rate limit requests.",
		}, []string{"algorithm"}),

		CBExecuteTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "resilience_cb_executions_total",
			Help: "Total number of circuit breaker execute calls.",
		}, []string{"name", "result"}),

		CBStateChanges: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "resilience_cb_state_changes_total",
			Help: "Total number of circuit breaker state transitions.",
		}, []string{"name", "from", "to"}),

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
