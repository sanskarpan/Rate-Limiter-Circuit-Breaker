package api

import (
	"log/slog"
	"sync/atomic"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/bulkhead"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
	metricprom "github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/metric/prometheus"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/server/simulation"

	"github.com/prometheus/client_golang/prometheus"
)

// maxConcurrentSimulations bounds the number of in-flight /simulate calls
// server-wide. Each simulation spawns up to Concurrency worker goroutines, so an
// unbounded number of concurrent simulations enables goroutine-amplification DoS
// (M-19). Requests beyond this cap are rejected with 429.
const maxConcurrentSimulations = 8

// Handlers holds all application-level dependencies used by HTTP handlers.
type Handlers struct {
	limiters  map[string]ratelimit.Limiter
	cbs       map[string]*circuitbreaker.CircuitBreaker
	registry  *circuitbreaker.Registry
	hub       *Hub
	logger    *slog.Logger
	ready     *atomic.Bool
	simEngine *simulation.Engine
	// execBulkhead bounds concurrent CB executions so the demo exercises (and the
	// Grafana dashboard populates) the bulkhead saturation metrics.
	execBulkhead *bulkhead.Bulkhead
	// simSem is a counting semaphore bounding concurrent simulations (M-19).
	simSem chan struct{}
}

// NewHandlers creates a new Handlers with the provided dependencies.
func NewHandlers(
	limiters map[string]ratelimit.Limiter,
	cbs map[string]*circuitbreaker.CircuitBreaker,
	registry *circuitbreaker.Registry,
	hub *Hub,
	logger *slog.Logger,
	ready *atomic.Bool,
) *Handlers {
	return &Handlers{
		limiters:  limiters,
		cbs:       cbs,
		registry:  registry,
		hub:       hub,
		logger:    logger,
		ready:     ready,
		simEngine: simulation.New(logger),
		execBulkhead: bulkhead.New(64, 0,
			bulkhead.WithName("cb-execute"),
			bulkhead.WithRecorder(metricprom.New(prometheus.DefaultRegisterer))),
		simSem: make(chan struct{}, maxConcurrentSimulations),
	}
}
