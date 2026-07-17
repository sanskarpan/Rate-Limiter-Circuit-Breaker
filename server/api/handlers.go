package api

import (
	"log/slog"
	"sync/atomic"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/server/simulation"
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
		simSem:    make(chan struct{}, maxConcurrentSimulations),
	}
}
