package api

import (
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sanskarpan/resilience/circuitbreaker"
	"github.com/sanskarpan/resilience/ratelimit"
	"github.com/sanskarpan/resilience/server/metrics"
)

// serverMetrics is registered once on the default Prometheus registry. Using a
// sync.Once avoids duplicate-registration panics when NewRouter is called more
// than once in the same process (e.g. across tests).
var (
	serverMetrics     *metrics.Metrics
	serverMetricsOnce sync.Once
)

func defaultMetrics() *metrics.Metrics {
	serverMetricsOnce.Do(func() {
		serverMetrics = metrics.NewDefault()
	})
	return serverMetrics
}

// Router holds the HTTP mux and all handlers.
type Router struct {
	mux      *http.ServeMux
	handlers *Handlers
	hub      *Hub
	logger   *slog.Logger
}

// NewRouter constructs the router, registers all routes and wraps middleware.
func NewRouter(
	limiters map[string]ratelimit.Limiter,
	cbs map[string]*circuitbreaker.CircuitBreaker,
	registry *circuitbreaker.Registry,
	logger *slog.Logger,
	ready *atomic.Bool,
	corsOrigins []string,
	apiKey string,
) http.Handler {
	hub := newHub(logger)
	go hub.Run()

	h := NewHandlers(limiters, cbs, registry, hub, logger, ready)
	health := newHealthHandler(ready)
	// Known topic keys for WS path-param validation (L-12).
	knownLimiters := make(map[string]bool, len(limiters))
	for name := range limiters {
		knownLimiters[name] = true
	}
	ws := newWSHandler(hub, logger, corsOrigins, knownLimiters, registry)

	// auth gates the mutating/control plane. When no key is configured it is a
	// no-op (demo mode); NewRouter logs a warning in that case.
	auth := APIKeyAuth(apiKey)
	if apiKey == "" {
		logger.Warn("API_KEY not set: control plane is UNAUTHENTICATED — " +
			"mutating routes (/execute, /force-*, /configure, /simulate) and /metrics are open to anyone. " +
			"Set API_KEY to require authentication.")
	}
	// protect wraps a HandlerFunc with the API-key check.
	protect := func(fn http.HandlerFunc) http.Handler { return auth(fn) }

	mux := http.NewServeMux()

	// Health (read-only, always open)
	mux.HandleFunc("GET /health/live", health.Live)
	mux.HandleFunc("GET /health/ready", health.Ready)

	// Rate limiter endpoints
	mux.HandleFunc("POST /api/v1/limiters/{algorithm}/allow", h.HandleAllow)
	mux.Handle("POST /api/v1/limiters/{algorithm}/configure", protect(h.HandleConfigure))
	mux.HandleFunc("GET /api/v1/limiters/{algorithm}/state", h.HandleState)

	// Circuit breaker endpoints — specific routes before wildcard
	mux.HandleFunc("GET /api/v1/cb/all", h.HandleCBAll)
	mux.Handle("POST /api/v1/cb/{name}/execute", protect(h.HandleCBExecute))
	mux.Handle("POST /api/v1/cb/{name}/force-open", protect(h.HandleCBForceOpen))
	mux.Handle("POST /api/v1/cb/{name}/force-close", protect(h.HandleCBForceClose))
	mux.Handle("POST /api/v1/cb/{name}/force-half-open", protect(h.HandleCBForceHalfOpen))
	mux.HandleFunc("GET /api/v1/cb/{name}/snapshot", h.HandleCBSnapshot)

	// Simulation (mutating / resource-intensive — protected)
	mux.Handle("POST /api/v1/simulate", protect(h.HandleSimulate))

	// WebSocket
	mux.HandleFunc("GET /ws/v1/limiters/{algorithm}", ws.HandleLimiter)
	mux.HandleFunc("GET /ws/v1/cb/{name}", ws.HandleCB)
	mux.HandleFunc("GET /ws/v1/events", ws.HandleEvents)

	// Prometheus metrics — gated behind the same API-key check (C-2).
	mux.Handle("GET /metrics", auth(promhttp.Handler()))

	// Apply middleware chain (outermost first):
	// Recovery → SecurityHeaders → LimitRequestBody → RequestID → Metrics → Logger → CORS
	var handler http.Handler = mux
	handler = CORS(corsOrigins)(handler)
	handler = Logger(logger)(handler)
	handler = Metrics(defaultMetrics())(handler)
	handler = RequestID(handler)
	handler = LimitRequestBody(handler)
	handler = SecurityHeaders(handler)
	handler = Recovery(logger)(handler)

	return handler
}
