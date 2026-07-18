package api

import (
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/observability/otelhttp"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/server/metrics"
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
	handler, _ := NewRouterWithHub(limiters, cbs, registry, logger, ready, corsOrigins, apiKey)
	return handler
}

// NewRouterWithHubAndProtection is like NewRouterWithHub but takes an explicit
// SelfProtectConfig (ENHANCEMENTS §7.4) governing the demo server's own DoS
// self-protection: the configurable body-size cap, the dogfooded per-IP rate
// limiter and the global control-plane concurrency guard. It also returns a
// closer that releases the self-protection limiter's resources on shutdown.
//
// NewRouterWithHub delegates here with DefaultSelfProtectConfig, so existing
// callers keep working unchanged (backward-compatible).
func NewRouterWithHubAndProtection(
	limiters map[string]ratelimit.Limiter,
	cbs map[string]*circuitbreaker.CircuitBreaker,
	registry *circuitbreaker.Registry,
	logger *slog.Logger,
	ready *atomic.Bool,
	corsOrigins []string,
	apiKey string,
	sp SelfProtectConfig,
) (http.Handler, *Hub, func()) {
	return newRouter(limiters, cbs, registry, logger, ready, corsOrigins, apiKey, sp)
}

// NewRouterWithHub is like NewRouter but also returns the *Hub so the caller can
// Stop() it on shutdown to drain WebSocket goroutines (H-17/F-2). The demo
// server (main) uses this; tests that don't need shutdown use NewRouter.
func NewRouterWithHub(
	limiters map[string]ratelimit.Limiter,
	cbs map[string]*circuitbreaker.CircuitBreaker,
	registry *circuitbreaker.Registry,
	logger *slog.Logger,
	ready *atomic.Bool,
	corsOrigins []string,
	apiKey string,
) (http.Handler, *Hub) {
	// Delegate with the built-in self-protection defaults. The closer is dropped
	// here for signature compatibility; the limiter is a lightweight in-memory
	// token bucket whose only resource is a background goroutine that exits when
	// the process ends, so this is safe for the demo. Callers needing explicit
	// shutdown (main) use NewRouterWithHubAndProtection.
	handler, hub, _ := newRouter(limiters, cbs, registry, logger, ready, corsOrigins, apiKey,
		DefaultSelfProtectConfig())
	return handler, hub
}

// newRouter is the shared implementation behind the exported constructors.
func newRouter(
	limiters map[string]ratelimit.Limiter,
	cbs map[string]*circuitbreaker.CircuitBreaker,
	registry *circuitbreaker.Registry,
	logger *slog.Logger,
	ready *atomic.Bool,
	corsOrigins []string,
	apiKey string,
	spCfg SelfProtectConfig,
) (http.Handler, *Hub, func()) {
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
	// Self-protection (§7.4): dogfooded per-IP rate limiter + global concurrency
	// guard applied ONLY to the mutating control plane so read-only/health
	// traffic is never throttled by the server's self-protection.
	sp, spClose := newSelfProtector(spCfg)

	// protect wraps a mutating HandlerFunc with the self-protection middleware
	// and then the API-key check (auth outermost so unauthenticated floods are
	// still shed, but auth failures don't consume the per-IP budget first —
	// self-protection runs after auth passes).
	protect := func(fn http.HandlerFunc) http.Handler {
		return auth(sp.Middleware(fn))
	}

	mux := http.NewServeMux()

	// Health (read-only, always open)
	mux.HandleFunc("GET /health/live", health.Live)
	mux.HandleFunc("GET /health/ready", health.Ready)

	// Rate limiter endpoints. /allow is an unauthenticated mutating POST and the
	// primary flood surface, so it gets self-protection (per-IP limit + inflight
	// guard) without the API-key check.
	mux.Handle("POST /api/v1/limiters/{algorithm}/allow", sp.Middleware(http.HandlerFunc(h.HandleAllow)))
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
	handler = LimitRequestBodyN(spCfg.effectiveMaxRequestBytes())(handler)
	handler = SecurityHeaders(handler)
	handler = Recovery(logger)(handler)
	handler = otelhttp.Middleware("resilience-server")(handler)

	return handler, hub, spClose
}
