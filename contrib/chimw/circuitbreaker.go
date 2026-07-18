package chimw

import (
	"net/http"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
	cbmw "github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker/middleware"
)

// CircuitBreaker returns a chi-compatible middleware (func(http.Handler) http.Handler)
// that wraps each request with the given circuit breaker. Because chi routers accept
// any stdlib func(http.Handler) http.Handler, this is a thin re-export of the core
// circuitbreaker/middleware so callers get a single, discoverable import path
// alongside the other framework adapters (ginmw, echomw, fibermw, connectmw).
//
// Behaviour (inherited from the core middleware):
//   - When the circuit is open, requests short-circuit with HTTP 503 and a JSON body
//     without invoking the downstream handler.
//   - Otherwise the handler runs; HTTP 5xx responses are recorded as failures and
//     2xx/3xx/4xx as successes.
//
// Mount it with chi's r.Use:
//
//	r := chi.NewRouter()
//	r.Use(chimw.CircuitBreaker(cb))
func CircuitBreaker(cb *circuitbreaker.CircuitBreaker) func(http.Handler) http.Handler {
	return cbmw.CircuitBreaker(cb)
}
