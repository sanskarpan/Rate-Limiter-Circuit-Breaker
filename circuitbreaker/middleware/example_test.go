package middleware_test

import (
	"net/http"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
	cbmw "github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker/middleware"
)

// Example wraps an http.Handler so failing responses trip a circuit breaker and
// subsequent requests are rejected fast while the downstream recovers.
func Example() {
	cb := circuitbreaker.New(circuitbreaker.Config{
		Name:             "downstream",
		FailureThreshold: 5,
		OpenTimeout:      10 * time.Second,
	})

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// protected can be mounted on any net/http mux.
	protected := cbmw.CircuitBreaker(cb)(handler)
	_ = protected
	// Output:
}
