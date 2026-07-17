// Package middleware provides HTTP middleware for circuit breaking.
package middleware

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/sanskarpan/resilience/circuitbreaker"
)

// responseRecorder wraps an http.ResponseWriter and captures the status code
// while passing all writes through to the underlying writer immediately.
// This allows the circuit breaker to inspect the status code after the handler runs.
type responseRecorder struct {
	http.ResponseWriter
	statusCode int
	written    bool
}

func newResponseRecorder(w http.ResponseWriter) *responseRecorder {
	return &responseRecorder{ResponseWriter: w}
}

// WriteHeader captures the status code and forwards it to the underlying writer.
func (rr *responseRecorder) WriteHeader(code int) {
	rr.statusCode = code
	rr.written = true
	rr.ResponseWriter.WriteHeader(code)
}

// Write passes bytes through and records that a response has started.
// If WriteHeader has not been called yet, it implicitly records 200.
func (rr *responseRecorder) Write(b []byte) (int, error) {
	if !rr.written {
		rr.statusCode = http.StatusOK
		rr.written = true
	}
	return rr.ResponseWriter.Write(b)
}

// CircuitBreaker returns an HTTP middleware that wraps each request with the
// given circuit breaker.
//
// Behaviour:
//   - If the circuit is open, the middleware responds with 503 Service Unavailable
//     and a JSON body without calling the downstream handler.
//   - If the circuit is closed or half-open, the downstream handler is called.
//     HTTP 5xx responses are counted as failures; 2xx/3xx/4xx are counted as successes.
//   - Response bytes are always written through to the client in real time; there is
//     no buffering. When a 5xx is detected, the response has already been sent, so the
//     circuit records the failure but does NOT send an additional error body.
func CircuitBreaker(cb *circuitbreaker.CircuitBreaker) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			err := cb.Execute(r.Context(), func(ctx context.Context) error {
				rr := newResponseRecorder(w)
				next.ServeHTTP(rr, r.WithContext(ctx))
				if rr.statusCode >= 500 {
					return fmt.Errorf("upstream error: status %d", rr.statusCode)
				}
				return nil
			})
			if err != nil {
				// Only write a response body if the circuit was open (fn never ran),
				// i.e. the ResponseWriter has NOT been touched yet.
				// When fn did run (circuit closed/half-open), the downstream handler
				// already wrote to w via the recorder, so we must not write again.
				if errors.Is(err, circuitbreaker.ErrCircuitOpen) || errors.Is(err, circuitbreaker.ErrTooManyRequests) {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusServiceUnavailable)
					_ = json.NewEncoder(w).Encode(map[string]string{
						"error":  "service_unavailable",
						"reason": "circuit_open",
					})
				}
				// For 5xx pass-through: the handler already wrote the response.
				// We intentionally do not write again.
			}
		})
	}
}
