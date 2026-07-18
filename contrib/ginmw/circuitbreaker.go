package ginmw

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
)

// cbOptions configures the circuit-breaker middleware.
type cbOptions struct {
	skipFunc func(c *gin.Context) bool
	onOpen   func(c *gin.Context)
}

// CBOption configures the circuit breaker middleware.
type CBOption func(*cbOptions)

// WithCBSkipFunc sets a predicate that, when true, bypasses the circuit breaker.
func WithCBSkipFunc(fn func(c *gin.Context) bool) CBOption {
	return func(o *cbOptions) { o.skipFunc = fn }
}

// WithCBOnOpen sets a custom handler invoked when the circuit is open and the
// request is short-circuited. The default aborts with HTTP 503 and a JSON body.
func WithCBOnOpen(fn func(c *gin.Context)) CBOption {
	return func(o *cbOptions) { o.onOpen = fn }
}

func defaultCBOnOpen(c *gin.Context) {
	c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{
		"error":  "service_unavailable",
		"reason": "circuit_open",
	})
}

// CircuitBreaker returns a gin.HandlerFunc that wraps each request with the given
// circuit breaker.
//
// Behaviour:
//   - When the circuit is open, the request is short-circuited with HTTP 503 (or the
//     WithCBOnOpen handler) and the downstream handler chain is not invoked.
//   - Otherwise the handler runs; an HTTP 5xx response is recorded as a failure and
//     2xx/3xx/4xx as a success, mirroring the core net/http circuit-breaker middleware.
//
// Mount it per-route or globally with gin's Use:
//
//	r.Use(ginmw.CircuitBreaker(cb))
func CircuitBreaker(cb *circuitbreaker.CircuitBreaker, opts ...CBOption) gin.HandlerFunc {
	o := &cbOptions{onOpen: defaultCBOnOpen}
	for _, opt := range opts {
		opt(o)
	}
	return func(c *gin.Context) {
		if o.skipFunc != nil && o.skipFunc(c) {
			c.Next()
			return
		}

		err := cb.Execute(c.Request.Context(), func(ctx context.Context) error {
			c.Request = c.Request.WithContext(ctx)
			c.Next()
			if c.Writer.Status() >= 500 {
				return fmt.Errorf("upstream error: status %d", c.Writer.Status())
			}
			return nil
		})
		// Only synthesize a response when the breaker itself rejected the call
		// (fn never ran). For a 5xx pass-through the handler already wrote the
		// response, so we must not write again.
		if err != nil && (errors.Is(err, circuitbreaker.ErrCircuitOpen) ||
			errors.Is(err, circuitbreaker.ErrTooManyRequests)) {
			o.onOpen(c)
		}
	}
}
