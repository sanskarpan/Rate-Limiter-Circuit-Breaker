package echomw

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
)

// cbOptions configures the circuit-breaker middleware.
type cbOptions struct {
	skipFunc func(c echo.Context) bool
	onOpen   func(c echo.Context) error
}

// CBOption configures the circuit breaker middleware.
type CBOption func(*cbOptions)

// WithCBSkipFunc sets a predicate that, when true, bypasses the circuit breaker.
func WithCBSkipFunc(fn func(c echo.Context) bool) CBOption {
	return func(o *cbOptions) { o.skipFunc = fn }
}

// WithCBOnOpen sets a custom handler invoked when the circuit is open and the
// request is short-circuited. The default responds with HTTP 503 and a JSON body.
func WithCBOnOpen(fn func(c echo.Context) error) CBOption {
	return func(o *cbOptions) { o.onOpen = fn }
}

func defaultCBOnOpen(c echo.Context) error {
	return c.JSON(http.StatusServiceUnavailable, map[string]string{
		"error":  "service_unavailable",
		"reason": "circuit_open",
	})
}

// CircuitBreaker returns an echo.MiddlewareFunc that wraps each request with the
// given circuit breaker.
//
// Behaviour:
//   - When the circuit is open, the request is short-circuited with HTTP 503 (or the
//     WithCBOnOpen handler) without invoking the downstream handler.
//   - Otherwise the handler runs; a returned error OR an HTTP 5xx response status is
//     recorded as a failure and everything else as a success. A handler error is
//     propagated unchanged so echo's error handling still applies.
//
// Mount it with echo's Use:
//
//	e.Use(echomw.CircuitBreaker(cb))
func CircuitBreaker(cb *circuitbreaker.CircuitBreaker, opts ...CBOption) echo.MiddlewareFunc {
	o := &cbOptions{onOpen: defaultCBOnOpen}
	for _, opt := range opts {
		opt(o)
	}
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			if o.skipFunc != nil && o.skipFunc(c) {
				return next(c)
			}

			var handlerErr error
			err := cb.Execute(c.Request().Context(), func(ctx context.Context) error {
				c.SetRequest(c.Request().WithContext(ctx))
				handlerErr = next(c)
				if handlerErr != nil {
					return handlerErr
				}
				if c.Response().Status >= 500 {
					return fmt.Errorf("upstream error: status %d", c.Response().Status)
				}
				return nil
			})
			if err != nil {
				if errors.Is(err, circuitbreaker.ErrCircuitOpen) ||
					errors.Is(err, circuitbreaker.ErrTooManyRequests) {
					return o.onOpen(c)
				}
				// The handler's own error tripped the breaker; propagate it so
				// echo's configured HTTPErrorHandler renders the response.
				return handlerErr
			}
			return nil
		}
	}
}
