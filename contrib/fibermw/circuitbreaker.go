package fibermw

import (
	"context"
	"errors"
	"fmt"

	"github.com/gofiber/fiber/v2"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
)

// cbOptions configures the circuit-breaker middleware.
type cbOptions struct {
	skipFunc func(c *fiber.Ctx) bool
	onOpen   func(c *fiber.Ctx) error
}

// CBOption configures the circuit breaker middleware.
type CBOption func(*cbOptions)

// WithCBSkipFunc sets a predicate that, when true, bypasses the circuit breaker.
func WithCBSkipFunc(fn func(c *fiber.Ctx) bool) CBOption {
	return func(o *cbOptions) { o.skipFunc = fn }
}

// WithCBOnOpen sets a custom handler invoked when the circuit is open and the
// request is short-circuited. The default responds with HTTP 503 and a JSON body.
func WithCBOnOpen(fn func(c *fiber.Ctx) error) CBOption {
	return func(o *cbOptions) { o.onOpen = fn }
}

func defaultCBOnOpen(c *fiber.Ctx) error {
	return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
		"error":  "service_unavailable",
		"reason": "circuit_open",
	})
}

// CircuitBreaker returns a fiber.Handler that wraps each request with the given
// circuit breaker.
//
// Behaviour:
//   - When the circuit is open, the request is short-circuited with HTTP 503 (or the
//     WithCBOnOpen handler) without invoking the downstream handler.
//   - Otherwise the handler runs; a returned error OR an HTTP 5xx response status is
//     recorded as a failure and everything else as a success. A handler error is
//     propagated unchanged so fiber's error handling still applies.
//
// Mount it with fiber's Use:
//
//	app.Use(fibermw.CircuitBreaker(cb))
func CircuitBreaker(cb *circuitbreaker.CircuitBreaker, opts ...CBOption) fiber.Handler {
	o := &cbOptions{onOpen: defaultCBOnOpen}
	for _, opt := range opts {
		opt(o)
	}
	return func(c *fiber.Ctx) error {
		if o.skipFunc != nil && o.skipFunc(c) {
			return c.Next()
		}

		var handlerErr error
		err := cb.Execute(c.UserContext(), func(ctx context.Context) error {
			c.SetUserContext(ctx)
			handlerErr = c.Next()
			if handlerErr != nil {
				return handlerErr
			}
			if c.Response().StatusCode() >= 500 {
				return fmt.Errorf("upstream error: status %d", c.Response().StatusCode())
			}
			return nil
		})
		if err != nil {
			if errors.Is(err, circuitbreaker.ErrCircuitOpen) ||
				errors.Is(err, circuitbreaker.ErrTooManyRequests) {
				return o.onOpen(c)
			}
			// The handler's own error tripped the breaker; propagate it so
			// fiber's configured ErrorHandler renders the response.
			return handlerErr
		}
		return nil
	}
}
