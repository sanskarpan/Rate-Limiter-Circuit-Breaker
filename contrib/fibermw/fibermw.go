// Package fibermw provides a fiber v2 rate-limiting middleware for the
// github.com/sanskarpan/Rate-Limiter-Circuit-Breaker rate limiter.
//
// fiber is built on fasthttp rather than net/http, so key extraction reads from
// *fiber.Ctx. The adapter still reuses the core ratelimit.Limiter and mirrors the
// standard rate-limit response headers set by ratelimit/middleware, honouring the
// WithCost weighting semantics.
package fibermw

import (
	"fmt"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
)

// KeyFunc extracts a rate-limit key from a fiber request context.
type KeyFunc func(c *fiber.Ctx) string

// CostFunc computes the token cost (weight) of a request. Values below 1 are
// clamped to 1, so a request always consumes at least one token.
type CostFunc func(c *fiber.Ctx) int

// KeyByIP extracts the client IP address.
// Order: X-Forwarded-For -> X-Real-IP -> c.IP(), matching the core middleware.
func KeyByIP() KeyFunc {
	return func(c *fiber.Ctx) string {
		if xff := c.Get("X-Forwarded-For"); xff != "" {
			return strings.TrimSpace(strings.Split(xff, ",")[0])
		}
		if xri := c.Get("X-Real-IP"); xri != "" {
			return xri
		}
		return c.IP()
	}
}

// KeyByHeader extracts the rate-limit key from a request header.
func KeyByHeader(name string) KeyFunc {
	return func(c *fiber.Ctx) string { return c.Get(name) }
}

// KeyByParam extracts the rate-limit key from a URL query parameter.
func KeyByParam(name string) KeyFunc {
	return func(c *fiber.Ctx) string { return c.Query(name) }
}

type options struct {
	keyFunc      KeyFunc
	costFunc     CostFunc
	skipFunc     func(c *fiber.Ctx) bool
	onLimited    func(c *fiber.Ctx, result ratelimit.Result) error
	errorHandler func(c *fiber.Ctx, err error) error
}

// Option configures the rate limit middleware.
type Option func(*options)

// WithKeyFunc sets a custom key extraction function (default: KeyByIP).
func WithKeyFunc(fn KeyFunc) Option { return func(o *options) { o.keyFunc = fn } }

// WithCost sets a function computing the token cost of each request (default: 1).
func WithCost(fn CostFunc) Option { return func(o *options) { o.costFunc = fn } }

// WithSkipFunc sets a predicate that, when true, skips rate limiting.
func WithSkipFunc(fn func(c *fiber.Ctx) bool) Option {
	return func(o *options) { o.skipFunc = fn }
}

// WithOnLimited sets a custom handler invoked when a request is rate limited.
// The default responds with HTTP 429 and a JSON body.
func WithOnLimited(fn func(c *fiber.Ctx, result ratelimit.Result) error) Option {
	return func(o *options) { o.onLimited = fn }
}

// WithErrorHandler sets a custom handler for internal errors.
func WithErrorHandler(fn func(c *fiber.Ctx, err error) error) Option {
	return func(o *options) { o.errorHandler = fn }
}

func defaultOnLimited(c *fiber.Ctx, result ratelimit.Result) error {
	body := fiber.Map{"error": "rate_limit_exceeded", "limit": result.Limit}
	if result.RetryAfter > 0 {
		body["retry_after"] = result.RetryAfter.Seconds()
	}
	return c.Status(fiber.StatusTooManyRequests).JSON(body)
}

// RateLimit returns a fiber.Handler that rate limits requests using the given
// limiter. The default key function is KeyByIP and the default cost is 1. On
// deny it sets the standard rate-limit headers and responds with HTTP 429.
func RateLimit(limiter ratelimit.Limiter, opts ...Option) fiber.Handler {
	o := &options{keyFunc: KeyByIP(), onLimited: defaultOnLimited}
	for _, opt := range opts {
		opt(o)
	}
	return func(c *fiber.Ctx) error {
		if o.skipFunc != nil && o.skipFunc(c) {
			return c.Next()
		}

		key := o.keyFunc(c)

		cost := 1
		if o.costFunc != nil {
			if v := o.costFunc(c); v > 1 {
				cost = v
			}
		}

		var result ratelimit.Result
		if cost == 1 {
			result = limiter.Allow(c.UserContext(), key)
		} else {
			result = limiter.AllowN(c.UserContext(), key, cost)
		}

		c.Set("X-RateLimit-Limit", fmt.Sprintf("%d", result.Limit))
		c.Set("X-RateLimit-Remaining", fmt.Sprintf("%d", result.Remaining))
		c.Set("X-RateLimit-Cost", fmt.Sprintf("%d", cost))
		if result.ResetAfter > 0 {
			c.Set("X-RateLimit-Reset", fmt.Sprintf("%d", time.Now().Add(result.ResetAfter).Unix()))
		}

		if !result.Allowed {
			if result.RetryAfter > 0 {
				c.Set("Retry-After", fmt.Sprintf("%.0f", result.RetryAfter.Seconds()))
			}
			return o.onLimited(c, result)
		}

		return c.Next()
	}
}
