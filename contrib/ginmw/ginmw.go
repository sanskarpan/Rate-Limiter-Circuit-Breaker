// Package ginmw provides a gin rate-limiting middleware for the
// github.com/sanskarpan/Rate-Limiter-Circuit-Breaker rate limiter.
//
// It reuses the core ratelimit.Limiter, mirrors the standard rate-limit response
// headers set by ratelimit/middleware (X-RateLimit-Limit / -Remaining / -Reset /
// -Cost and Retry-After), and honours the WithCost weighting semantics.
package ginmw

import (
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
)

// KeyFunc extracts a rate-limit key from a gin request context.
type KeyFunc func(c *gin.Context) string

// CostFunc computes the token cost (weight) of a request. Values below 1 are
// clamped to 1, so a request always consumes at least one token.
type CostFunc func(c *gin.Context) int

// KeyByIP extracts the client IP address.
// Order: X-Forwarded-For -> X-Real-IP -> RemoteAddr, matching the core middleware.
func KeyByIP() KeyFunc {
	return func(c *gin.Context) string {
		return clientIP(c.Request)
	}
}

// KeyByHeader extracts the rate-limit key from a request header.
func KeyByHeader(name string) KeyFunc {
	return func(c *gin.Context) string { return c.GetHeader(name) }
}

// KeyByParam extracts the rate-limit key from a URL query parameter.
func KeyByParam(name string) KeyFunc {
	return func(c *gin.Context) string { return c.Query(name) }
}

type options struct {
	keyFunc      KeyFunc
	costFunc     CostFunc
	skipFunc     func(c *gin.Context) bool
	onLimited    func(c *gin.Context, result ratelimit.Result)
	errorHandler func(c *gin.Context, err error)
}

// Option configures the rate limit middleware.
type Option func(*options)

// WithKeyFunc sets a custom key extraction function (default: KeyByIP).
func WithKeyFunc(fn KeyFunc) Option { return func(o *options) { o.keyFunc = fn } }

// WithCost sets a function computing the token cost of each request (default: 1).
func WithCost(fn CostFunc) Option { return func(o *options) { o.costFunc = fn } }

// WithSkipFunc sets a predicate that, when true, skips rate limiting.
func WithSkipFunc(fn func(c *gin.Context) bool) Option {
	return func(o *options) { o.skipFunc = fn }
}

// WithOnLimited sets a custom handler invoked when a request is rate limited.
// The default aborts with HTTP 429 and a JSON body.
func WithOnLimited(fn func(c *gin.Context, result ratelimit.Result)) Option {
	return func(o *options) { o.onLimited = fn }
}

// WithErrorHandler sets a custom handler for internal errors.
func WithErrorHandler(fn func(c *gin.Context, err error)) Option {
	return func(o *options) { o.errorHandler = fn }
}

func defaultOnLimited(c *gin.Context, result ratelimit.Result) {
	body := gin.H{"error": "rate_limit_exceeded", "limit": result.Limit}
	if result.RetryAfter > 0 {
		body["retry_after"] = result.RetryAfter.Seconds()
	}
	c.AbortWithStatusJSON(http.StatusTooManyRequests, body)
}

// RateLimit returns a gin.HandlerFunc that rate limits requests using the given
// limiter. The default key function is KeyByIP and the default cost is 1. On
// deny it sets the standard rate-limit headers and aborts with HTTP 429.
func RateLimit(limiter ratelimit.Limiter, opts ...Option) gin.HandlerFunc {
	o := &options{keyFunc: KeyByIP(), onLimited: defaultOnLimited}
	for _, opt := range opts {
		opt(o)
	}
	return func(c *gin.Context) {
		if o.skipFunc != nil && o.skipFunc(c) {
			c.Next()
			return
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
			result = limiter.Allow(c.Request.Context(), key)
		} else {
			result = limiter.AllowN(c.Request.Context(), key, cost)
		}

		h := c.Writer.Header()
		h.Set("X-RateLimit-Limit", fmt.Sprintf("%d", result.Limit))
		h.Set("X-RateLimit-Remaining", fmt.Sprintf("%d", result.Remaining))
		h.Set("X-RateLimit-Cost", fmt.Sprintf("%d", cost))
		if result.ResetAfter > 0 {
			h.Set("X-RateLimit-Reset", fmt.Sprintf("%d", time.Now().Add(result.ResetAfter).Unix()))
		}

		if !result.Allowed {
			if result.RetryAfter > 0 {
				h.Set("Retry-After", fmt.Sprintf("%.0f", result.RetryAfter.Seconds()))
			}
			o.onLimited(c, result)
			return
		}

		c.Next()
	}
}

// clientIP mirrors ratelimit/middleware.KeyByIP: X-Forwarded-For (first hop) ->
// X-Real-IP -> RemoteAddr host.
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		return strings.TrimSpace(strings.Split(xff, ",")[0])
	}
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return xri
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
