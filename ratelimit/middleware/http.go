// Package middleware provides HTTP middleware for rate limiting and circuit breaking.
package middleware

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/sanskarpan/resilience/ratelimit"
)

// KeyFunc extracts a rate-limit key from an HTTP request.
type KeyFunc func(r *http.Request) string

// KeyByIP extracts the client IP address.
// Order: X-Forwarded-For → X-Real-IP → RemoteAddr.
func KeyByIP() KeyFunc {
	return func(r *http.Request) string {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			parts := strings.Split(xff, ",")
			return strings.TrimSpace(parts[0])
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
}

// KeyByHeader extracts the rate-limit key from a request header.
func KeyByHeader(name string) KeyFunc {
	return func(r *http.Request) string {
		return r.Header.Get(name)
	}
}

// KeyByParam extracts the rate-limit key from a URL query parameter.
func KeyByParam(name string) KeyFunc {
	return func(r *http.Request) string {
		return r.URL.Query().Get(name)
	}
}

// options holds middleware configuration.
type options struct {
	keyFunc      KeyFunc
	onLimited    func(w http.ResponseWriter, r *http.Request, result ratelimit.Result)
	skipFunc     func(r *http.Request) bool
	errorHandler func(w http.ResponseWriter, r *http.Request, err error)
}

// Option configures the rate limit middleware.
type Option func(*options)

// WithKeyFunc sets a custom key extraction function.
func WithKeyFunc(fn KeyFunc) Option {
	return func(o *options) { o.keyFunc = fn }
}

// WithOnLimited sets a custom handler called when a request is rate limited.
func WithOnLimited(fn func(w http.ResponseWriter, r *http.Request, result ratelimit.Result)) Option {
	return func(o *options) { o.onLimited = fn }
}

// WithSkipFunc sets a function that, when it returns true, skips rate limiting.
func WithSkipFunc(fn func(r *http.Request) bool) Option {
	return func(o *options) { o.skipFunc = fn }
}

// WithErrorHandler sets a custom handler for internal errors.
func WithErrorHandler(fn func(w http.ResponseWriter, r *http.Request, err error)) Option {
	return func(o *options) { o.errorHandler = fn }
}

// defaultOnLimited writes HTTP 429 with a JSON body and standard rate limit headers.
func defaultOnLimited(w http.ResponseWriter, r *http.Request, result ratelimit.Result) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-RateLimit-Limit", fmt.Sprintf("%d", result.Limit))
	w.Header().Set("X-RateLimit-Remaining", "0")
	if result.RetryAfter > 0 {
		w.Header().Set("Retry-After", fmt.Sprintf("%.0f", result.RetryAfter.Seconds()))
	}
	w.WriteHeader(http.StatusTooManyRequests)
	body := map[string]any{
		"error": "rate_limit_exceeded",
		"limit": result.Limit,
	}
	if result.RetryAfter > 0 {
		body["retry_after"] = result.RetryAfter.Seconds()
	}
	_ = json.NewEncoder(w).Encode(body)
}

// RateLimit returns an HTTP middleware that rate limits requests using the given limiter.
//
// Standard response headers set on every allowed request (RFC 6585 + IETF draft-ietf-httpapi-ratelimit):
//
//	X-RateLimit-Limit:     configured limit
//	X-RateLimit-Remaining: remaining tokens/requests
//	X-RateLimit-Reset:     unix timestamp when the window resets
//	Retry-After:           seconds to wait (only on 429 responses)
func RateLimit(limiter ratelimit.Limiter, opts ...Option) func(http.Handler) http.Handler {
	o := &options{
		keyFunc:   KeyByIP(),
		onLimited: defaultOnLimited,
	}
	for _, opt := range opts {
		opt(o)
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if o.skipFunc != nil && o.skipFunc(r) {
				next.ServeHTTP(w, r)
				return
			}

			key := o.keyFunc(r)
			result := limiter.Allow(r.Context(), key)

			// Set standard rate limit headers.
			w.Header().Set("X-RateLimit-Limit", fmt.Sprintf("%d", result.Limit))
			w.Header().Set("X-RateLimit-Remaining", fmt.Sprintf("%d", result.Remaining))
			if result.ResetAfter > 0 {
				resetAt := time.Now().Add(result.ResetAfter)
				w.Header().Set("X-RateLimit-Reset", fmt.Sprintf("%d", resetAt.Unix()))
			}

			if !result.Allowed {
				o.onLimited(w, r, result)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
