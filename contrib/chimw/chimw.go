// Package chimw provides a chi-compatible rate-limiting middleware for the
// github.com/sanskarpan/Rate-Limiter-Circuit-Breaker rate limiter.
//
// chi routers accept any stdlib middleware of the form
// func(http.Handler) http.Handler, which is exactly what the core
// ratelimit/middleware package already returns. This package is therefore a
// thin re-export of the core middleware so callers get a single, discoverable
// import path alongside the other framework adapters (ginmw, echomw, fibermw,
// connectmw). You can equally use ratelimit/middleware.RateLimit directly with
// chi's r.Use(...).
package chimw

import (
	"net/http"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
	mw "github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/middleware"
)

// Option re-exports the core middleware option type so callers can configure
// the key function, cost function, skip function and limited/error handlers
// with the same vocabulary used by the other adapters.
type Option = mw.Option

// KeyFunc re-exports the core key-extraction function type.
type KeyFunc = mw.KeyFunc

// CostFunc re-exports the core cost function type.
type CostFunc = mw.CostFunc

// Re-exported key extractors and options for a self-contained import path.
var (
	// KeyByIP extracts the client IP (X-Forwarded-For -> X-Real-IP -> RemoteAddr).
	KeyByIP = mw.KeyByIP
	// KeyByHeader extracts the key from a request header.
	KeyByHeader = mw.KeyByHeader
	// KeyByParam extracts the key from a URL query parameter.
	KeyByParam = mw.KeyByParam

	// WithKeyFunc sets a custom key extraction function (default: KeyByIP).
	WithKeyFunc = mw.WithKeyFunc
	// WithCost sets a function computing the token cost of each request (default: 1).
	WithCost = mw.WithCost
	// WithOnLimited sets a custom handler invoked when a request is rate limited.
	WithOnLimited = mw.WithOnLimited
	// WithSkipFunc sets a predicate that, when true, skips rate limiting.
	WithSkipFunc = mw.WithSkipFunc
	// WithErrorHandler sets a custom handler for internal errors.
	WithErrorHandler = mw.WithErrorHandler
)

// RateLimit returns a chi-compatible middleware (func(http.Handler) http.Handler)
// that rate limits requests using the given limiter. The default key function is
// KeyByIP and the default cost is 1. On deny it writes HTTP 429 with the standard
// X-RateLimit-* headers and a Retry-After header.
//
// Mount it with chi's r.Use:
//
//	r := chi.NewRouter()
//	r.Use(chimw.RateLimit(limiter))
func RateLimit(limiter ratelimit.Limiter, opts ...Option) func(http.Handler) http.Handler {
	return mw.RateLimit(limiter, opts...)
}
