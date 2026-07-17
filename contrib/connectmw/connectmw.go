// Package connectmw provides a connectrpc.com/connect interceptor that rate
// limits unary RPCs using the github.com/sanskarpan/Rate-Limiter-Circuit-Breaker
// rate limiter.
//
// It reuses the core ratelimit.Limiter and honours the WithCost weighting
// semantics. On deny it returns connect.NewError(connect.CodeResourceExhausted, ...)
// and sets the standard X-RateLimit-* / Retry-After response headers on the
// returned connect error's metadata so gateways and clients can observe them.
//
// The interceptor only applies to unary requests (IsClient == false). Streaming
// requests pass through unmodified, matching how the gRPC unary interceptor works.
package connectmw

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"connectrpc.com/connect"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
)

// KeyFunc extracts a rate-limit key from a unary request. The default derives the
// key from the peer address; use KeyByHeader for API keys or KeyByPeer explicitly.
type KeyFunc func(ctx context.Context, req connect.AnyRequest) string

// CostFunc computes the token cost (weight) of a request. Values below 1 are
// clamped to 1, so a request always consumes at least one token.
type CostFunc func(ctx context.Context, req connect.AnyRequest) int

// KeyByPeer extracts the client address from the request peer.
// Order: X-Forwarded-For -> X-Real-IP header -> Peer.Addr host.
func KeyByPeer() KeyFunc {
	return func(_ context.Context, req connect.AnyRequest) string {
		h := req.Header()
		if xff := h.Get("X-Forwarded-For"); xff != "" {
			return strings.TrimSpace(strings.Split(xff, ",")[0])
		}
		if xri := h.Get("X-Real-IP"); xri != "" {
			return xri
		}
		addr := req.Peer().Addr
		if host, _, err := net.SplitHostPort(addr); err == nil {
			return host
		}
		return addr
	}
}

// KeyByHeader extracts the rate-limit key from a request header.
func KeyByHeader(name string) KeyFunc {
	return func(_ context.Context, req connect.AnyRequest) string {
		return req.Header().Get(name)
	}
}

type options struct {
	keyFunc  KeyFunc
	costFunc CostFunc
	skipFunc func(ctx context.Context, req connect.AnyRequest) bool
}

// Option configures the rate limit interceptor.
type Option func(*options)

// WithKeyFunc sets a custom key extraction function (default: KeyByPeer).
func WithKeyFunc(fn KeyFunc) Option { return func(o *options) { o.keyFunc = fn } }

// WithCost sets a function computing the token cost of each request (default: 1).
func WithCost(fn CostFunc) Option { return func(o *options) { o.costFunc = fn } }

// WithSkipFunc sets a predicate that, when true, skips rate limiting.
func WithSkipFunc(fn func(ctx context.Context, req connect.AnyRequest) bool) Option {
	return func(o *options) { o.skipFunc = fn }
}

// RateLimit returns a connect.Interceptor that rate limits unary RPCs using the
// given limiter. The default key function is KeyByPeer and the default cost is 1.
// On deny it returns a connect error with code ResourceExhausted carrying the
// standard rate-limit headers in its metadata.
func RateLimit(limiter ratelimit.Limiter, opts ...Option) connect.Interceptor {
	o := &options{keyFunc: KeyByPeer()}
	for _, opt := range opts {
		opt(o)
	}
	return connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			// Only limit server-side unary handlers; leave client calls alone.
			if req.Spec().IsClient {
				return next(ctx, req)
			}
			if o.skipFunc != nil && o.skipFunc(ctx, req) {
				return next(ctx, req)
			}

			key := o.keyFunc(ctx, req)

			cost := 1
			if o.costFunc != nil {
				if v := o.costFunc(ctx, req); v > 1 {
					cost = v
				}
			}

			var result ratelimit.Result
			if cost == 1 {
				result = limiter.Allow(ctx, key)
			} else {
				result = limiter.AllowN(ctx, key, cost)
			}

			if !result.Allowed {
				err := connect.NewError(connect.CodeResourceExhausted,
					fmt.Errorf("rate limit exceeded (limit %d)", result.Limit))
				setRateLimitMeta(err.Meta(), result, cost)
				return nil, err
			}

			resp, err := next(ctx, req)
			if err == nil && resp != nil {
				setRateLimitMeta(resp.Header(), result, cost)
			}
			return resp, err
		}
	})
}

func setRateLimitMeta(h interface{ Set(string, string) }, result ratelimit.Result, cost int) {
	h.Set("X-RateLimit-Limit", fmt.Sprintf("%d", result.Limit))
	h.Set("X-RateLimit-Remaining", fmt.Sprintf("%d", result.Remaining))
	h.Set("X-RateLimit-Cost", fmt.Sprintf("%d", cost))
	if result.ResetAfter > 0 {
		h.Set("X-RateLimit-Reset", fmt.Sprintf("%d", time.Now().Add(result.ResetAfter).Unix()))
	}
	if result.RetryAfter > 0 {
		h.Set("Retry-After", fmt.Sprintf("%.0f", result.RetryAfter.Seconds()))
	}
}
