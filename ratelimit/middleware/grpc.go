// Package middleware provides HTTP and gRPC middleware for rate limiting.
package middleware

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
)

// GRPCKeyFunc extracts a rate-limit key from a gRPC context.
type GRPCKeyFunc func(ctx context.Context, fullMethod string) string

// GRPCKeyByMetadata extracts the rate-limit key from a gRPC metadata header.
func GRPCKeyByMetadata(header string) GRPCKeyFunc {
	return func(ctx context.Context, _ string) string {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return ""
		}
		vals := md.Get(header)
		if len(vals) > 0 {
			return vals[0]
		}
		return ""
	}
}

// GRPCKeyByMethod returns a KeyFunc that uses the gRPC method name as the key.
func GRPCKeyByMethod() GRPCKeyFunc {
	return func(_ context.Context, fullMethod string) string {
		return fullMethod
	}
}

// GRPCCostFunc computes the token cost (weight) of a gRPC call from its context
// and full method name. It must return a positive value; a value < 1 is clamped
// to 1 so a call always consumes at least one token.
type GRPCCostFunc func(ctx context.Context, fullMethod string) int

// GRPCOptions configures gRPC interceptors.
type GRPCOptions struct {
	// KeyFunc extracts the rate-limit key from the context.
	// Default: uses "" (global rate limit).
	KeyFunc GRPCKeyFunc

	// CostFunc computes the token cost (weight) of each call from its context and
	// full method name. When set, the interceptor consumes that many tokens via
	// AllowN instead of a single token. Costs below 1 are clamped to 1.
	// Default: every call costs 1.
	CostFunc GRPCCostFunc

	// SkipMethods is a list of full method names to skip (e.g., "/grpc.health.v1.Health/Check").
	SkipMethods []string
}

// UnaryServerInterceptor returns a gRPC unary server interceptor that rate limits requests.
// On rate limit: returns codes.ResourceExhausted with rate limit headers in response metadata.
func UnaryServerInterceptor(limiter ratelimit.Limiter, opts ...func(*GRPCOptions)) grpc.UnaryServerInterceptor {
	o := &GRPCOptions{}
	for _, opt := range opts {
		opt(o)
	}
	skipSet := make(map[string]bool, len(o.SkipMethods))
	for _, m := range o.SkipMethods {
		skipSet[m] = true
	}

	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if skipSet[info.FullMethod] {
			return handler(ctx, req)
		}

		key := ""
		if o.KeyFunc != nil {
			key = o.KeyFunc(ctx, info.FullMethod)
		}

		cost := grpcCost(o.CostFunc, ctx, info.FullMethod)
		result := grpcAllow(limiter, ctx, key, cost)
		setGRPCRateLimitMetadata(ctx, result, cost)

		if !result.Allowed {
			return nil, rateLimitedError(result)
		}

		return handler(ctx, req)
	}
}

// grpcCost resolves the token cost for a call, clamping any value below 1 to 1
// and defaulting to 1 when no CostFunc is configured.
func grpcCost(fn GRPCCostFunc, ctx context.Context, fullMethod string) int {
	if fn == nil {
		return 1
	}
	if c := fn(ctx, fullMethod); c > 1 {
		return c
	}
	return 1
}

// grpcAllow consumes cost tokens, using the single-token fast path when cost==1.
func grpcAllow(limiter ratelimit.Limiter, ctx context.Context, key string, cost int) ratelimit.Result {
	if cost == 1 {
		return limiter.Allow(ctx, key)
	}
	return limiter.AllowN(ctx, key, cost)
}

// StreamServerInterceptor returns a gRPC stream server interceptor that rate limits requests.
func StreamServerInterceptor(limiter ratelimit.Limiter, opts ...func(*GRPCOptions)) grpc.StreamServerInterceptor {
	o := &GRPCOptions{}
	for _, opt := range opts {
		opt(o)
	}
	skipSet := make(map[string]bool, len(o.SkipMethods))
	for _, m := range o.SkipMethods {
		skipSet[m] = true
	}

	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if skipSet[info.FullMethod] {
			return handler(srv, ss)
		}

		key := ""
		if o.KeyFunc != nil {
			key = o.KeyFunc(ss.Context(), info.FullMethod)
		}

		cost := grpcCost(o.CostFunc, ss.Context(), info.FullMethod)
		result := grpcAllow(limiter, ss.Context(), key, cost)
		setGRPCRateLimitMetadata(ss.Context(), result, cost)

		if !result.Allowed {
			return rateLimitedError(result)
		}

		return handler(srv, ss)
	}
}

// setGRPCRateLimitMetadata attaches rate limit headers (including the consumed
// cost) to the gRPC response metadata.
func setGRPCRateLimitMetadata(ctx context.Context, result ratelimit.Result, cost int) {
	md := metadata.Pairs(
		"x-ratelimit-limit", fmt.Sprintf("%d", result.Limit),
		"x-ratelimit-remaining", fmt.Sprintf("%d", result.Remaining),
		"x-ratelimit-cost", fmt.Sprintf("%d", cost),
	)
	if result.ResetAfter > 0 {
		resetAt := time.Now().Add(result.ResetAfter).Unix()
		md.Append("x-ratelimit-reset", fmt.Sprintf("%d", resetAt))
	}
	grpc.SetHeader(ctx, md) //nolint:errcheck
}

// rateLimitedError returns a gRPC ResourceExhausted error with retry-after info.
func rateLimitedError(result ratelimit.Result) error {
	msg := "rate limit exceeded"
	if result.RetryAfter > 0 {
		msg = fmt.Sprintf("rate limit exceeded, retry after %.2fs", result.RetryAfter.Seconds())
	}
	return status.Error(codes.ResourceExhausted, msg)
}

// GRPCWithKeyFunc is an option setter for GRPCOptions.
func GRPCWithKeyFunc(fn GRPCKeyFunc) func(*GRPCOptions) {
	return func(o *GRPCOptions) { o.KeyFunc = fn }
}

// GRPCWithSkipMethods sets methods to skip from rate limiting.
func GRPCWithSkipMethods(methods ...string) func(*GRPCOptions) {
	return func(o *GRPCOptions) { o.SkipMethods = methods }
}

// GRPCWithCost sets a function that computes the token cost (weight) of each
// call. The interceptor then consumes that many tokens via AllowN. Costs below 1
// are clamped to 1. Default: every call costs 1.
func GRPCWithCost(fn GRPCCostFunc) func(*GRPCOptions) {
	return func(o *GRPCOptions) { o.CostFunc = fn }
}
