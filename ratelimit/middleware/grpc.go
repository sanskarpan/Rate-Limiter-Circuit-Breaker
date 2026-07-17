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

// GRPCOptions configures gRPC interceptors.
type GRPCOptions struct {
	// KeyFunc extracts the rate-limit key from the context.
	// Default: uses "" (global rate limit).
	KeyFunc GRPCKeyFunc

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

		result := limiter.Allow(ctx, key)
		setGRPCRateLimitMetadata(ctx, result)

		if !result.Allowed {
			return nil, rateLimitedError(result)
		}

		return handler(ctx, req)
	}
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

		result := limiter.Allow(ss.Context(), key)
		setGRPCRateLimitMetadata(ss.Context(), result)

		if !result.Allowed {
			return rateLimitedError(result)
		}

		return handler(srv, ss)
	}
}

// setGRPCRateLimitMetadata attaches rate limit headers to the gRPC response metadata.
func setGRPCRateLimitMetadata(ctx context.Context, result ratelimit.Result) {
	md := metadata.Pairs(
		"x-ratelimit-limit", fmt.Sprintf("%d", result.Limit),
		"x-ratelimit-remaining", fmt.Sprintf("%d", result.Remaining),
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
