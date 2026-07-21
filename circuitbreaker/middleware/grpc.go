// Package middleware provides HTTP and gRPC middleware for the circuit breaker.
package middleware

import (
	"context"
	"errors"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
)

// UnaryServerInterceptor returns a gRPC unary server interceptor that wraps
// requests with the given circuit breaker. cb may be a
// *circuitbreaker.CircuitBreaker or a *circuitbreaker.DistributedCircuitBreaker
// (both satisfy circuitbreaker.Executor).
//
// When the circuit is open: returns codes.Unavailable.
// When the downstream handler returns a gRPC error with code >= Internal: counts as CB failure.
// When the downstream handler returns a non-gRPC error: counts as CB failure.
// Client errors (NotFound, InvalidArgument, etc.) pass through without tripping the circuit.
func UnaryServerInterceptor(cb circuitbreaker.Executor) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		var resp any
		var handlerErr error // captured from the handler, passed through if not a CB failure

		err := cb.Execute(ctx, func(ctx context.Context) error {
			var e error
			resp, e = handler(ctx, req)
			if e != nil {
				// Only count server-side errors as CB failures.
				if isCBFailure(e) {
					return e
				}
				// Client error: record it so we can pass it through, but return nil to CB.
				handlerErr = e
				return nil
			}
			return nil
		})

		if err != nil {
			if errors.Is(err, circuitbreaker.ErrCircuitOpen) {
				return nil, status.Error(codes.Unavailable, "service temporarily unavailable: circuit open")
			}
			return nil, err
		}

		// Propagate client error (if any) without tripping the CB
		if handlerErr != nil {
			return nil, handlerErr
		}

		return resp, nil
	}
}

// CBUnaryServerInterceptor is a deprecated alias for UnaryServerInterceptor.
//
// Deprecated: Use UnaryServerInterceptor instead.
var CBUnaryServerInterceptor = UnaryServerInterceptor

// StreamServerInterceptor returns a gRPC stream server interceptor that wraps
// streams with the given circuit breaker. cb may be a
// *circuitbreaker.CircuitBreaker or a *circuitbreaker.DistributedCircuitBreaker
// (both satisfy circuitbreaker.Executor).
func StreamServerInterceptor(cb circuitbreaker.Executor) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		var handlerErr error

		err := cb.Execute(ss.Context(), func(ctx context.Context) error {
			e := handler(srv, ss)
			if e != nil {
				if isCBFailure(e) {
					return e
				}
				handlerErr = e
				return nil
			}
			return nil
		})

		if err != nil {
			if errors.Is(err, circuitbreaker.ErrCircuitOpen) {
				return status.Error(codes.Unavailable, "service temporarily unavailable: circuit open")
			}
			return err
		}

		if handlerErr != nil {
			return handlerErr
		}
		return nil
	}
}

// CBStreamServerInterceptor is a deprecated alias for StreamServerInterceptor.
//
// Deprecated: Use StreamServerInterceptor instead.
var CBStreamServerInterceptor = StreamServerInterceptor

// isCBFailure returns true if the error should count as a circuit breaker failure.
// gRPC codes that are considered server failures: Internal, Unavailable, DataLoss, Unknown.
// Client errors (NotFound, InvalidArgument, PermissionDenied, etc.) do NOT trip the CB.
//
// DeadlineExceeded is intentionally NOT counted: like the core library, a deadline
// is treated as caller-imposed cancellation rather than a downstream failure, so it
// must not trip the breaker (M-7).
func isCBFailure(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	if !ok {
		// Non-gRPC error — count as failure
		return true
	}
	switch st.Code() {
	case codes.Internal, codes.Unavailable, codes.DataLoss, codes.Unknown:
		return true
	default:
		return false
	}
}
