package connectmw

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
)

// cbOptions configures the circuit-breaker interceptor.
type cbOptions struct {
	skipFunc  func(ctx context.Context, req connect.AnyRequest) bool
	failureIf func(err error) bool
}

// CBOption configures the circuit breaker interceptor.
type CBOption func(*cbOptions)

// WithCBSkipFunc sets a predicate that, when true, bypasses the circuit breaker.
func WithCBSkipFunc(fn func(ctx context.Context, req connect.AnyRequest) bool) CBOption {
	return func(o *cbOptions) { o.skipFunc = fn }
}

// WithCBFailureIf overrides which handler errors are counted as failures against
// the breaker. The default (defaultFailureIf) counts only server-fault connect
// codes — client errors such as InvalidArgument or NotFound do NOT trip the
// breaker, mirroring how the net/http middleware treats 4xx as a success.
func WithCBFailureIf(fn func(err error) bool) CBOption {
	return func(o *cbOptions) { o.failureIf = fn }
}

// defaultFailureIf treats server-fault codes as failures and everything else
// (including nil and client-fault codes) as a success.
func defaultFailureIf(err error) bool {
	if err == nil {
		return false
	}
	switch connect.CodeOf(err) {
	case connect.CodeUnknown,
		connect.CodeInternal,
		connect.CodeUnavailable,
		connect.CodeDataLoss,
		connect.CodeDeadlineExceeded,
		connect.CodeResourceExhausted:
		return true
	default:
		return false
	}
}

// CircuitBreaker returns a connect.Interceptor that wraps unary RPCs with the
// given circuit breaker.
//
// Behaviour:
//   - When the circuit is open, the call short-circuits with a connect error of
//     code Unavailable without invoking the downstream handler.
//   - Otherwise the handler runs; a server-fault error (see WithCBFailureIf) is
//     recorded as a failure and everything else as a success. The handler's own
//     response and error are returned unchanged when the breaker is not open.
//
// The interceptor only applies to unary server handlers (IsClient == false);
// client calls and streaming requests pass through unmodified.
func CircuitBreaker(cb *circuitbreaker.CircuitBreaker, opts ...CBOption) connect.Interceptor {
	o := &cbOptions{failureIf: defaultFailureIf}
	for _, opt := range opts {
		opt(o)
	}
	return connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			if req.Spec().IsClient {
				return next(ctx, req)
			}
			if o.skipFunc != nil && o.skipFunc(ctx, req) {
				return next(ctx, req)
			}

			var resp connect.AnyResponse
			var callErr error
			err := cb.Execute(ctx, func(ctx context.Context) error {
				resp, callErr = next(ctx, req)
				if o.failureIf(callErr) {
					// Report failure to the breaker but preserve the original
					// error/response to return to the caller below.
					return fmt.Errorf("upstream error: %w", callErr)
				}
				return nil
			})
			if err != nil && (errors.Is(err, circuitbreaker.ErrCircuitOpen) ||
				errors.Is(err, circuitbreaker.ErrTooManyRequests)) {
				return nil, connect.NewError(connect.CodeUnavailable,
					fmt.Errorf("circuit open"))
			}
			// Either the call succeeded, or it failed with its own error which we
			// return verbatim (the breaker recorded it but does not mask it).
			return resp, callErr
		}
	})
}
