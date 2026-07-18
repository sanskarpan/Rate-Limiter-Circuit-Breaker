package resiliencex

import (
	"context"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/bulkhead"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/retry"
)

// ExecuteCB runs fn under the protection of cb and returns fn's typed result.
//
// It is the value-returning counterpart of circuitbreaker.CircuitBreaker.Execute:
// the circuit-breaker state machine, timeout handling, panic recovery, and
// metrics all behave exactly as they do for a bare cb.Execute call. fn is
// invoked at most once and only if the breaker permits it.
//
// The error returned by cb.Execute is propagated verbatim, so sentinel checks
// keep working through the wrapper — for example
// errors.Is(err, circuitbreaker.ErrCircuitOpen) when the circuit is open, or
// errors.Is(err, circuitbreaker.ErrTooManyRequests) when a half-open probe slot
// is unavailable. When err is non-nil (whether it originates from the breaker or
// from fn) the returned value is the zero value of T; the value is meaningful
// only when err is nil.
//
// ExecuteCB adds no locking of its own; it is safe for concurrent use to exactly
// the same extent as cb.
func ExecuteCB[T any](
	ctx context.Context,
	cb *circuitbreaker.CircuitBreaker,
	fn func(context.Context) (T, error),
) (T, error) {
	var result T
	err := cb.Execute(ctx, func(ctx context.Context) error {
		var fnErr error
		result, fnErr = fn(ctx)
		return fnErr
	})
	if err != nil {
		var zero T
		return zero, err
	}
	return result, nil
}

// ExecuteCBWithFallback runs fn under cb and, if the primary path fails or the
// circuit rejects the call, resolves the result via fallback.
//
// It mirrors circuitbreaker.CircuitBreaker.ExecuteWithFallback for typed values:
// fn is attempted through the breaker; on any error (including
// circuitbreaker.ErrCircuitOpen) fallback is invoked with that error and its
// (value, error) result is returned. When fallback itself returns a non-nil
// error, the returned value is the zero value of T and that error is propagated
// verbatim.
//
// ExecuteCBWithFallback adds no locking of its own; it is safe for concurrent
// use to exactly the same extent as cb.
func ExecuteCBWithFallback[T any](
	ctx context.Context,
	cb *circuitbreaker.CircuitBreaker,
	fn func(context.Context) (T, error),
	fallback func(context.Context, error) (T, error),
) (T, error) {
	result, err := ExecuteCB(ctx, cb, fn)
	if err != nil {
		fbResult, fbErr := fallback(ctx, err)
		if fbErr != nil {
			var zero T
			return zero, fbErr
		}
		return fbResult, nil
	}
	return result, nil
}

// ExecuteRetry runs fn under the retry Policy p and returns fn's typed result.
//
// It is the value-returning counterpart of retry.Policy.Do: fn is called up to
// p.MaxAttempts times with the policy's backoff, RetryIf predicate, budget, and
// callbacks applied unchanged. On success the value from the successful attempt
// is returned. If every attempt fails, the last error from fn is returned; if
// ctx is cancelled while waiting between attempts, ctx.Err() is returned. In all
// error cases the returned value is the zero value of T.
//
// The error is whatever p.Do returns, propagated verbatim (for example a
// sentinel checkable with errors.Is against the caller's own error, or
// context.Canceled / context.DeadlineExceeded). ExecuteRetry adds no locking of
// its own; it is safe for concurrent use to exactly the same extent as p.
func ExecuteRetry[T any](
	ctx context.Context,
	p *retry.Policy,
	fn func(context.Context) (T, error),
) (T, error) {
	var result T
	err := p.Do(ctx, func(ctx context.Context) error {
		var fnErr error
		result, fnErr = fn(ctx)
		return fnErr
	})
	if err != nil {
		var zero T
		return zero, err
	}
	return result, nil
}

// ExecuteBulkhead runs fn under the concurrency limit enforced by b and returns
// fn's typed result.
//
// It is the value-returning counterpart of bulkhead.Bulkhead.Execute: a
// concurrency slot is acquired (respecting the bulkhead's maxWait), fn is run
// while the slot is held, and the slot is always released afterwards. If no slot
// becomes available in time the call is rejected with bulkhead.ErrBulkheadFull,
// and if ctx is cancelled while waiting the context error is returned; both are
// propagated verbatim so errors.Is(err, bulkhead.ErrBulkheadFull) keeps working.
//
// On any error the returned value is the zero value of T. ExecuteBulkhead adds
// no locking of its own; it is safe for concurrent use to exactly the same
// extent as b.
func ExecuteBulkhead[T any](
	ctx context.Context,
	b *bulkhead.Bulkhead,
	fn func(context.Context) (T, error),
) (T, error) {
	var result T
	err := b.Execute(ctx, func(ctx context.Context) error {
		var fnErr error
		result, fnErr = fn(ctx)
		return fnErr
	})
	if err != nil {
		var zero T
		return zero, err
	}
	return result, nil
}
