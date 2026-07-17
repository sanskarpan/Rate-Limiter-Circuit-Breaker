// Package retry provides a configurable retry mechanism with pluggable backoff strategies.
//
// Usage:
//
//	p := &retry.Policy{
//	    MaxAttempts: 5,
//	    Backoff:     backoff.Exponential(100*time.Millisecond, 5*time.Second),
//	}
//	err := p.Do(ctx, func(ctx context.Context) error {
//	    return callExternalService(ctx)
//	})
package retry

import (
	"context"
	"errors"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/retry/backoff"
)

// ErrMaxAttemptsExceeded is returned when all retry attempts have been exhausted.
var ErrMaxAttemptsExceeded = errors.New("retry: max attempts exceeded")

// Policy defines how a function should be retried on failure.
type Policy struct {
	// MaxAttempts is the total number of calls to fn, including the first attempt.
	// A value of 1 means no retries: fn is called exactly once.
	// A value of 0 is treated as 1 (no retries).
	MaxAttempts int

	// Backoff determines the delay between consecutive attempts.
	// If nil, there is no delay between retries.
	Backoff backoff.BackoffStrategy

	// RetryIf is an optional predicate that determines whether an error should
	// trigger a retry. If nil, all non-nil errors trigger a retry.
	RetryIf func(err error) bool

	// OnRetry is called before each retry (not before the first attempt).
	// attempt is the 0-indexed retry number, err is the failure that caused
	// the retry, and nextWait is the duration the policy will sleep before
	// the next attempt.
	OnRetry func(attempt int, err error, nextWait time.Duration)

	// MaxDelay caps the backoff delay: if MaxDelay > 0, the effective delay is
	// min(backoffDelay, MaxDelay).
	MaxDelay time.Duration

	// Clock is the time source used for sleeping between retries.
	// If nil, clock.RealClock{} is used.
	Clock clock.Clock
}

// clock returns the policy's clock, defaulting to RealClock.
func (p *Policy) clock() clock.Clock {
	if p.Clock != nil {
		return p.Clock
	}
	return clock.RealClock{}
}

// maxAttempts returns the effective maximum attempts (minimum 1).
func (p *Policy) maxAttempts() int {
	if p.MaxAttempts <= 0 {
		return 1
	}
	return p.MaxAttempts
}

// shouldRetry returns true if the given error warrants a retry.
func (p *Policy) shouldRetry(err error) bool {
	if err == nil {
		return false
	}
	if p.RetryIf != nil {
		return p.RetryIf(err)
	}
	return true
}

// delay returns the effective sleep duration for the given retry attempt (0-indexed).
func (p *Policy) delay(attempt int) time.Duration {
	if p.Backoff == nil {
		return 0
	}
	d := p.Backoff.Next(attempt)
	if p.MaxDelay > 0 && d > p.MaxDelay {
		d = p.MaxDelay
	}
	if d < 0 {
		d = 0
	}
	return d
}

// Do executes fn according to the policy.
//
// fn is called up to MaxAttempts times. If fn succeeds (returns nil), Do
// returns nil immediately. If all attempts fail, Do returns the last error
// returned by fn (which may be wrapped with context from the retry policy).
//
// If ctx is cancelled while waiting between retries, Do returns ctx.Err()
// immediately without attempting another call.
func (p *Policy) Do(ctx context.Context, fn func(context.Context) error) error {
	clk := p.clock()
	max := p.maxAttempts()

	var lastErr error
	for attempt := 0; attempt < max; attempt++ {
		// Check context before each attempt.
		if ctx.Err() != nil {
			return ctx.Err()
		}

		lastErr = fn(ctx)
		if lastErr == nil {
			return nil
		}

		// This was the last attempt — do not sleep.
		if attempt == max-1 {
			break
		}

		// Determine whether we should retry this error.
		if !p.shouldRetry(lastErr) {
			return lastErr
		}

		// Compute wait duration for this retry (attempt index = attempt, 0-indexed).
		wait := p.delay(attempt)

		// Notify caller.
		if p.OnRetry != nil {
			p.OnRetry(attempt, lastErr, wait)
		}

		// Wait or bail out if context is cancelled.
		if wait > 0 {
			if err := sleepWithContext(ctx, clk, wait); err != nil {
				return err
			}
		}
	}

	return lastErr
}

// DoWithResult executes fn and returns the typed result along with any error.
// It is a generic wrapper around Policy.Do for functions that return a value.
func DoWithResult[T any](ctx context.Context, p *Policy, fn func(context.Context) (T, error)) (T, error) {
	var result T
	err := p.Do(ctx, func(ctx context.Context) error {
		var fnErr error
		result, fnErr = fn(ctx)
		return fnErr
	})
	return result, err
}

// sleepWithContext sleeps for d using the given clock. It returns ctx.Err() if
// the context is cancelled before the sleep completes.
func sleepWithContext(ctx context.Context, clk clock.Clock, d time.Duration) error {
	timer := clk.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
