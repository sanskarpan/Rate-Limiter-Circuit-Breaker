// Package timeout provides a simple timeout wrapper for functions.
//
// It enforces a deadline on an operation and returns context.DeadlineExceeded
// if the operation does not complete within the given duration.
// Unlike adding context.WithTimeout directly, this package provides a clean,
// composable abstraction for use in the resilience pipeline.
package timeout

import (
	"context"
	"errors"
	"time"
)

// ErrTimeout is returned when an operation exceeds the configured timeout.
// It wraps context.DeadlineExceeded so callers can use errors.Is.
var ErrTimeout = context.DeadlineExceeded

// TimeoutError is returned by Do / DoWithResult when fn does not complete before
// the deadline. It implements error and unwraps to context.DeadlineExceeded so
// that errors.Is(err, context.DeadlineExceeded) and
// errors.As(err, new(*TimeoutError)) both succeed.
type TimeoutError struct {
	// Duration is the timeout that was exceeded.
	Duration time.Duration
}

// Error implements the error interface.
func (e *TimeoutError) Error() string {
	return "timeout: operation exceeded " + e.Duration.String()
}

// Unwrap returns context.DeadlineExceeded so errors.Is/As chains work.
func (e *TimeoutError) Unwrap() error {
	return context.DeadlineExceeded
}

// Do executes fn with an enforced timeout. fn runs in its own goroutine; if it
// does not return within d, Do returns a *TimeoutError without waiting for fn.
//
// The context passed to fn is cancelled on timeout, but a fn that ignores its
// context will keep running in the background until it returns on its own; its
// eventual result is discarded. Callers must ensure fn does not hold resources
// indefinitely.
//
// If d <= 0 the function is executed synchronously without any timeout.
func Do(ctx context.Context, d time.Duration, fn func(context.Context) error) error {
	if d <= 0 {
		return fn(ctx)
	}
	ctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()

	// Buffered so the goroutine never blocks sending even if we've already
	// returned on timeout (prevents a goroutine leak on abandonment).
	errCh := make(chan error, 1)
	go func() {
		errCh <- fn(ctx)
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		// Distinguish our deadline from a parent cancellation: only report a
		// TimeoutError when the deadline was actually exceeded.
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return &TimeoutError{Duration: d}
		}
		return ctx.Err()
	}
}

// DoWithResult executes fn with an enforced timeout and returns a typed result.
// Semantics match Do: on timeout it returns the zero value of T and a
// *TimeoutError; an uncooperative fn keeps running in the background and its
// result is discarded.
func DoWithResult[T any](ctx context.Context, d time.Duration, fn func(context.Context) (T, error)) (T, error) {
	if d <= 0 {
		return fn(ctx)
	}
	ctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()

	type result struct {
		val T
		err error
	}
	resCh := make(chan result, 1)
	go func() {
		val, err := fn(ctx)
		resCh <- result{val: val, err: err}
	}()

	select {
	case r := <-resCh:
		return r.val, r.err
	case <-ctx.Done():
		var zero T
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return zero, &TimeoutError{Duration: d}
		}
		return zero, ctx.Err()
	}
}

// Timeout is a configurable timeout wrapper that can be reused.
type Timeout struct {
	duration time.Duration
}

// New creates a new Timeout with the given duration.
// If d <= 0, no timeout is applied.
func New(d time.Duration) *Timeout {
	return &Timeout{duration: d}
}

// Do executes fn with the configured timeout.
func (t *Timeout) Do(ctx context.Context, fn func(context.Context) error) error {
	return Do(ctx, t.duration, fn)
}

// Duration returns the configured timeout duration.
func (t *Timeout) Duration() time.Duration {
	return t.duration
}
