package backoff

import "time"

// exponentialBackoff computes base * 2^attempt, capped at max.
type exponentialBackoff struct {
	base time.Duration
	max  time.Duration
}

// Exponential returns a BackoffStrategy that doubles the delay on each attempt,
// starting at base and capped at max. The formula is base * 2^attempt.
func Exponential(base, max time.Duration) BackoffStrategy {
	return &exponentialBackoff{base: base, max: max}
}

// Next returns base * 2^attempt, capped at max.
func (e *exponentialBackoff) Next(attempt int) time.Duration {
	// A non-positive base has no meaningful exponential growth; return 0 so it
	// is not mistaken for an overflow (which clamps to max below).
	if e.base <= 0 {
		return 0
	}
	// Use bit shifting to avoid overflow: shift at most 62 positions.
	// Beyond that the result is always >= max anyway.
	const maxShift = 62
	shift := attempt
	if shift > maxShift {
		shift = maxShift
	}
	d := e.base << uint(shift)
	// Overflow guard: if the shift caused wrap-around to negative, clamp to max.
	if d <= 0 {
		return e.max
	}
	if d > e.max {
		return e.max
	}
	return d
}
