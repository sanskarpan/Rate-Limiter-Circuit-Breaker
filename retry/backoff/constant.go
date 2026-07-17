package backoff

import "time"

// constantBackoff always returns the same duration regardless of the attempt number.
type constantBackoff struct {
	d time.Duration
}

// Constant returns a BackoffStrategy that always waits d between retries.
func Constant(d time.Duration) BackoffStrategy {
	return &constantBackoff{d: d}
}

// Next returns the constant duration for every attempt.
func (c *constantBackoff) Next(_ int) time.Duration {
	return c.d
}
