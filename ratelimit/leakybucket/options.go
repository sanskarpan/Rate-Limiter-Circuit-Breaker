package leakybucket

import (
	"time"

	"github.com/sanskarpan/resilience/internal/clock"
)

// Option configures a LeakyBucket.
type Option func(*LeakyBucket)

// WithClock sets a custom clock (used for testing).
func WithClock(c clock.Clock) Option {
	return func(lb *LeakyBucket) {
		lb.clock = c
	}
}

// WithIdleCleanup sets how long a queue must be idle before eviction.
// Pass 0 to disable cleanup. Default: 5 minutes.
func WithIdleCleanup(d time.Duration) Option {
	return func(lb *LeakyBucket) {
		lb.idleClean = d
	}
}
