package tokenbucket

import (
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
)

// Option configures a TokenBucket.
type Option func(*TokenBucket)

// WithClock sets a custom clock for deterministic testing.
func WithClock(c clock.Clock) Option {
	return func(tb *TokenBucket) { tb.clock = c }
}

// WithIdleCleanup sets the duration after which inactive buckets are evicted.
// Default is 5 minutes. Set to 0 to disable cleanup.
func WithIdleCleanup(d time.Duration) Option {
	return func(tb *TokenBucket) { tb.idleClean = d }
}
