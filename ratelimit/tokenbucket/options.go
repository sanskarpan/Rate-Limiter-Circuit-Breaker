package tokenbucket

import (
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/metric"
)

// Option configures a TokenBucket.
type Option func(*TokenBucket)

// WithClock sets a custom clock for deterministic testing.
func WithClock(c clock.Clock) Option {
	return func(tb *TokenBucket) { tb.clock = c }
}

// WithRecorder wires a metric.Recorder so allow/deny decisions and decision
// latency are emitted. Defaults to metric.Default() (a no-op) when unset, which
// keeps the hot path allocation-free. A nil recorder is ignored.
func WithRecorder(rec metric.Recorder) Option {
	return func(tb *TokenBucket) {
		if rec != nil {
			tb.rec = rec
		}
	}
}

// WithIdleCleanup sets the duration after which inactive buckets are evicted.
// Default is 5 minutes. Set to 0 to disable cleanup.
func WithIdleCleanup(d time.Duration) Option {
	return func(tb *TokenBucket) { tb.idleClean = d }
}
