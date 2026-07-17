package leakybucket

import (
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/metric"
)

// Option configures a LeakyBucket.
type Option func(*LeakyBucket)

// WithClock sets a custom clock (used for testing).
func WithClock(c clock.Clock) Option {
	return func(lb *LeakyBucket) {
		lb.clock = c
	}
}

// WithRecorder wires a metric.Recorder so allow/deny decisions and decision
// latency are emitted. Defaults to metric.Default() (a no-op) when unset. A nil
// recorder is ignored.
func WithRecorder(rec metric.Recorder) Option {
	return func(lb *LeakyBucket) {
		if rec != nil {
			lb.rec = rec
		}
	}
}

// WithIdleCleanup sets how long a queue must be idle before eviction.
// Pass 0 to disable cleanup. Default: 5 minutes.
func WithIdleCleanup(d time.Duration) Option {
	return func(lb *LeakyBucket) {
		lb.idleClean = d
	}
}
