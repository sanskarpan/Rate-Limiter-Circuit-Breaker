package adaptive

import (
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/metric"
)

// Option configures an AdaptiveLimiter.
type Option func(*AdaptiveLimiter)

// WithClock sets a custom clock (used for testing).
func WithClock(c clock.Clock) Option {
	return func(al *AdaptiveLimiter) {
		al.clock = c
	}
}

// WithRecorder wires a metric.Recorder so allow/deny decisions and decision
// latency are emitted under the "adaptive" algorithm. Defaults to
// metric.Default() (a no-op) when unset. A nil recorder is ignored.
func WithRecorder(rec metric.Recorder) Option {
	return func(al *AdaptiveLimiter) {
		if rec != nil {
			al.rec = rec
		}
	}
}

// WithAdjustInterval sets how often the limit is recalculated.
// Default: 1 second.
func WithAdjustInterval(d time.Duration) Option {
	return func(al *AdaptiveLimiter) {
		al.adjustInterval = d
	}
}

// WithLatencyThresholds sets the P99 latency thresholds for stress scoring.
// warn: latency at which stress score starts rising (default: 100ms).
// critical: latency at which stress score is maximum (default: 500ms).
func WithLatencyThresholds(warn, critical time.Duration) Option {
	return func(al *AdaptiveLimiter) {
		al.p99Warn = warn
		al.p99Critical = critical
	}
}
