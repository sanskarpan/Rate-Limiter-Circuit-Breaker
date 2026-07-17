package adaptive

import (
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
)

// Option configures an AdaptiveLimiter.
type Option func(*AdaptiveLimiter)

// WithClock sets a custom clock (used for testing).
func WithClock(c clock.Clock) Option {
	return func(al *AdaptiveLimiter) {
		al.clock = c
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
