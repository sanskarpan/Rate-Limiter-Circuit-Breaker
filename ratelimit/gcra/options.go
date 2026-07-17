package gcra

import (
	"time"

	"github.com/sanskarpan/resilience/internal/clock"
)

// Option configures a GCRA limiter.
type Option func(*GCRA)

// WithClock sets a custom clock (used for testing).
func WithClock(c clock.Clock) Option {
	return func(g *GCRA) {
		g.clock = c
	}
}

// WithIdleCleanup sets how long an entry must be idle before eviction.
// Pass 0 to disable cleanup. Default: 5 minutes.
func WithIdleCleanup(d time.Duration) Option {
	return func(g *GCRA) {
		g.idleClean = d
	}
}
