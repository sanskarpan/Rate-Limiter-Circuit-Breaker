package composite

import (
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/metric"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
)

// Option configures a CompositeLimiter at construction time.
// Pass one or more Option values as trailing arguments to New.
type Option func(*CompositeLimiter)

// WithClock sets the clock used by WaitN's retry timer. Inject a ManualClock
// in tests so WaitN wakes deterministically when the clock is advanced.
// Matches the pattern used by gcra.WithClock, tokenbucket.WithClock, etc.
func WithClock(clk clock.Clock) Option {
	return func(c *CompositeLimiter) {
		c.clock = clk
	}
}

// WithRecorder wires a metric.Recorder so the composite's own allow/deny
// decision and decision latency are emitted under composite_and/composite_or.
// Defaults to metric.Default() (a no-op) when unset. A nil recorder is ignored.
func WithRecorder(rec metric.Recorder) Option {
	return func(c *CompositeLimiter) {
		if rec != nil {
			c.rec = rec
		}
	}
}

// WithOnDecision registers a hook fired after every Allow/AllowN decision (both
// allow and deny), receiving the key and the composite's final Result. The
// default is nil (a cheap no-op guarded by a nil check on the hot path). The
// hook runs synchronously on the calling goroutine before the decision is
// returned, so keep it fast and non-blocking. A nil hook is ignored.
func WithOnDecision(fn func(key string, r ratelimit.Result)) Option {
	return func(c *CompositeLimiter) {
		if fn != nil {
			c.onDecision = fn
		}
	}
}
