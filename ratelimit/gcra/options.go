package gcra

import (
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/metric"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
)

// Option configures a GCRA limiter.
type Option func(*GCRA)

// WithOnDecision registers a hook fired after every Allow/AllowN decision
// (both allow and deny), receiving the key and the resulting Result. The
// default is nil (a cheap no-op guarded by a nil check on the hot path). The
// hook runs synchronously on the calling goroutine before the decision is
// returned, so keep it fast and non-blocking. A nil hook is ignored.
func WithOnDecision(fn func(key string, r ratelimit.Result)) Option {
	return func(g *GCRA) {
		if fn != nil {
			g.onDecision = fn
		}
	}
}

// WithClock sets a custom clock (used for testing).
func WithClock(c clock.Clock) Option {
	return func(g *GCRA) {
		g.clock = c
	}
}

// WithRecorder wires a metric.Recorder so allow/deny decisions and decision
// latency are emitted. Defaults to metric.Default() (a no-op) when unset. A nil
// recorder is ignored.
func WithRecorder(rec metric.Recorder) Option {
	return func(g *GCRA) {
		if rec != nil {
			g.rec = rec
		}
	}
}

// WithIdleCleanup sets how long an entry must be idle before eviction.
// Pass 0 to disable cleanup. Default: 5 minutes.
func WithIdleCleanup(d time.Duration) Option {
	return func(g *GCRA) {
		g.idleClean = d
	}
}
