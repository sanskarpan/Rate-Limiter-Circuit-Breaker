package tokenbucket

import (
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/metric"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
)

// Option configures a TokenBucket.
type Option func(*TokenBucket)

// WithOnDecision registers a hook fired after every Allow/AllowN decision
// (both allow and deny), receiving the key and the resulting Result. The
// default is nil (a cheap no-op guarded by a nil check on the hot path). The
// hook runs synchronously on the calling goroutine before the decision is
// returned, so keep it fast and non-blocking; offload heavy work to a
// goroutine or buffered channel. A nil hook is ignored.
func WithOnDecision(fn func(key string, r ratelimit.Result)) Option {
	return func(tb *TokenBucket) {
		if fn != nil {
			tb.onDecision = fn
		}
	}
}

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
