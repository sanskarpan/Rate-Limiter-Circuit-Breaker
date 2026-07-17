// Package adaptive implements an adaptive rate limiter that adjusts its limit
// based on system health signals (CPU utilization, error rate, P99 latency).
//
// The adaptive limiter wraps a token bucket and periodically adjusts its
// effective rate limit based on observed system metrics. When the system is
// under stress, the limit decreases; when the system is healthy, the limit
// gradually increases up to a configured maximum.
//
// Adjustment algorithm (runs every adjustInterval, default 1s). It follows the
// threshold rules mandated by SPEC.md (authoritative), not a weighted score:
//
//	// Decrease 10% (floor at min) if the system shows ANY sign of stress:
//	if CPUPercent > 80 || ErrorRate > 0.05 || P99Latency > threshold:
//	    target = current * 0.9
//	// Increase 5% (ceil at max) only if EVERY signal is healthy:
//	else if CPUPercent < 50 && ErrorRate < 0.01 && P99Latency < threshold*0.5:
//	    target = current * 1.05
//	else:
//	    no change
//
//	// Gradient smoothing prevents oscillation:
//	new_limit = current*0.9 + target*0.1
//
// "threshold" is the P99 latency threshold configured via WithLatencyThresholds
// (the "critical" value; default 500ms). Because integer truncation of the
// smoothed value can otherwise stall small limits, a triggered adjustment always
// moves the limit by at least one step in the chosen direction when the min/max
// bounds allow it (C-6).
//
// All methods on AdaptiveLimiter are safe for concurrent use.
package adaptive

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sanskarpan/resilience/internal/clock"
	"github.com/sanskarpan/resilience/ratelimit"
	"github.com/sanskarpan/resilience/ratelimit/tokenbucket"
)

const algorithmName = "adaptive"

// AdaptiveLimiter wraps a token bucket and adjusts its limit based on signals.
// All methods are safe for concurrent use.
type AdaptiveLimiter struct {
	signals SignalSource

	minLimit       int
	maxLimit       int
	adjustInterval time.Duration

	// currentLimit is the effective token bucket limit (atomic for reads)
	currentLimit atomic.Int64

	// inner is created once and retuned in place via SetLimit; it is never
	// rebuilt, so per-key token state and in-flight Wait callers are preserved
	// across adjustments (H-12). mu guards adjust's read-modify-write of the
	// limit and the pointer for Close.
	mu    sync.Mutex
	inner *tokenbucket.TokenBucket

	clock   clock.Clock
	done    chan struct{}
	wg      sync.WaitGroup
	closed  bool
	closeMu sync.Mutex

	// P99 latency thresholds (configurable via WithLatencyThresholds). p99Critical
	// is the authoritative "threshold" in the SPEC rules: decrease when
	// P99 > p99Critical, increase only when P99 < p99Critical/2. p99Warn is
	// retained for backward-compatible configuration.
	p99Warn     time.Duration
	p99Critical time.Duration
}

// New creates an AdaptiveLimiter.
// initialLimit is the starting rate limit (requests/second).
// minLimit and maxLimit bound how far the limit can adjust.
func New(initialLimit, minLimit, maxLimit int, signals SignalSource, opts ...Option) *AdaptiveLimiter {
	al := &AdaptiveLimiter{
		signals:        signals,
		minLimit:       minLimit,
		maxLimit:       maxLimit,
		adjustInterval: time.Second,
		clock:          clock.RealClock{},
		done:           make(chan struct{}),
		p99Warn:        100 * time.Millisecond,
		p99Critical:    500 * time.Millisecond,
	}
	al.currentLimit.Store(int64(initialLimit))
	for _, opt := range opts {
		opt(al)
	}
	al.inner = tokenbucket.New(float64(initialLimit), float64(initialLimit),
		tokenbucket.WithClock(al.clock))
	al.wg.Add(1)
	go al.adjustLoop()
	return al
}

// Allow checks if 1 request is allowed. Non-blocking.
func (al *AdaptiveLimiter) Allow(ctx context.Context, key string) ratelimit.Result {
	// inner is immutable after New (SetLimit retunes it in place), so no lock needed.
	inner := al.inner
	result := inner.Allow(ctx, key)
	result.Algorithm = algorithmName
	result.Limit = int(al.currentLimit.Load())
	return result
}

// AllowN checks if n requests are allowed. Non-blocking.
func (al *AdaptiveLimiter) AllowN(ctx context.Context, key string, n int) ratelimit.Result {
	// inner is immutable after New (SetLimit retunes it in place), so no lock needed.
	inner := al.inner
	result := inner.AllowN(ctx, key, n)
	result.Algorithm = algorithmName
	result.Limit = int(al.currentLimit.Load())
	return result
}

// Wait blocks until 1 token is available or ctx is cancelled.
func (al *AdaptiveLimiter) Wait(ctx context.Context, key string) error {
	// inner is immutable after New (SetLimit retunes it in place), so no lock needed.
	inner := al.inner
	return inner.Wait(ctx, key)
}

// WaitN blocks until n tokens are available or ctx is cancelled.
func (al *AdaptiveLimiter) WaitN(ctx context.Context, key string, n int) error {
	// inner is immutable after New (SetLimit retunes it in place), so no lock needed.
	inner := al.inner
	return inner.WaitN(ctx, key, n)
}

// Peek returns current state without consuming a token.
func (al *AdaptiveLimiter) Peek(ctx context.Context, key string) ratelimit.State {
	// inner is immutable after New (SetLimit retunes it in place), so no lock needed.
	inner := al.inner
	state := inner.Peek(ctx, key)
	state.Algorithm = algorithmName
	state.Limit = int(al.currentLimit.Load())
	if state.Extra == nil {
		state.Extra = make(map[string]any)
	}
	state.Extra["current_limit"] = int(al.currentLimit.Load())
	state.Extra["min_limit"] = al.minLimit
	state.Extra["max_limit"] = al.maxLimit
	state.Extra["cpu_percent"] = al.signals.CPUPercent()
	state.Extra["error_rate"] = al.signals.ErrorRate()
	state.Extra["p99_latency"] = al.signals.P99Latency()
	return state
}

// Reset removes all state for the given key.
func (al *AdaptiveLimiter) Reset(ctx context.Context, key string) error {
	// inner is immutable after New (SetLimit retunes it in place), so no lock needed.
	inner := al.inner
	return inner.Reset(ctx, key)
}

// Close stops the adjustment goroutine and the inner token bucket.
func (al *AdaptiveLimiter) Close() error {
	al.closeMu.Lock()
	defer al.closeMu.Unlock()
	if al.closed {
		return nil
	}
	al.closed = true
	close(al.done)
	al.wg.Wait()

	// inner is immutable after New (SetLimit retunes it in place), so no lock needed.
	inner := al.inner
	return inner.Close()
}

// String returns a human-readable description.
func (al *AdaptiveLimiter) String() string {
	return fmt.Sprintf("AdaptiveLimiter(current=%d, min=%d, max=%d)",
		al.currentLimit.Load(), al.minLimit, al.maxLimit)
}

// CurrentLimit returns the current effective limit.
func (al *AdaptiveLimiter) CurrentLimit() int {
	return int(al.currentLimit.Load())
}

// ForceAdjust triggers an immediate adjustment cycle (useful for testing).
func (al *AdaptiveLimiter) ForceAdjust() {
	al.adjust()
}

// adjustDirection reports the direction the limit should move this cycle based
// on the SPEC threshold rules:
//
//	-1 → decrease (stress): CPU>80 OR ErrorRate>0.05 OR P99>threshold
//	+1 → increase (healthy): CPU<50 AND ErrorRate<0.01 AND P99<threshold*0.5
//	 0 → hold
//
// Decrease takes precedence: any single stress signal forces a back-off even if
// the others look healthy (so e.g. ErrorRate>0.05 alone triggers a decrease).
// The P99 "threshold" is p99Critical (configurable via WithLatencyThresholds).
func (al *AdaptiveLimiter) adjustDirection() int {
	cpu := al.signals.CPUPercent()
	errRate := al.signals.ErrorRate()
	p99 := al.signals.P99Latency()
	threshold := al.p99Critical

	// Stress: any one signal over its limit triggers a decrease.
	if cpu > 80 || errRate > 0.05 || p99 > threshold {
		return -1
	}
	// Healthy: every signal must be comfortably below its limit to increase.
	if cpu < 50 && errRate < 0.01 && p99 < threshold/2 {
		return 1
	}
	return 0
}

// adjustLoop periodically adjusts the limit based on signals.
func (al *AdaptiveLimiter) adjustLoop() {
	defer al.wg.Done()
	ticker := al.clock.NewTicker(al.adjustInterval)
	defer ticker.Stop()
	for {
		select {
		case <-al.done:
			return
		case <-ticker.C():
			al.adjust()
		}
	}
}

// adjust computes the new limit from the SPEC threshold rules and retunes the
// inner token bucket in place (H-12) — it never rebuilds the bucket, so per-key
// token balances and in-flight Wait callers survive the adjustment.
func (al *AdaptiveLimiter) adjust() {
	// Serialize adjustments so concurrent ForceAdjust/tick cycles don't interleave
	// their read-modify-write of currentLimit.
	al.mu.Lock()
	defer al.mu.Unlock()

	current := int(al.currentLimit.Load())
	dir := al.adjustDirection()
	if dir == 0 {
		return // no adjustment
	}

	var target float64
	switch dir {
	case -1:
		target = float64(current) * 0.9 // decrease 10%
	case 1:
		target = float64(current) * 1.05 // increase 5%
	}

	// Gradient smoothing (SPEC): new = current*0.9 + target*0.1. math.Round
	// avoids the integer-truncation no-op that would otherwise stall small limits.
	smoothed := float64(current)*0.9 + target*0.1
	newLimit := int(math.Round(smoothed))

	// C-6: guarantee at least a one-step move in the triggered direction, so a
	// small current (where the 5%/10% delta rounds back to `current`) is never
	// stuck. Bounds still take precedence below.
	if dir == 1 && newLimit <= current {
		newLimit = current + 1
	}
	if dir == -1 && newLimit >= current {
		newLimit = current - 1
	}

	// Clamp to [min, max].
	if newLimit < al.minLimit {
		newLimit = al.minLimit
	}
	if newLimit > al.maxLimit {
		newLimit = al.maxLimit
	}
	if newLimit == current {
		return // already at the bound in the requested direction
	}

	// Retune the existing bucket in place rather than replacing it (H-12).
	al.inner.SetLimit(float64(newLimit), float64(newLimit))
	al.currentLimit.Store(int64(newLimit))
}
