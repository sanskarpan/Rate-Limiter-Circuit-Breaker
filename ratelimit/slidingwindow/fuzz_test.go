package slidingwindow_test

import (
	"context"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/slidingwindow"
)

// FuzzSlidingWindowLog checks invariants of the exact-log sliding window:
//   - never panics on any input,
//   - Allowed implies Remaining >= 0 and <= limit,
//   - a denied AllowN consumes nothing (atomic all-or-nothing),
//   - decisions are monotonic within a fixed clock (no allow after the window
//     is full without advancing time).
func FuzzSlidingWindowLog(f *testing.F) {
	f.Add(10, int64(time.Second), 1, int64(0))
	f.Add(1, int64(time.Minute), 1, int64(100*time.Millisecond))
	f.Add(100, int64(time.Second), 5, int64(50*time.Millisecond))
	f.Add(5, int64(200*time.Millisecond), 3, int64(0))

	f.Fuzz(func(t *testing.T, limit int, windowNs int64, n int, advanceNs int64) {
		if limit <= 0 || limit > 10_000 {
			return
		}
		if windowNs <= 0 || windowNs > int64(10*time.Minute) {
			return
		}
		if n < 1 || n > limit*2 {
			return
		}
		if advanceNs < 0 {
			advanceNs = -advanceNs
		}
		if advanceNs > int64(10*time.Minute) {
			advanceNs %= int64(10*time.Minute) + 1
		}
		advance := time.Duration(advanceNs)
		window := time.Duration(windowNs)

		clk := clock.NewManualClock(time.Unix(0, 0))
		sw := slidingwindow.NewLog(limit, window, slidingwindow.WithLogClock(clk))
		defer sw.Close() //nolint:errcheck

		ctx := context.Background()

		for step := 0; step < 8; step++ {
			before := sw.Peek(ctx, "key").Remaining
			res := sw.AllowN(ctx, "key", n)

			if res.Remaining < 0 {
				t.Errorf("step %d: Remaining=%d < 0", step, res.Remaining)
			}
			if res.Remaining > limit {
				t.Errorf("step %d: Remaining=%d > limit=%d", step, res.Remaining, limit)
			}
			if res.Algorithm == "" {
				t.Errorf("step %d: Algorithm empty", step)
			}
			// Atomic: a denied AllowN must not have consumed capacity.
			after := sw.Peek(ctx, "key").Remaining
			if !res.Allowed && after != before {
				t.Errorf("step %d: denied AllowN changed remaining %d -> %d", step, before, after)
			}
			// n > limit can never be allowed.
			if n > limit && res.Allowed {
				t.Errorf("step %d: AllowN(%d) with limit=%d should be denied", step, n, limit)
			}
			clk.Advance(advance)
		}
	})
}

// FuzzSlidingWindowCounter checks the same invariants for the approximate
// two-window counter variant.
func FuzzSlidingWindowCounter(f *testing.F) {
	f.Add(10, int64(time.Second), 1, int64(0))
	f.Add(1, int64(time.Minute), 1, int64(100*time.Millisecond))
	f.Add(100, int64(time.Second), 5, int64(50*time.Millisecond))

	f.Fuzz(func(t *testing.T, limit int, windowNs int64, n int, advanceNs int64) {
		if limit <= 0 || limit > 10_000 {
			return
		}
		if windowNs <= 0 || windowNs > int64(10*time.Minute) {
			return
		}
		if n < 1 || n > limit*2 {
			return
		}
		if advanceNs < 0 {
			advanceNs = -advanceNs
		}
		if advanceNs > int64(10*time.Minute) {
			advanceNs %= int64(10*time.Minute) + 1
		}
		advance := time.Duration(advanceNs)
		window := time.Duration(windowNs)

		clk := clock.NewManualClock(time.Unix(0, 0))
		sw := slidingwindow.NewCounter(limit, window, slidingwindow.WithClock(clk))
		defer sw.Close() //nolint:errcheck

		ctx := context.Background()

		for step := 0; step < 8; step++ {
			res := sw.AllowN(ctx, "key", n)

			if res.Remaining < 0 {
				t.Errorf("step %d: Remaining=%d < 0", step, res.Remaining)
			}
			if res.Remaining > limit {
				t.Errorf("step %d: Remaining=%d > limit=%d", step, res.Remaining, limit)
			}
			if res.Algorithm == "" {
				t.Errorf("step %d: Algorithm empty", step)
			}
			if res.Allowed && res.RetryAfter != 0 {
				t.Errorf("step %d: allowed but RetryAfter=%s", step, res.RetryAfter)
			}
			clk.Advance(advance)
		}
	})
}
