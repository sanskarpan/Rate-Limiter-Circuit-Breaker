package composite_test

import (
	"context"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/composite"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
)

// FuzzComposite drives a two-limiter composite (AND or OR) over fuzzed
// per-limiter capacities and request cost, asserting invariants:
//   - never panics on any input,
//   - Allowed implies Remaining >= 0,
//   - a denied AllowN consumes nothing from the composite as a whole (checked
//     via Peek: the most-restrictive Remaining does not drop on a deny),
//   - Algorithm is always set.
//
// Both limiters use a ManualClock frozen at t=0 so no refill occurs during the
// run and decisions are monotonic within the fixed clock.
func FuzzComposite(f *testing.F) {
	f.Add(10.0, 5.0, 1, false)
	f.Add(3.0, 100.0, 2, true)
	f.Add(1.0, 1.0, 1, false)
	f.Add(50.0, 20.0, 3, true)

	f.Fuzz(func(t *testing.T, cap1, cap2 float64, n int, useOR bool) {
		if cap1 < 1 || cap1 > 10_000 {
			return
		}
		if cap2 < 1 || cap2 > 10_000 {
			return
		}
		if n < 1 || n > 100 {
			return
		}

		clk := clock.NewManualClock(time.Unix(0, 0))
		// Frozen clock: refillRate is irrelevant to refill because the clock never
		// advances, so capacity is the whole budget for the run.
		tb1 := tokenbucket.New(cap1, cap1, tokenbucket.WithClock(clk))
		tb2 := tokenbucket.New(cap2, cap2, tokenbucket.WithClock(clk))

		mode := composite.AND
		if useOR {
			mode = composite.OR
		}
		c := composite.New(mode, []ratelimit.Limiter{tb1, tb2}, composite.WithClock(clk))
		defer c.Close() //nolint:errcheck

		ctx := context.Background()

		for step := 0; step < 8; step++ {
			before := c.Peek(ctx, "key").Remaining
			res := c.AllowN(ctx, "key", n)

			if res.Remaining < 0 {
				t.Errorf("step %d: Remaining=%d < 0", step, res.Remaining)
			}
			if res.Algorithm == "" {
				t.Errorf("step %d: Algorithm empty", step)
			}
			if res.Allowed && res.RetryAfter != 0 {
				t.Errorf("step %d: allowed but RetryAfter=%s", step, res.RetryAfter)
			}
			after := c.Peek(ctx, "key").Remaining
			// In AND mode a denied request must not consume from any limiter, so the
			// composite's most-restrictive remaining cannot fall on a deny.
			if mode == composite.AND && !res.Allowed && after < before {
				t.Errorf("step %d: AND deny consumed capacity: remaining %d -> %d", step, before, after)
			}
		}
	})
}
