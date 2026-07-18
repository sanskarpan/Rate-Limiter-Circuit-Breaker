package leakybucket_test

import (
	"context"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/leakybucket"
)

// FuzzLeakyBucket verifies invariants of the leaky bucket over fuzzed
// capacity/leak-rate/cost inputs:
//   - never panics on any input,
//   - Allowed implies Remaining >= 0 and <= capacity,
//   - AllowN(n > capacity) is always denied,
//   - Algorithm is always set.
//
// A real clock plus a high leak rate is used (as the package's own tests do) so
// the background leaker actually drains queued tokens; every call is bounded by
// a short context timeout so no input can hang the fuzzer.
func FuzzLeakyBucket(f *testing.F) {
	f.Add(10, 1000.0, 1)
	f.Add(1, 500.0, 1)
	f.Add(100, 10000.0, 5)
	f.Add(5, 100.0, 3)

	f.Fuzz(func(t *testing.T, capacity int, leakRate float64, n int) {
		if capacity < 1 || capacity > 1000 {
			return
		}
		// Keep the leak rate high and finite so queued tokens drain promptly and
		// deterministically within the per-call timeout.
		if leakRate < 100 || leakRate > 1_000_000 {
			return
		}
		if n < 1 || n > capacity*2 {
			return
		}

		lb := leakybucket.New(capacity, leakRate, leakybucket.WithClock(clock.RealClock{}))
		defer lb.Close() //nolint:errcheck

		for step := 0; step < 6; step++ {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			res := lb.AllowN(ctx, "key", n)
			cancel()

			if res.Remaining < 0 {
				t.Errorf("step %d: Remaining=%d < 0", step, res.Remaining)
			}
			if res.Remaining > capacity {
				t.Errorf("step %d: Remaining=%d > capacity=%d", step, res.Remaining, capacity)
			}
			if res.Algorithm == "" {
				t.Errorf("step %d: Algorithm empty", step)
			}
			if n > capacity && res.Allowed {
				t.Errorf("step %d: AllowN(%d) with capacity=%d should be denied", step, n, capacity)
			}
		}
	})
}
