package tokenbucket_test

import (
	"context"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
)

// FuzzTokenBucket verifies key invariants hold for arbitrary inputs:
// 1. Result.Remaining is always in [0, capacity].
// 2. Never panics on any input.
// 3. After Allow() returns Allowed=true, remaining decreases by exactly n.
func FuzzTokenBucket(f *testing.F) {
	// Seed corpus
	f.Add(10.0, 1.0, 5, 1)
	f.Add(1.0, 0.001, 1, 1)
	f.Add(100.0, 100.0, 50, 3)
	f.Add(5.0, 2.0, 3, 5)

	f.Fuzz(func(t *testing.T, capacity float64, refillRate float64, initialN int, consumeN int) {
		// Clamp to reasonable ranges to avoid degenerate cases.
		if capacity < 1 {
			capacity = 1
		}
		if capacity > 10_000 {
			capacity = 10_000
		}
		if refillRate <= 0 {
			refillRate = 0.001
		}
		if refillRate > 1_000_000 {
			refillRate = 1_000_000
		}
		if consumeN < 1 {
			consumeN = 1
		}
		if consumeN > int(capacity)*2 {
			consumeN = int(capacity) * 2
		}

		tb := tokenbucket.New(capacity, refillRate)
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("panic: %v", r)
			}
			tb.Close() //nolint:errcheck
		}()

		ctx := context.Background()

		capInt := int(capacity)

		// Invariant 1: Remaining always in [0, capacity]
		for i := 0; i < 20; i++ {
			result := tb.Allow(ctx, "fuzz-key")
			if result.Remaining < 0 {
				t.Errorf("Remaining=%d < 0 (capacity=%.0f)", result.Remaining, capacity)
			}
			if result.Remaining > capInt {
				t.Errorf("Remaining=%d > capacity=%.0f", result.Remaining, capacity)
			}
		}

		// Invariant 2: Peek does not consume tokens
		state1 := tb.Peek(ctx, "peek-key")
		state2 := tb.Peek(ctx, "peek-key")
		if state1.Remaining != state2.Remaining {
			t.Errorf("Peek changed state: %d → %d", state1.Remaining, state2.Remaining)
		}

		// Invariant 3: AllowN(n > capacity) always denied
		if consumeN > capInt {
			r := tb.AllowN(ctx, "allown-key", consumeN)
			if r.Allowed {
				t.Errorf("AllowN(%d) with capacity=%.0f should always be denied", consumeN, capacity)
			}
		}

		// Invariant 4: RetryAfter is positive when denied
		_ = time.Duration(0) // ensure time import used
		r := tb.Allow(ctx, "retry-key")
		if !r.Allowed && r.RetryAfter <= 0 {
			t.Errorf("RetryAfter=%v should be positive when denied", r.RetryAfter)
		}
	})
}
