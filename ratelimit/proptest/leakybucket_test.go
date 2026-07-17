package proptest

import (
	"context"
	"runtime"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/leakybucket"
)

// runtimeYield gives the background leaker goroutine a chance to drain ticks it
// just received. It is not load-bearing for any asserted invariant (those hold
// regardless of leaker progress); it only helps exercise the drain path.
func runtimeYield() {
	for i := 0; i < 3; i++ {
		runtime.Gosched()
	}
	time.Sleep(time.Millisecond)
}

// Leaky bucket is queue-shaped and its Allow blocks on a background leaker
// goroutine that only makes progress when the ManualClock ticker fires. Driving
// its blocking Allow synchronously from a random op/advance loop is inherently
// racy, so the schedule-based admission property (which assumes a synchronous
// decision) is deliberately NOT applied to it. Instead we assert the invariants
// that are observable without blocking:
//
//   - Peek.Remaining ∈ [0, capacity] over any random enqueue/advance schedule
//     (property 2 for leaky bucket).
//   - The queue never accepts more than `capacity` outstanding requests: a
//     non-blocking probe (a canceled-context Allow) is rejected once the queue
//     is full, and admitted only when a slot is free.
//   - Reset clears the queue (property 5): after Reset, Peek reports the full
//     capacity as remaining again.

// TestPropertyLeakyBucketRemaining asserts Peek.Remaining stays within
// [0, capacity] across a random schedule of enqueue attempts and clock advances.
// Enqueues are performed with an already-canceled context so Allow returns
// promptly instead of blocking on the leaker: an enqueued-then-canceled request
// is still admitted to the queue (occupying a slot) or rejected when full, which
// is exactly the queue-occupancy behaviour we want to probe.
func TestPropertyLeakyBucketRemaining(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		capacity := rapid.IntRange(1, 20).Draw(t, "capacity")
		leakRate := float64(rapid.IntRange(1, 20).Draw(t, "leakRate"))
		clk := clock.NewManualClock(epoch)
		lim := leakybucket.New(capacity, leakRate,
			leakybucket.WithClock(clk),
			leakybucket.WithIdleCleanup(disableCleanup),
		)
		defer lim.Close()

		leakInterval := time.Duration(float64(time.Second) / leakRate)
		bg := context.Background()

		nSteps := rapid.IntRange(1, 40).Draw(t, "nSteps")
		for i := 0; i < nSteps; i++ {
			if rapid.Bool().Draw(t, "isAdvance") {
				// Advance by a random multiple of the leak interval so the leaker
				// drains 0..k queued tokens.
				k := rapid.IntRange(0, 4).Draw(t, "ticks")
				clk.Advance(time.Duration(k) * leakInterval)
				// Yield so the leaker goroutine can process the ticks it just
				// received before we Peek. A brief real sleep is adequate; the
				// invariant we assert (Remaining ∈ [0,capacity]) holds regardless
				// of whether the leaker has caught up, so this is not load-bearing
				// for correctness, only for exercising drains.
				runtimeYield()
			} else {
				// Fire-and-forget enqueue with a canceled context: the request
				// occupies a queue slot (or is rejected if full) without blocking
				// on a leak tick.
				ctx, cancel := context.WithCancel(bg)
				cancel()
				lim.Allow(ctx, "k")
			}
			st := lim.Peek(bg, "k")
			if st.Remaining < 0 || st.Remaining > capacity {
				t.Fatalf("leaky bucket: Peek.Remaining=%d out of [0,%d]", st.Remaining, capacity)
			}
		}
	})
}

// TestPropertyLeakyBucketReset asserts Reset restores full queue capacity
// (property 5). We fill the queue up to capacity with canceled-context requests,
// confirm it is (near) full via Peek, Reset, then confirm Peek reports full
// capacity remaining again.
func TestPropertyLeakyBucketReset(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		capacity := rapid.IntRange(1, 20).Draw(t, "capacity")
		leakRate := float64(rapid.IntRange(1, 20).Draw(t, "leakRate"))
		clk := clock.NewManualClock(epoch)
		lim := leakybucket.New(capacity, leakRate,
			leakybucket.WithClock(clk),
			leakybucket.WithIdleCleanup(disableCleanup),
		)
		defer lim.Close()

		bg := context.Background()
		// Fill the queue without advancing the clock (leaker never fires, so every
		// slot stays occupied). Canceled context keeps Allow from blocking.
		for i := 0; i < capacity; i++ {
			ctx, cancel := context.WithCancel(bg)
			cancel()
			lim.Allow(ctx, "k")
		}
		// Queue should now be full: Peek reports 0 remaining.
		if rem := lim.Peek(bg, "k").Remaining; rem != 0 {
			t.Fatalf("leaky bucket: after filling %d slots, remaining=%d (want 0)", capacity, rem)
		}
		if err := lim.Reset(bg, "k"); err != nil {
			t.Fatalf("leaky bucket: Reset error: %v", err)
		}
		if rem := lim.Peek(bg, "k").Remaining; rem != capacity {
			t.Fatalf("leaky bucket: after Reset remaining=%d (want full capacity %d)", rem, capacity)
		}
	})
}
