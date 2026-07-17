package gcra_test

import (
	"context"
	"testing"
	"time"

	"github.com/sanskarpan/resilience/internal/clock"
	"github.com/sanskarpan/resilience/ratelimit/gcra"
)

func FuzzGCRA(f *testing.F) {
	// Seed corpus
	f.Add(10, 5, int64(time.Second), int64(0))
	f.Add(1, 1, int64(time.Minute), int64(100*time.Millisecond))
	f.Add(100, 10, int64(time.Second), int64(50*time.Millisecond))

	f.Fuzz(func(t *testing.T, limit, burst int, windowNs, advanceNs int64) {
		// Sanitize inputs
		if limit <= 0 || limit > 10000 {
			return
		}
		if burst <= 0 || burst > limit {
			return
		}
		if windowNs <= 0 || windowNs > int64(10*time.Minute) {
			return
		}
		if advanceNs < 0 {
			advanceNs = -advanceNs
		}

		// Bound the per-step advance so it stays a sane fraction of the window,
		// otherwise a single huge advance would trivially refill the limiter every
		// step and we'd never observe a deny. advanceNs may legitimately be 0.
		if advanceNs > int64(10*time.Minute) {
			advanceNs = advanceNs % (int64(10*time.Minute) + 1)
		}
		advance := time.Duration(advanceNs)

		window := time.Duration(windowNs)
		// Inject a ManualClock so we can actually advance time between requests
		// (L-9). Previously advanceNs was computed but never applied, so the
		// fuzzer only ever exercised the first-request-always-allowed path.
		clk := clock.NewManualClock(time.Unix(0, 0))
		g := gcra.New(limit, burst, window, gcra.WithClock(clk))
		defer g.Close()

		ctx := context.Background()

		// Drive several requests, advancing the clock between them so we explore
		// the allow → deny → recover transitions rather than just the first call.
		for step := 0; step < 8; step++ {
			// Invariant 1: Never panics on any input
			result := g.Allow(ctx, "key")

			// Invariant 2: Remaining always in [0, burst]
			if result.Remaining < 0 || result.Remaining > burst {
				t.Errorf("step %d: invariant violated: remaining=%d not in [0, %d]", step, result.Remaining, burst)
			}

			// Invariant 3: If Allowed=true, RetryAfter == 0
			if result.Allowed && result.RetryAfter != 0 {
				t.Errorf("step %d: invariant violated: allowed=true but RetryAfter=%s", step, result.RetryAfter)
			}

			// Invariant 4: If Allowed=false, RetryAfter > 0
			if !result.Allowed && result.RetryAfter <= 0 {
				t.Errorf("step %d: invariant violated: allowed=false but RetryAfter=%s", step, result.RetryAfter)
			}

			// Invariant 5: Algorithm field is always set
			if result.Algorithm == "" {
				t.Errorf("step %d: Algorithm field must not be empty", step)
			}

			clk.Advance(advance)
		}
	})
}
