package ratelimit_test

import (
	"context"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/fixedwindow"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/gcra"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/leakybucket"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/slidingwindow"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
)

// Compile-time assertions that representative concrete limiters satisfy the
// read-only Peeker view. These fail to compile — not at runtime —
// if a limiter stops implementing Peek or the interface drifts.
var (
	_ ratelimit.Peeker = (*tokenbucket.TokenBucket)(nil)
	_ ratelimit.Peeker = (*gcra.GCRA)(nil)
	_ ratelimit.Peeker = (*fixedwindow.FixedWindowCounter)(nil)
	_ ratelimit.Peeker = (*slidingwindow.SlidingWindowLog)(nil)
	_ ratelimit.Peeker = (*leakybucket.LeakyBucket)(nil)

	// The full Limiter interface must remain assignable to the read-only Peeker view.
	_ ratelimit.Peeker = ratelimit.Limiter(nil)
)

// TestPeekerObserveWithoutMutation verifies that using a limiter purely through
// the read-only Peeker view does not consume tokens: repeated Peek calls leave
// Remaining unchanged, proving the segregated interface is side-effect free.
func TestPeekerObserveWithoutMutation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		newPeek func() ratelimit.Peeker
	}{
		{
			name:    "tokenbucket",
			newPeek: func() ratelimit.Peeker { return tokenbucket.New(10, 1) },
		},
		{
			name:    "gcra",
			newPeek: func() ratelimit.Peeker { return gcra.New(10, 10, time.Second) },
		},
		{
			name:    "fixedwindow",
			newPeek: func() ratelimit.Peeker { return fixedwindow.New(10, time.Second) },
		},
		{
			name:    "slidingwindow",
			newPeek: func() ratelimit.Peeker { return slidingwindow.NewLog(10, time.Second) },
		},
		{
			name:    "leakybucket",
			newPeek: func() ratelimit.Peeker { return leakybucket.New(10, 1) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := tt.newPeek()
			ctx := context.Background()

			first := p.Peek(ctx, "k")
			for i := 0; i < 5; i++ {
				got := p.Peek(ctx, "k")
				if got.Remaining != first.Remaining {
					t.Fatalf("Peek mutated state: Remaining %d -> %d on call %d",
						first.Remaining, got.Remaining, i)
				}
			}
		})
	}
}

// TestPeekerAcceptsLimiter documents the intended usage: a consumer function
// that only observes accepts a Peeker, and any Limiter can be passed to it.
func TestPeekerAcceptsLimiter(t *testing.T) {
	t.Parallel()

	observe := func(o ratelimit.Peeker) ratelimit.State {
		return o.Peek(context.Background(), "k")
	}

	var lim ratelimit.Limiter = tokenbucket.New(10, 1)
	if st := observe(lim); st.Algorithm == "" {
		t.Fatal("expected a non-empty Algorithm from Peek via Peeker")
	}
}
