package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/loadshed"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/retry"
)

// TestLoadShed_SortsToAdmissionPosition verifies the LoadShed stage is the
// outermost (admission) stage regardless of builder call order.
func TestLoadShed_SortsToAdmissionPosition(t *testing.T) {
	s := loadshed.New(loadshed.Config{})
	p := New().
		Retry(&retry.Policy{MaxAttempts: 1}).
		RateLimit(nil, nil).
		LoadShed(s).
		Build()

	want := []stageKind{kindLoadShed, kindRateLimit, kindRetry}
	if len(p.kinds) != len(want) {
		t.Fatalf("expected %d stages, got %d (%v)", len(want), len(p.kinds), p.kinds)
	}
	for i := range want {
		if p.kinds[i] != want[i] {
			t.Fatalf("stage %d: got kind %d, want %d (full: %v)", i, p.kinds[i], want[i], p.kinds)
		}
	}
}

// TestLoadShed_ShedsWithErrLoadShed drives the shedder into its dropping state
// via a manual clock, then confirms a low-priority Execute is shed with
// ErrLoadShed while the downstream fn is never invoked.
func TestLoadShed_ShedsWithErrLoadShed(t *testing.T) {
	clk := clock.NewManualClock(time.Unix(0, 0))
	target := 5 * time.Millisecond
	interval := 100 * time.Millisecond
	s := loadshed.New(loadshed.Config{Target: target, Interval: interval, PriorityStep: 2}, loadshed.WithClock(clk))

	p := New().LoadShed(s).Build()

	// Drive the shedder into the dropping state by executing over-target work
	// past the interval. The fn advances the clock (its sojourn) each call.
	slow := func(ctx context.Context) error {
		clk.Advance(target * 2)
		return nil
	}
	for i := 0; i < 30 && !s.Dropping(); i++ {
		clk.Advance(interval / 4)
		_ = p.Execute(context.Background(), slow)
	}
	if !s.Dropping() {
		t.Fatal("setup: shedder never entered dropping state")
	}

	// A low-priority request must now be shed with ErrLoadShed, and the guarded
	// fn must not run.
	lowCtx := loadshed.WithPriority(context.Background(), loadshed.PriorityLow)
	ran := false
	err := p.Execute(lowCtx, func(ctx context.Context) error {
		ran = true
		return nil
	})
	if !errors.Is(err, ErrLoadShed) {
		t.Fatalf("expected ErrLoadShed, got %v", err)
	}
	if ran {
		t.Fatal("downstream fn ran despite being shed")
	}

	// A critical-priority request must still pass through.
	critCtx := loadshed.WithPriority(context.Background(), loadshed.PriorityCritical)
	critRan := false
	if err := p.Execute(critCtx, func(ctx context.Context) error {
		critRan = true
		clk.Advance(target * 2)
		return nil
	}); err != nil {
		t.Fatalf("critical request unexpectedly failed: %v", err)
	}
	if !critRan {
		t.Fatal("critical request was shed while dropping (priority not honoured)")
	}
}

// TestLoadShed_AdmitsWhenHealthy confirms a healthy shedder passes work through.
func TestLoadShed_AdmitsWhenHealthy(t *testing.T) {
	s := loadshed.New(loadshed.Config{})
	p := New().LoadShed(s).Build()

	ran := false
	if err := p.Execute(context.Background(), func(ctx context.Context) error {
		ran = true
		return nil
	}); err != nil {
		t.Fatalf("healthy execute failed: %v", err)
	}
	if !ran {
		t.Fatal("healthy request was not admitted")
	}
}
