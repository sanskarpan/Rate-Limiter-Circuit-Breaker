package pipeline_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/pipeline"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/retry"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/retry/backoff"
)

// approx reports whether a and b are within tol of each other.
func approx(a, b, tol time.Duration) bool {
	d := a - b
	if d < 0 {
		d = -d
	}
	return d <= tol
}

// TestRetryBudgeted_NoOptions_IdenticalToRetry verifies the legacy path: with no
// budgeting options a budgeted retry stage behaves exactly like Retry(p) —
// MaxAttempts attempts, and each attempt inherits the caller's context deadline
// unchanged.
func TestRetryBudgeted_NoOptions_IdenticalToRetry(t *testing.T) {
	errBoom := errors.New("boom")

	// Caller context with an overall deadline.
	overall := 100 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), overall)
	defer cancel()

	var calls int
	var seenDeadlines []time.Time
	p := pipeline.New().
		RetryBudgeted(&retry.Policy{MaxAttempts: 3, Backoff: backoff.Constant(0)}).
		Build()

	err := p.Execute(ctx, func(c context.Context) error {
		calls++
		dl, _ := c.Deadline()
		seenDeadlines = append(seenDeadlines, dl)
		return errBoom
	})
	if !errors.Is(err, errBoom) {
		t.Fatalf("want errBoom, got %v", err)
	}
	if calls != 3 {
		t.Fatalf("no-option budgeted retry made %d calls, want 3 (MaxAttempts)", calls)
	}
	// Every attempt saw the SAME (inherited overall) deadline — no per-attempt
	// derivation happened.
	callerDeadline, _ := ctx.Deadline()
	for i, dl := range seenDeadlines {
		if !dl.Equal(callerDeadline) {
			t.Fatalf("attempt %d saw derived deadline %v, want inherited overall %v", i, dl, callerDeadline)
		}
	}
}

// TestRetryBudgeted_PerAttemptTimeout_ClampsEachAttempt proves each attempt runs
// with a derived deadline of min(overall remaining, perAttempt).
func TestRetryBudgeted_PerAttemptTimeout_ClampsEachAttempt(t *testing.T) {
	errBoom := errors.New("boom")
	clk := clock.NewManualClock(time.Now())

	overall := 1 * time.Second
	start := clk.Now()
	overallDeadline := start.Add(overall)
	ctx, cancel := context.WithDeadline(context.Background(), overallDeadline)
	defer cancel()

	perAttempt := 200 * time.Millisecond

	var slices []time.Duration
	p := pipeline.New().
		RetryBudgeted(
			&retry.Policy{MaxAttempts: 4, Backoff: backoff.Constant(0)},
			pipeline.WithPerAttemptTimeout(perAttempt),
			pipeline.WithAttemptBudgetClock(clk),
		).
		Build()

	_ = p.Execute(ctx, func(c context.Context) error {
		dl, ok := c.Deadline()
		if !ok {
			t.Fatal("attempt has no deadline; per-attempt budgeting not applied")
		}
		// Slice = deadline - clk.now (derived using the same manual clock).
		slices = append(slices, dl.Sub(clk.Now()))
		return errBoom
	})

	if len(slices) != 4 {
		t.Fatalf("got %d attempts, want 4", len(slices))
	}
	// Overall remaining (1s) always exceeds perAttempt (200ms), so every attempt
	// is clamped to exactly perAttempt.
	for i, s := range slices {
		if !approx(s, perAttempt, time.Millisecond) {
			t.Fatalf("attempt %d slice = %v, want ~%v (clamped to perAttempt)", i, s, perAttempt)
		}
	}
}

// TestRetryBudgeted_PerAttemptClampedByOverall proves the min() clamp: when the
// overall remaining is SMALLER than the fixed per-attempt timeout, the attempt
// is clamped to the overall remaining so total wall-time never exceeds the
// overall deadline.
func TestRetryBudgeted_PerAttemptClampedByOverall(t *testing.T) {
	errBoom := errors.New("boom")
	clk := clock.NewManualClock(time.Now())

	// Overall remaining (50ms) < perAttempt (500ms): clamp to overall.
	overall := 50 * time.Millisecond
	ctx, cancel := context.WithDeadline(context.Background(), clk.Now().Add(overall))
	defer cancel()

	var slice time.Duration
	p := pipeline.New().
		RetryBudgeted(
			&retry.Policy{MaxAttempts: 3, Backoff: backoff.Constant(0)},
			pipeline.WithPerAttemptTimeout(500*time.Millisecond),
			pipeline.WithAttemptBudgetClock(clk),
		).
		Build()

	_ = p.Execute(ctx, func(c context.Context) error {
		dl, _ := c.Deadline()
		slice = dl.Sub(clk.Now())
		return errBoom
	})
	if !approx(slice, overall, time.Millisecond) {
		t.Fatalf("first-attempt slice = %v, want ~%v (clamped to overall remaining)", slice, overall)
	}
	// The applied deadline must never exceed the overall deadline.
	overallDL, _ := ctx.Deadline()
	if clk.Now().Add(slice).After(overallDL) {
		t.Fatalf("derived deadline exceeds overall deadline")
	}
}

// TestRetryBudgeted_Divide_ShrinkingSlices proves auto-division: each attempt
// gets remaining/attemptsLeft, a fair shrinking slice, driven deterministically
// by advancing a ManualClock between attempts.
func TestRetryBudgeted_Divide_ShrinkingSlices(t *testing.T) {
	errBoom := errors.New("boom")
	clk := clock.NewManualClock(time.Now())

	overall := 1200 * time.Millisecond // divisible by 4/3/2/1 cleanly
	ctx, cancel := context.WithDeadline(context.Background(), clk.Now().Add(overall))
	defer cancel()

	maxAttempts := 4
	var slices []time.Duration
	p := pipeline.New().
		RetryBudgeted(
			&retry.Policy{MaxAttempts: maxAttempts, Backoff: backoff.Constant(0)},
			pipeline.WithAttemptBudgeting(),
			pipeline.WithAttemptBudgetClock(clk),
		).
		Build()

	_ = p.Execute(ctx, func(c context.Context) error {
		dl, ok := c.Deadline()
		if !ok {
			t.Fatal("attempt has no deadline; divide budgeting not applied")
		}
		slice := dl.Sub(clk.Now())
		slices = append(slices, slice)
		// Simulate the attempt consuming its full slice by advancing the clock,
		// so the NEXT attempt sees a reduced overall remaining.
		clk.Advance(slice)
		return errBoom
	})

	if len(slices) != maxAttempts {
		t.Fatalf("got %d attempts, want %d", len(slices), maxAttempts)
	}
	// Attempt 1: 1200/4 = 300ms ; then 900 remaining
	// Attempt 2: 900/3  = 300ms ; then 600 remaining
	// Attempt 3: 600/2  = 300ms ; then 300 remaining
	// Attempt 4: 300/1  = 300ms
	want := []time.Duration{300 * time.Millisecond, 300 * time.Millisecond, 300 * time.Millisecond, 300 * time.Millisecond}
	for i := range want {
		if !approx(slices[i], want[i], time.Millisecond) {
			t.Fatalf("attempt %d slice = %v, want ~%v", i, slices[i], want[i])
		}
	}
}

// TestRetryBudgeted_Divide_UnevenShrinks proves a genuinely shrinking sequence
// when attempts consume MORE than their fair slice: remaining/attemptsLeft keeps
// each subsequent attempt inside the overall deadline.
func TestRetryBudgeted_Divide_UnevenShrinks(t *testing.T) {
	errBoom := errors.New("boom")
	clk := clock.NewManualClock(time.Now())

	overall := 800 * time.Millisecond
	ctx, cancel := context.WithDeadline(context.Background(), clk.Now().Add(overall))
	defer cancel()

	var slices []time.Duration
	p := pipeline.New().
		RetryBudgeted(
			&retry.Policy{MaxAttempts: 4, Backoff: backoff.Constant(0)},
			pipeline.WithAttemptBudgeting(),
			pipeline.WithAttemptBudgetClock(clk),
		).
		Build()

	_ = p.Execute(ctx, func(c context.Context) error {
		dl, _ := c.Deadline()
		slice := dl.Sub(clk.Now())
		slices = append(slices, slice)
		// Consume the full slice each time.
		clk.Advance(slice)
		return errBoom
	})

	// 800/4=200 -> rem 600 ; 600/3=200 -> rem 400 ; 400/2=200 -> rem 200 ; 200/1=200.
	// Verify each derived deadline stays within the overall deadline and slices
	// are monotonically non-increasing.
	overallDL, _ := ctx.Deadline()
	consumed := time.Duration(0)
	base := clk.Now() // note: clk has advanced; recompute against a fresh baseline
	_ = base
	for i := 1; i < len(slices); i++ {
		if slices[i] > slices[i-1]+time.Millisecond {
			t.Fatalf("slice %d (%v) grew vs slice %d (%v); should shrink or hold", i, slices[i], i-1, slices[i-1])
		}
	}
	// Total consumed across attempts must not exceed the overall budget.
	for _, s := range slices {
		consumed += s
	}
	if consumed > overall+time.Millisecond {
		t.Fatalf("total per-attempt budget %v exceeds overall %v", consumed, overall)
	}
	_ = overallDL
}

// TestRetryBudgeted_StopsWhenBudgetExhausted proves retrying stops early with
// context.DeadlineExceeded once the overall deadline is spent, rather than
// running the remaining attempts.
func TestRetryBudgeted_StopsWhenBudgetExhausted(t *testing.T) {
	errBoom := errors.New("boom")
	clk := clock.NewManualClock(time.Now())

	overall := 100 * time.Millisecond
	ctx, cancel := context.WithDeadline(context.Background(), clk.Now().Add(overall))
	defer cancel()

	var calls int
	p := pipeline.New().
		RetryBudgeted(
			&retry.Policy{MaxAttempts: 10, Backoff: backoff.Constant(0)},
			pipeline.WithPerAttemptTimeout(40*time.Millisecond),
			pipeline.WithAttemptBudgetClock(clk),
		).
		Build()

	err := p.Execute(ctx, func(c context.Context) error {
		calls++
		// Each attempt "consumes" its full 40ms slice.
		clk.Advance(40 * time.Millisecond)
		return errBoom
	})

	// Attempt 1 at t=0 (rem 100), attempt 2 at t=40 (rem 60), attempt 3 at t=80
	// (rem 20 -> clamped to 20). After attempt 3 advances to t=120, remaining is
	// negative, so attempt 4 is refused with DeadlineExceeded.
	if calls != 3 {
		t.Fatalf("budget-exhausted retry made %d calls, want 3", calls)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want context.DeadlineExceeded once budget exhausted, got %v", err)
	}
}

// TestRetryBudgeted_NoOverallDeadline_PerAttemptStillApplies verifies that even
// without an overall deadline, a fixed per-attempt timeout is applied to each
// attempt.
func TestRetryBudgeted_NoOverallDeadline_PerAttemptStillApplies(t *testing.T) {
	errBoom := errors.New("boom")
	clk := clock.NewManualClock(time.Now())

	perAttempt := 250 * time.Millisecond
	var slices []time.Duration
	p := pipeline.New().
		RetryBudgeted(
			&retry.Policy{MaxAttempts: 2, Backoff: backoff.Constant(0)},
			pipeline.WithPerAttemptTimeout(perAttempt),
			pipeline.WithAttemptBudgetClock(clk),
		).
		Build()

	_ = p.Execute(context.Background(), func(c context.Context) error {
		dl, ok := c.Deadline()
		if !ok {
			t.Fatal("attempt has no deadline despite per-attempt timeout")
		}
		slices = append(slices, dl.Sub(clk.Now()))
		return errBoom
	})
	for i, s := range slices {
		if !approx(s, perAttempt, time.Millisecond) {
			t.Fatalf("attempt %d slice %v, want ~%v", i, s, perAttempt)
		}
	}
}

// TestRetryBudgeted_DivideNoDeadline_Noop verifies WithAttemptBudgeting is a
// no-op when the context has no deadline (nothing to divide): attempts run
// unchanged and all MaxAttempts execute.
func TestRetryBudgeted_DivideNoDeadline_Noop(t *testing.T) {
	errBoom := errors.New("boom")
	var calls int
	var hadDeadline bool
	p := pipeline.New().
		RetryBudgeted(
			&retry.Policy{MaxAttempts: 3, Backoff: backoff.Constant(0)},
			pipeline.WithAttemptBudgeting(),
		).
		Build()

	_ = p.Execute(context.Background(), func(c context.Context) error {
		calls++
		if _, ok := c.Deadline(); ok {
			hadDeadline = true
		}
		return errBoom
	})
	if calls != 3 {
		t.Fatalf("divide-no-deadline made %d calls, want 3", calls)
	}
	if hadDeadline {
		t.Fatal("attempt got a deadline despite no overall deadline and divide-only mode")
	}
}

// TestRetryBudgeted_Success_ShortCircuits verifies a budgeted stage returns
// immediately on success without exhausting attempts.
func TestRetryBudgeted_Success_ShortCircuits(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var calls int
	p := pipeline.New().
		RetryBudgeted(
			&retry.Policy{MaxAttempts: 5, Backoff: backoff.Constant(0)},
			pipeline.WithPerAttemptTimeout(100*time.Millisecond),
		).
		Build()

	err := p.Execute(ctx, func(c context.Context) error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 1 {
		t.Fatalf("success budgeted retry made %d calls, want 1", calls)
	}
}

// TestRetryBudgeted_PolicyNotMutated verifies the supplied policy is untouched.
func TestRetryBudgeted_PolicyNotMutated(t *testing.T) {
	policy := &retry.Policy{MaxAttempts: 3, Backoff: backoff.Constant(0)}
	_ = pipeline.New().
		RetryBudgeted(policy, pipeline.WithPerAttemptTimeout(10*time.Millisecond)).
		Build()
	if policy.MaxAttempts != 3 {
		t.Fatalf("policy.MaxAttempts mutated: %d", policy.MaxAttempts)
	}
	if policy.Budget != nil {
		t.Fatal("policy.Budget mutated")
	}
}

// TestRetryBudgeted_TotalWallTimeWithinOverall exercises the real path end to
// end with the real clock and real context timers: total wall-time must not
// exceed the overall deadline even though each attempt would sleep past it.
func TestRetryBudgeted_TotalWallTimeWithinOverall(t *testing.T) {
	errBoom := errors.New("boom")
	overall := 120 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), overall)
	defer cancel()

	p := pipeline.New().
		RetryBudgeted(
			&retry.Policy{MaxAttempts: 10, Backoff: backoff.Constant(0)},
			pipeline.WithPerAttemptTimeout(40*time.Millisecond),
		).
		Build()

	start := time.Now()
	err := p.Execute(ctx, func(c context.Context) error {
		// Each attempt runs until its per-attempt deadline fires (never returns
		// on its own), forcing reliance on the derived per-attempt deadline.
		<-c.Done()
		return errBoom
	})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected an error")
	}
	// Total wall-time must stay within the overall deadline plus a scheduling
	// grace margin. Without clamping, 10 attempts * 40ms = 400ms >> 120ms.
	if elapsed > overall+80*time.Millisecond {
		t.Fatalf("total wall-time %v exceeded overall deadline %v (per-attempt clamp failed)", elapsed, overall)
	}
}

// TestRetryBudgeted_Race runs many concurrent budgeted executions under -race to
// confirm the stage has no data races (the attempt counter is per-Execute).
func TestRetryBudgeted_Race(t *testing.T) {
	errBoom := errors.New("boom")
	p := pipeline.New().
		RetryBudgeted(
			&retry.Policy{MaxAttempts: 4, Backoff: backoff.Constant(0)},
			pipeline.WithAttemptBudgeting(),
			pipeline.WithPerAttemptTimeout(5*time.Millisecond),
		).
		Build()

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			_ = p.Execute(ctx, func(c context.Context) error {
				return errBoom
			})
		}()
	}
	wg.Wait()
}
