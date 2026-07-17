package retry_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/retry"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/retry/backoff"
)

// TestDo_BudgetBoundsRetryStorm verifies that a persistently-failing function
// under a shared budget stops retrying once the budget is exhausted, bounding
// the total call count — whereas the same policy without a budget retries
// MaxAttempts every single time.
func TestDo_BudgetBoundsRetryStorm(t *testing.T) {
	errBoom := errors.New("boom")

	// Budget: no ratio, no floor refill, burst of 5 retry tokens. Over a storm
	// of many top-level calls the total number of *extra* attempts is capped at
	// 5 regardless of how many calls fail.
	clk := clock.NewManualClock(time.Unix(0, 0))
	budget := retry.NewBudget(
		retry.BudgetConfig{Ratio: 0, MinPerSecond: 0, Burst: 5},
		retry.WithBudgetClock(clk),
	)

	p := retry.New(
		retry.WithMaxAttempts(3),
		retry.WithBackoff(backoff.Constant(0)),
		retry.WithBudget(budget),
	)

	const storm = 20
	var calls int
	for i := 0; i < storm; i++ {
		_ = p.Do(context.Background(), func(_ context.Context) error {
			calls++
			return errBoom
		})
	}

	// Each top-level call makes at least 1 call (the first attempt is never
	// budget-gated). Extra attempts are capped by the budget at 5 total.
	// => total calls == storm (first attempts) + 5 (budgeted retries).
	wantMax := storm + 5
	if calls != wantMax {
		t.Fatalf("budgeted storm made %d calls, want exactly %d (storm firsts + 5 budgeted retries)", calls, wantMax)
	}

	// Contrast: no budget => every call retries the full MaxAttempts (3 each).
	var callsNoBudget int
	pNoBudget := &retry.Policy{MaxAttempts: 3, Backoff: backoff.Constant(0)}
	for i := 0; i < storm; i++ {
		_ = pNoBudget.Do(context.Background(), func(_ context.Context) error {
			callsNoBudget++
			return errBoom
		})
	}
	if callsNoBudget != storm*3 {
		t.Fatalf("unbudgeted storm made %d calls, want %d (MaxAttempts each)", callsNoBudget, storm*3)
	}

	if calls >= callsNoBudget {
		t.Fatalf("budget did not reduce retries: budgeted=%d unbudgeted=%d", calls, callsNoBudget)
	}
}

// TestDo_BudgetDeniesWithoutConsuming verifies the budget is not consumed when
// it denies: the last error is returned and the (already-empty) bucket is not
// driven negative, so a later refill still grants exactly the refilled amount.
func TestDo_BudgetDeniesWithoutConsuming(t *testing.T) {
	errBoom := errors.New("boom")
	clk := clock.NewManualClock(time.Unix(0, 0))
	budget := retry.NewBudget(
		retry.BudgetConfig{Ratio: 0, MinPerSecond: 1, Burst: 1},
		retry.WithBudgetClock(clk),
	)
	p := retry.New(
		retry.WithMaxAttempts(5),
		retry.WithBackoff(backoff.Constant(0)),
		retry.WithBudget(budget),
	)

	// First call: burst gives exactly 1 retry, so 2 calls (1 first + 1 retry).
	var calls int
	err := p.Do(context.Background(), func(_ context.Context) error {
		calls++
		return errBoom
	})
	if !errors.Is(err, errBoom) {
		t.Fatalf("got err %v, want boom", err)
	}
	if calls != 2 {
		t.Fatalf("first budgeted call made %d calls, want 2", calls)
	}

	// Second call immediately: budget empty (no time advanced), so no retry.
	calls = 0
	_ = p.Do(context.Background(), func(_ context.Context) error {
		calls++
		return errBoom
	})
	if calls != 1 {
		t.Fatalf("exhausted-budget call made %d calls, want 1 (no retry)", calls)
	}

	// Advance 1s at MinPerSecond=1 => exactly 1 token. Next call gets 1 retry.
	clk.Advance(1 * time.Second)
	calls = 0
	_ = p.Do(context.Background(), func(_ context.Context) error {
		calls++
		return errBoom
	})
	if calls != 2 {
		t.Fatalf("refilled-budget call made %d calls, want 2 (exactly 1 refilled retry)", calls)
	}
}

// TestDo_NoBudgetUnchanged ensures behaviour with no budget is exactly the old
// behaviour: fn is called MaxAttempts times on persistent failure.
func TestDo_NoBudgetUnchanged(t *testing.T) {
	errBoom := errors.New("boom")
	p := &retry.Policy{MaxAttempts: 4, Backoff: backoff.Constant(0)}
	var calls int
	err := p.Do(context.Background(), func(_ context.Context) error {
		calls++
		return errBoom
	})
	if !errors.Is(err, errBoom) {
		t.Fatalf("got %v, want boom", err)
	}
	if calls != 4 {
		t.Fatalf("no-budget policy made %d calls, want 4 (MaxAttempts)", calls)
	}
}

// ExampleBudget demonstrates a shared retry budget that guards against a retry
// storm: once the budget is spent, further retries are denied.
func ExampleBudget() {
	// A budget that permits a burst of 2 retries with no refill (for a
	// deterministic example).
	budget := retry.NewBudget(retry.BudgetConfig{Ratio: 0.1, MinPerSecond: 0, Burst: 2})

	p := retry.New(
		retry.WithMaxAttempts(3),
		retry.WithBackoff(backoff.Constant(0)),
		retry.WithBudget(budget),
	)

	failing := func(_ context.Context) error { return errors.New("dependency down") }

	total := 0
	for i := 0; i < 4; i++ {
		attempts := 0
		_ = p.Do(context.Background(), func(ctx context.Context) error {
			attempts++
			return failing(ctx)
		})
		total += attempts
	}

	// 4 top-level calls: each makes 1 first attempt (4), plus 2 budgeted retries
	// total before the budget is exhausted => 6 calls overall.
	fmt.Printf("total calls across 4 storming requests: %d\n", total)
	// Output:
	// total calls across 4 storming requests: 6
}
