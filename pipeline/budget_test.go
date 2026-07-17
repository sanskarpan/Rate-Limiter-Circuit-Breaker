package pipeline_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/pipeline"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/retry"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/retry/backoff"
)

// TestPipeline_RetryWithBudget verifies the pipeline retry stage honours a
// shared budget: a storm of failing requests stops retrying once the budget is
// spent, and the caller's policy is not mutated.
func TestPipeline_RetryWithBudget(t *testing.T) {
	errBoom := errors.New("boom")
	clk := clock.NewManualClock(time.Unix(0, 0))
	budget := retry.NewBudget(
		retry.BudgetConfig{Ratio: 0, MinPerSecond: 0, Burst: 3},
		retry.WithBudgetClock(clk),
	)

	policy := &retry.Policy{MaxAttempts: 3, Backoff: backoff.Constant(0)}
	p := pipeline.New().RetryWithBudget(policy, budget).Build()

	// The caller's policy must not have been mutated.
	if policy.Budget != nil {
		t.Fatal("RetryWithBudget mutated the caller's policy (Budget set)")
	}

	const storm = 10
	var calls int
	for i := 0; i < storm; i++ {
		_ = p.Execute(context.Background(), func(_ context.Context) error {
			calls++
			return errBoom
		})
	}

	// storm first attempts + 3 budgeted retries.
	want := storm + 3
	if calls != want {
		t.Fatalf("budgeted pipeline made %d calls, want %d", calls, want)
	}
}

// TestPipeline_RetryWithBudget_NilBudget verifies a nil budget behaves like the
// plain Retry stage (retries MaxAttempts every time).
func TestPipeline_RetryWithBudget_NilBudget(t *testing.T) {
	errBoom := errors.New("boom")
	policy := &retry.Policy{MaxAttempts: 3, Backoff: backoff.Constant(0)}
	p := pipeline.New().RetryWithBudget(policy, nil).Build()

	var calls int
	_ = p.Execute(context.Background(), func(_ context.Context) error {
		calls++
		return errBoom
	})
	if calls != 3 {
		t.Fatalf("nil-budget pipeline made %d calls, want 3 (MaxAttempts)", calls)
	}
}

// TestPipeline_Retry_PolicyWithBudget verifies the documented alternative:
// passing a budget-configured Policy straight to Retry works too, sharing the
// budget across pipelines.
func TestPipeline_Retry_PolicyWithBudget(t *testing.T) {
	errBoom := errors.New("boom")
	clk := clock.NewManualClock(time.Unix(0, 0))
	budget := retry.NewBudget(
		retry.BudgetConfig{Ratio: 0, MinPerSecond: 0, Burst: 2},
		retry.WithBudgetClock(clk),
	)
	policy := retry.New(
		retry.WithMaxAttempts(4),
		retry.WithBackoff(backoff.Constant(0)),
		retry.WithBudget(budget),
	)

	// Two independent pipelines sharing the same budget.
	p1 := pipeline.New().Retry(policy).Build()
	p2 := pipeline.New().Retry(policy).Build()

	var calls int
	fn := func(_ context.Context) error { calls++; return errBoom }
	_ = p1.Execute(context.Background(), fn)
	_ = p2.Execute(context.Background(), fn)
	_ = p1.Execute(context.Background(), fn)

	// 3 first attempts + 2 shared budgeted retries = 5.
	if calls != 5 {
		t.Fatalf("shared-budget pipelines made %d calls, want 5", calls)
	}
}
