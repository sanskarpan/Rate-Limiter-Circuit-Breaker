package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/retry"
)

// TestBuild_EnforcesCanonicalOrder is the regression test for M-9: the package
// documents a "fixed, non-configurable" stage order, but Build previously
// preserved builder call order. Build must now sort stages into the canonical
// sequence regardless of the order the methods were called in.
func TestBuild_EnforcesCanonicalOrder(t *testing.T) {
	// Add stages in deliberately reversed / scrambled order.
	p := New().
		Retry(&retry.Policy{MaxAttempts: 1}).
		Timeout(5*time.Millisecond).
		CircuitBreaker(nil). // nil is fine; the stage fn isn't executed here
		Bulkhead(1, 0).
		RateLimit(nil, nil).
		Build()

	want := []stageKind{kindRateLimit, kindBulkhead, kindTimeout, kindCircuitBreaker, kindRetry}
	if len(p.kinds) != len(want) {
		t.Fatalf("expected %d stages, got %d", len(want), len(p.kinds))
	}
	for i := range want {
		if p.kinds[i] != want[i] {
			t.Fatalf("stage %d: canonical order violated: got kind %d, want %d (full: %v)", i, p.kinds[i], want[i], p.kinds)
		}
	}
}

// TestBuild_RetryIsInnermostRelativeToTimeout verifies the observable
// consequence: added as Retry-then-Timeout, the timeout must still wrap the
// retry (retry innermost), so all attempts share one timeout budget rather than
// each attempt getting a fresh one.
func TestBuild_RetryIsInnermostRelativeToTimeout(t *testing.T) {
	var calls int
	p := New().
		Retry(&retry.Policy{MaxAttempts: 5}).
		Timeout(20 * time.Millisecond).
		Build()

	err := p.Execute(context.Background(), func(ctx context.Context) error {
		calls++
		// Respect the shared deadline: if it's already blown, return promptly.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(15 * time.Millisecond):
			return context.DeadlineExceeded // force a retry
		}
	})
	if err == nil {
		t.Fatal("expected an error")
	}
	// With one shared 20ms timeout wrapping the retry, the first attempt burns
	// ~15ms and the shared deadline expires almost immediately after, so we
	// cannot fit all 5 full attempts. If retry were outermost (bug), each of the
	// 5 attempts would get a fresh 20ms budget and all 5 would run.
	if calls >= 5 {
		t.Fatalf("timeout is not wrapping retry: fn ran %d times (retry appears outermost)", calls)
	}
}

// TestBuild_CustomStagesRunInnermostInOrder verifies Use stages sort last and
// keep insertion order.
func TestBuild_CustomStagesRunInnermostInOrder(t *testing.T) {
	p := New().
		Use(func(ctx context.Context, fn func(context.Context) error) error { return fn(ctx) }).
		RateLimit(nil, nil).
		Use(func(ctx context.Context, fn func(context.Context) error) error { return fn(ctx) }).
		Build()
	want := []stageKind{kindRateLimit, kindCustom, kindCustom}
	for i := range want {
		if p.kinds[i] != want[i] {
			t.Fatalf("stage %d: got %d want %d (full %v)", i, p.kinds[i], want[i], p.kinds)
		}
	}
}
