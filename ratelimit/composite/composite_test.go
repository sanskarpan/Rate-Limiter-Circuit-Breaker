package composite_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/composite"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/fixedwindow"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
)

func newClock() *clock.ManualClock {
	return clock.NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
}

// TestComposite_AND_BothAllow verifies AND mode allows when both limiters allow.
func TestComposite_AND_BothAllow(t *testing.T) {
	clk := newClock()
	limA := tokenbucket.New(10, 10, tokenbucket.WithClock(clk))
	limB := fixedwindow.New(10, time.Second, fixedwindow.WithClock(clk))
	defer limA.Close()
	defer limB.Close()

	comp := composite.New(composite.AND, limA, limB)
	defer comp.Close()

	ctx := context.Background()
	result := comp.Allow(ctx, "key")
	if !result.Allowed {
		t.Fatal("AND mode: both allow → should be allowed")
	}
}

// TestComposite_AND_AAllowsBDenies verifies AND mode denies when B denies, A's token NOT consumed.
func TestComposite_AND_AAllowsBDenies(t *testing.T) {
	clk := newClock()
	// A has 10 tokens, B has limit=1 and already exhausted
	limA := tokenbucket.New(10, 10, tokenbucket.WithClock(clk))
	limB := tokenbucket.New(1, 1, tokenbucket.WithClock(clk))

	ctx := context.Background()
	// Exhaust limB
	limB.Allow(ctx, "key") //nolint:errcheck

	comp := composite.New(composite.AND, limA, limB)
	defer comp.Close()

	// AND mode: A would allow, B denies → overall denied, A's token NOT consumed
	result := comp.Allow(ctx, "key")
	if result.Allowed {
		t.Fatal("AND mode: B denied → should be denied overall")
	}

	// Verify A's token was NOT consumed (should still have 10 tokens)
	stateA := limA.Peek(ctx, "key")
	if stateA.Remaining != 10 {
		t.Fatalf("A's token should NOT be consumed when B denied, got remaining=%d", stateA.Remaining)
	}
}

// TestComposite_AND_BothConsume verifies AND mode consumes from both when both allow.
func TestComposite_AND_BothConsume(t *testing.T) {
	clk := newClock()
	limA := tokenbucket.New(10, 10, tokenbucket.WithClock(clk))
	limB := tokenbucket.New(5, 5, tokenbucket.WithClock(clk))

	comp := composite.New(composite.AND, limA, limB)
	defer comp.Close()

	ctx := context.Background()
	result := comp.Allow(ctx, "key")
	if !result.Allowed {
		t.Fatal("AND mode: both allow → should be allowed")
	}

	// Verify both were consumed
	stateA := limA.Peek(ctx, "key")
	stateB := limB.Peek(ctx, "key")
	if stateA.Remaining != 9 {
		t.Fatalf("A should have 9 remaining after consume, got %d", stateA.Remaining)
	}
	if stateB.Remaining != 4 {
		t.Fatalf("B should have 4 remaining after consume, got %d", stateB.Remaining)
	}
}

// TestComposite_OR_AAllows verifies OR mode allows when first limiter allows.
func TestComposite_OR_AAllows(t *testing.T) {
	clk := newClock()
	limA := tokenbucket.New(10, 10, tokenbucket.WithClock(clk))
	limB := tokenbucket.New(0, 1, tokenbucket.WithClock(clk)) // empty, will deny

	comp := composite.New(composite.OR, limA, limB)
	defer comp.Close()

	ctx := context.Background()
	result := comp.Allow(ctx, "key")
	if !result.Allowed {
		t.Fatal("OR mode: A allows → should be allowed")
	}
}

// TestComposite_OR_ADenies_BAllows verifies OR mode allows when second limiter allows.
func TestComposite_OR_ADenies_BAllows(t *testing.T) {
	clk := newClock()
	limA := tokenbucket.New(0, 1, tokenbucket.WithClock(clk)) // empty, will deny
	limB := tokenbucket.New(10, 10, tokenbucket.WithClock(clk))

	comp := composite.New(composite.OR, limA, limB)
	defer comp.Close()

	ctx := context.Background()
	result := comp.Allow(ctx, "key")
	if !result.Allowed {
		t.Fatal("OR mode: B allows → should be allowed")
	}
}

// TestComposite_OR_BothDeny verifies OR mode denies when all limiters deny.
func TestComposite_OR_BothDeny(t *testing.T) {
	clk := newClock()
	limA := tokenbucket.New(0, 1, tokenbucket.WithClock(clk)) // empty
	limB := tokenbucket.New(0, 1, tokenbucket.WithClock(clk)) // empty

	comp := composite.New(composite.OR, limA, limB)
	defer comp.Close()

	ctx := context.Background()
	result := comp.Allow(ctx, "key")
	if result.Allowed {
		t.Fatal("OR mode: both deny → should be denied")
	}
}

// TestComposite_AND_ReturnsMostRestrictiveResult verifies AND returns most restrictive values.
func TestComposite_AND_ReturnsMostRestrictiveResult(t *testing.T) {
	clk := newClock()
	limA := tokenbucket.New(10, 10, tokenbucket.WithClock(clk))
	limB := tokenbucket.New(3, 3, tokenbucket.WithClock(clk))

	comp := composite.New(composite.AND, limA, limB)
	defer comp.Close()

	ctx := context.Background()
	result := comp.Allow(ctx, "key")
	if !result.Allowed {
		t.Fatal("should be allowed")
	}
	// Remaining should be the minimum across both limiters
	if result.Remaining != 2 { // B has 3-1=2 remaining
		t.Fatalf("expected remaining=2 (min of A=9, B=2), got %d", result.Remaining)
	}
}

// TestComposite_Concurrent_NoRace verifies no data races.
func TestComposite_Concurrent_NoRace(t *testing.T) {
	limA := tokenbucket.New(1000, 1000, tokenbucket.WithClock(clock.RealClock{}))
	limB := tokenbucket.New(2000, 2000, tokenbucket.WithClock(clock.RealClock{}))
	defer limA.Close()
	defer limB.Close()

	comp := composite.New(composite.AND, limA, limB)
	defer comp.Close()

	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			comp.Allow(ctx, "key") //nolint:errcheck
		}()
	}
	wg.Wait()
}

// TestComposite_Reset_AllLimiters verifies Reset propagates to all limiters.
func TestComposite_Reset_AllLimiters(t *testing.T) {
	clk := newClock()
	limA := tokenbucket.New(2, 1, tokenbucket.WithClock(clk))
	limB := tokenbucket.New(2, 1, tokenbucket.WithClock(clk))

	comp := composite.New(composite.AND, limA, limB)
	defer comp.Close()

	ctx := context.Background()
	// Exhaust both
	comp.Allow(ctx, "key") //nolint:errcheck
	comp.Allow(ctx, "key") //nolint:errcheck

	// Reset
	if err := comp.Reset(ctx, "key"); err != nil {
		t.Fatalf("Reset: %v", err)
	}

	// Should be allowed again
	if !comp.Allow(ctx, "key").Allowed {
		t.Fatal("should be allowed after reset")
	}
}

// TestComposite_Algorithm field in result.
func TestComposite_AlgorithmField(t *testing.T) {
	lim := tokenbucket.New(10, 10, tokenbucket.WithClock(clock.RealClock{}))
	defer lim.Close()

	and := composite.New(composite.AND, lim)
	defer and.Close()

	ctx := context.Background()
	r := and.Allow(ctx, "key")
	if r.Algorithm != "composite_and" {
		t.Fatalf("expected algorithm 'composite_and', got %q", r.Algorithm)
	}
}

func TestComposite_Wait_BlocksUntilAllowed(t *testing.T) {
	clk := newClock()
	lim := tokenbucket.New(1, 1, tokenbucket.WithClock(clk))
	defer lim.Close()

	comp := composite.New(composite.AND, lim)
	defer comp.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// First request should succeed immediately
	if err := comp.Wait(ctx, "key"); err != nil {
		t.Fatalf("first Wait: %v", err)
	}

	// Second request should block until we advance time
	waitCh := make(chan error, 1)
	go func() {
		waitCh <- comp.Wait(ctx, "key")
	}()

	// Request should be blocked
	select {
	case <-waitCh:
		t.Fatal("Wait should still be blocking")
	case <-time.After(10 * time.Millisecond):
		// Expected - still blocked
	}

	// Advance time to refill tokens
	clk.Advance(time.Second)

	// Now should succeed
	select {
	case err := <-waitCh:
		if err != nil {
			t.Fatalf("second Wait: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Wait should have succeeded after time advance")
	}
}

func TestComposite_WaitN(t *testing.T) {
	clk := newClock()
	lim := tokenbucket.New(10, 10, tokenbucket.WithClock(clk))
	defer lim.Close()

	comp := composite.New(composite.AND, lim)
	defer comp.Close()

	ctx := context.Background()

	// WaitN for 5 should succeed immediately
	if err := comp.WaitN(ctx, "key", 5); err != nil {
		t.Fatalf("WaitN(5): %v", err)
	}
}

func TestComposite_Peek(t *testing.T) {
	clk := newClock()
	lim := tokenbucket.New(10, 10, tokenbucket.WithClock(clk))
	defer lim.Close()

	comp := composite.New(composite.AND, lim)
	defer comp.Close()

	ctx := context.Background()

	// Peek before any request
	state := comp.Peek(ctx, "key")
	if state.Remaining != 10 {
		t.Fatalf("expected remaining=10, got %d", state.Remaining)
	}

	// Make a request
	comp.Allow(ctx, "key")

	// Peek after request
	state = comp.Peek(ctx, "key")
	if state.Remaining != 9 {
		t.Fatalf("expected remaining=9 after Allow, got %d", state.Remaining)
	}
}

func TestComposite_Peek_DoesNotConsume(t *testing.T) {
	clk := newClock()
	lim := tokenbucket.New(10, 10, tokenbucket.WithClock(clk))
	defer lim.Close()

	comp := composite.New(composite.AND, lim)
	defer comp.Close()

	ctx := context.Background()

	// Peek twice - should not consume
	comp.Peek(ctx, "key")
	comp.Peek(ctx, "key")

	// Now actually allow
	result := comp.Allow(ctx, "key")
	if result.Remaining != 9 {
		t.Fatalf("expected remaining=9 after Allow following Peek, got %d", result.Remaining)
	}
}

func TestComposite_OR_ShortestRetry(t *testing.T) {
	clk := newClock()
	limA := tokenbucket.New(0, 1, tokenbucket.WithClock(clk)) // Denies immediately
	limB := tokenbucket.New(0, 1, tokenbucket.WithClock(clk)) // Denies immediately

	comp := composite.New(composite.OR, limA, limB)
	defer comp.Close()

	ctx := context.Background()
	result := comp.Allow(ctx, "key")

	if result.Allowed {
		t.Fatal("should be denied")
	}

	// Both should deny, so retry should be set
	if result.RetryAfter <= 0 {
		t.Fatalf("expected positive RetryAfter, got %v", result.RetryAfter)
	}
}

func TestComposite_MostRestrictive(t *testing.T) {
	clk := newClock()
	lim1 := tokenbucket.New(10, 10, tokenbucket.WithClock(clk))
	lim2 := tokenbucket.New(5, 5, tokenbucket.WithClock(clk))
	defer lim1.Close()
	defer lim2.Close()

	comp := composite.New(composite.AND, lim1, lim2)
	defer comp.Close()

	ctx := context.Background()
	comp.Allow(ctx, "key")

	state := comp.Peek(ctx, "key")
	// Most restrictive is lim2 (5 tokens), so remaining should be 4
	if state.Remaining != 4 {
		t.Fatalf("expected remaining=4 (most restrictive), got %d", state.Remaining)
	}
}

func TestComposite_SingleLimiter(t *testing.T) {
	lim := tokenbucket.New(10, 10, tokenbucket.WithClock(clock.RealClock{}))
	defer lim.Close()

	// Single limiter in AND mode should work
	comp := composite.New(composite.AND, lim)
	defer comp.Close()

	ctx := context.Background()
	result := comp.Allow(ctx, "key")
	if !result.Allowed {
		t.Fatal("should be allowed with single limiter")
	}
}

func TestComposite_Empty_Denies(t *testing.T) {
	comp := composite.New(composite.AND)
	defer comp.Close()

	ctx := context.Background()
	result := comp.Allow(ctx, "key")
	if result.Allowed {
		t.Fatal("empty composite should deny")
	}
}

func TestComposite_OR_OneAllows(t *testing.T) {
	clk := newClock()
	limA := tokenbucket.New(1, 1, tokenbucket.WithClock(clk))
	limB := tokenbucket.New(0, 1, tokenbucket.WithClock(clk)) // Denies
	defer limA.Close()
	defer limB.Close()

	comp := composite.New(composite.OR, limA, limB)
	defer comp.Close()

	ctx := context.Background()
	result := comp.Allow(ctx, "key")
	if !result.Allowed {
		t.Fatal("OR mode: one allows → should be allowed")
	}
}

func TestComposite_OR_AllDeny(t *testing.T) {
	clk := newClock()
	limA := tokenbucket.New(0, 1, tokenbucket.WithClock(clk))
	limB := tokenbucket.New(0, 1, tokenbucket.WithClock(clk))
	defer limA.Close()
	defer limB.Close()

	comp := composite.New(composite.OR, limA, limB)
	defer comp.Close()

	ctx := context.Background()
	result := comp.Allow(ctx, "key")
	if result.Allowed {
		t.Fatal("OR mode: all deny → should be denied")
	}
}

func TestComposite_WaitN_ExceedsLimit(t *testing.T) {
	clk := newClock()
	lim := tokenbucket.New(2, 2, tokenbucket.WithClock(clk))
	defer lim.Close()

	comp := composite.New(composite.AND, lim)
	defer comp.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := comp.WaitN(ctx, "key", 5)
	if err == nil {
		t.Fatal("WaitN(5) should fail with timeout when limit is 2")
	}
}

func TestComposite_Reset(t *testing.T) {
	clk := newClock()
	lim := tokenbucket.New(1, 1, tokenbucket.WithClock(clk))
	defer lim.Close()

	comp := composite.New(composite.AND, lim)
	defer comp.Close()

	ctx := context.Background()
	comp.Allow(ctx, "key") //nolint:errcheck
	comp.Reset(ctx, "key") //nolint:errcheck

	state := comp.Peek(ctx, "key")
	if state.Remaining != 1 {
		t.Fatalf("expected remaining=1 after reset, got %d", state.Remaining)
	}
}

func TestComposite_Close_Idempotent(t *testing.T) {
	lim := tokenbucket.New(10, 10, tokenbucket.WithClock(clock.RealClock{}))
	comp := composite.New(composite.AND, lim)

	comp.Close() //nolint:errcheck
	comp.Close() //nolint:errcheck
	comp.Close() //nolint:errcheck
}

func TestComposite_InvalidKey(t *testing.T) {
	clk := newClock()
	lim := tokenbucket.New(10, 10, tokenbucket.WithClock(clk))
	defer lim.Close()

	comp := composite.New(composite.AND, lim)
	defer comp.Close()

	result := comp.Allow(context.Background(), "")
	if result.Allowed {
		t.Fatal("empty key should be denied")
	}
}

func TestComposite_InvalidN(t *testing.T) {
	clk := newClock()
	lim := tokenbucket.New(10, 10, tokenbucket.WithClock(clk))
	defer lim.Close()

	comp := composite.New(composite.AND, lim)
	defer comp.Close()

	result := comp.AllowN(context.Background(), "key", 0)
	if result.Allowed {
		t.Fatal("n=0 should be denied")
	}

	result = comp.AllowN(context.Background(), "key", -1)
	if result.Allowed {
		t.Fatal("n=-1 should be denied")
	}
}
