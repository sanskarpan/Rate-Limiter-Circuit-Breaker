package composite_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskarpan/resilience/internal/clock"
	"github.com/sanskarpan/resilience/ratelimit/composite"
	"github.com/sanskarpan/resilience/ratelimit/tokenbucket"
)

// TestComposite_AND_NoTokenLeak_UnderConcurrency is the regression test for C-5.
// Under concurrent load, AND mode must not consume a token from an early limiter
// unless the whole chain allows. The bottleneck limiter B has capacity 50, so
// exactly 50 requests may pass; limiter A (huge capacity) must therefore have
// consumed exactly 50 tokens — never more.
func TestComposite_AND_NoTokenLeak_UnderConcurrency(t *testing.T) {
	clk := clock.NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	// refillRate is >0 (required) but the manual clock is never advanced, so no
	// refills happen during the test.
	const capA = 100000
	limA := tokenbucket.New(capA, 1, tokenbucket.WithClock(clk))
	limB := tokenbucket.New(50, 1, tokenbucket.WithClock(clk))
	defer limA.Close()
	defer limB.Close()

	comp := composite.New(composite.AND, limA, limB)
	defer comp.Close()

	ctx := context.Background()
	const goroutines = 200
	const perG = 20
	var allowed int64
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perG; j++ {
				if comp.AllowN(ctx, "key", 1).Allowed {
					atomic.AddInt64(&allowed, 1)
				}
			}
		}()
	}
	wg.Wait()

	if allowed != 50 {
		t.Fatalf("expected exactly 50 allowed (limiter B capacity), got %d", allowed)
	}
	remA := limA.Peek(ctx, "key").Remaining
	consumedA := capA - remA
	if int64(consumedA) != allowed {
		t.Fatalf("token leak: limiter A consumed %d tokens but composite allowed %d", consumedA, allowed)
	}
}

// TestComposite_AND_Deny_RetryAfterPositive is the regression test for M-1.
// When AND mode denies because a limiter is exhausted, RetryAfter must be a
// real, positive, clock-correct duration (previously it was hard-coded to 0,
// which turned WaitN into a 1ms busy loop). The deny must also NOT consume a
// token from the allowing limiter (C-5 interaction).
func TestComposite_AND_Deny_RetryAfterPositive(t *testing.T) {
	clk := clock.NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	limA := tokenbucket.New(10, 1, tokenbucket.WithClock(clk)) // roomy
	limB := tokenbucket.New(1, 1, tokenbucket.WithClock(clk))  // 1 token, refills in 1s
	defer limA.Close()
	defer limB.Close()

	comp := composite.New(composite.AND, limA, limB)
	defer comp.Close()
	ctx := context.Background()

	if r := comp.AllowN(ctx, "key", 1); !r.Allowed {
		t.Fatalf("first request should be allowed, got %+v", r)
	}
	remAAfterFirst := limA.Peek(ctx, "key").Remaining

	// B is now exhausted → this must deny with a positive RetryAfter.
	r := comp.AllowN(ctx, "key", 1)
	if r.Allowed {
		t.Fatal("expected deny once limiter B is exhausted")
	}
	if r.RetryAfter <= 0 {
		t.Fatalf("expected positive RetryAfter on deny, got %v", r.RetryAfter)
	}
	if r.RetryAfter > 2*time.Second {
		t.Fatalf("RetryAfter unreasonably large: %v", r.RetryAfter)
	}
	// The deny must not have consumed another token from A.
	if remA := limA.Peek(ctx, "key").Remaining; remA != remAAfterFirst {
		t.Fatalf("deny leaked a token from limiter A: remaining %d -> %d", remAAfterFirst, remA)
	}
}
