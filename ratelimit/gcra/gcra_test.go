package gcra_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sanskarpan/resilience/internal/clock"
	"github.com/sanskarpan/resilience/internal/testutil"
	"github.com/sanskarpan/resilience/ratelimit"
	"github.com/sanskarpan/resilience/ratelimit/gcra"
)

func newTestClock() *clock.ManualClock {
	return clock.NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
}

// TestGCRA_BasicRate verifies requests are allowed at the configured rate.
func TestGCRA_BasicRate(t *testing.T) {
	clk := newTestClock()
	// 10 req/s, burst=1 (strict), 1s window
	g := gcra.New(10, 1, time.Second, gcra.WithClock(clk))
	defer g.Close()
	ctx := context.Background()

	// First request always allowed (TAT starts at zero)
	result := g.Allow(ctx, "key")
	if !result.Allowed {
		t.Fatal("first request should be allowed")
	}
	// Second request denied — emissionInterval=100ms, only 0ms elapsed
	result = g.Allow(ctx, "key")
	if result.Allowed {
		t.Fatal("second request should be denied — TAT not yet reached")
	}
	if result.RetryAfter <= 0 {
		t.Fatal("RetryAfter should be positive when denied")
	}

	// Advance 100ms → next request allowed
	clk.Advance(100 * time.Millisecond)
	result = g.Allow(ctx, "key")
	if !result.Allowed {
		t.Fatalf("request after 100ms should be allowed (emissionInterval=100ms), retryAfter=%s", result.RetryAfter)
	}
}

// TestGCRA_BurstAllowed verifies burst=5 allows 5 initial requests.
func TestGCRA_BurstAllowed(t *testing.T) {
	clk := newTestClock()
	// 10 req/s, burst=5, 1s window
	g := gcra.New(10, 5, time.Second, gcra.WithClock(clk))
	defer g.Close()
	ctx := context.Background()

	// First 5 requests should all be allowed immediately
	for i := 0; i < 5; i++ {
		result := g.Allow(ctx, "key")
		if !result.Allowed {
			t.Fatalf("burst request %d should be allowed (burst=5)", i+1)
		}
	}
}

// TestGCRA_BurstExhausted verifies 6th request in burst is denied.
func TestGCRA_BurstExhausted(t *testing.T) {
	clk := newTestClock()
	// 10 req/s, burst=5, 1s window
	g := gcra.New(10, 5, time.Second, gcra.WithClock(clk))
	defer g.Close()
	ctx := context.Background()

	// Exhaust burst
	for i := 0; i < 5; i++ {
		g.Allow(ctx, "key") //nolint:errcheck
	}
	// 6th should be denied
	result := g.Allow(ctx, "key")
	if result.Allowed {
		t.Fatal("6th request should be denied — burst exhausted")
	}
}

// TestGCRA_ExactTATCalculation verifies TAT formula with known values.
func TestGCRA_ExactTATCalculation(t *testing.T) {
	clk := newTestClock()
	start := clk.Now()
	// 10 req/s, burst=1, 1s window → emissionInterval=100ms
	g := gcra.New(10, 1, time.Second, gcra.WithClock(clk))
	defer g.Close()
	ctx := context.Background()

	// First request: TAT = max(zero, start) + 100ms = start + 100ms
	r1 := g.Allow(ctx, "key")
	if !r1.Allowed {
		t.Fatal("first request should be allowed")
	}

	// 60ms later — next request should be denied
	// TAT = start + 100ms, threshold = TAT - 0 = start + 100ms, now = start + 60ms
	// threshold > now → denied
	clk.Advance(60 * time.Millisecond)
	r2 := g.Allow(ctx, "key")
	if r2.Allowed {
		t.Fatal("request at 60ms should be denied (TAT=100ms)")
	}
	// RetryAfter should be approximately 40ms
	expected := 40 * time.Millisecond
	if r2.RetryAfter < 30*time.Millisecond || r2.RetryAfter > 50*time.Millisecond {
		t.Fatalf("expected RetryAfter~40ms, got %s", r2.RetryAfter)
	}
	_ = start
	_ = expected
}

// TestGCRA_RetryAfterPrecise verifies RetryAfter is the exact time to next allowed request.
func TestGCRA_RetryAfterPrecise(t *testing.T) {
	clk := newTestClock()
	// 5 req/s, burst=1, 1s window → emissionInterval=200ms
	g := gcra.New(5, 1, time.Second, gcra.WithClock(clk))
	defer g.Close()
	ctx := context.Background()

	g.Allow(ctx, "key") //nolint:errcheck

	// Advance 100ms — should be denied with RetryAfter ≈ 100ms
	clk.Advance(100 * time.Millisecond)
	result := g.Allow(ctx, "key")
	if result.Allowed {
		t.Fatal("should be denied")
	}

	// Advance by RetryAfter — should now be allowed
	clk.Advance(result.RetryAfter)
	result = g.Allow(ctx, "key")
	if !result.Allowed {
		t.Fatalf("should be allowed after advancing by RetryAfter=%s", result.RetryAfter)
	}
}

// TestGCRA_RemainingCalculation verifies Remaining field accuracy.
func TestGCRA_RemainingCalculation(t *testing.T) {
	clk := newTestClock()
	// 10 req/s, burst=5, 1s window → emissionInterval=100ms, burstOffset=400ms
	g := gcra.New(10, 5, time.Second, gcra.WithClock(clk))
	defer g.Close()
	ctx := context.Background()

	// First request: remaining = burst - 1 = 4
	result := g.Allow(ctx, "key")
	if !result.Allowed {
		t.Fatal("first request should be allowed")
	}
	if result.Remaining != 4 {
		t.Fatalf("expected remaining=4 after first request, got %d", result.Remaining)
	}
}

// TestGCRA_KeyAbsent_BehavesCorrectly verifies first request for new key is always allowed.
func TestGCRA_KeyAbsent_BehavesCorrectly(t *testing.T) {
	clk := newTestClock()
	g := gcra.New(10, 1, time.Second, gcra.WithClock(clk))
	defer g.Close()
	ctx := context.Background()

	// Fresh key — must always be allowed
	for _, key := range []string{"user:1", "user:2", "user:3"} {
		result := g.Allow(ctx, key)
		if !result.Allowed {
			t.Fatalf("first request for new key %q should always be allowed", key)
		}
	}
}

// TestGCRA_Concurrent_NoRace verifies no data races under high concurrency.
func TestGCRA_Concurrent_NoRace(t *testing.T) {
	g := gcra.New(1000, 100, time.Second, gcra.WithClock(clock.RealClock{}))
	defer g.Close()
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			g.Allow(ctx, "key") //nolint:errcheck
		}()
	}
	wg.Wait()
}

// TestGCRA_NoFloatDrift verifies no timing drift over many iterations using integer arithmetic.
func TestGCRA_NoFloatDrift(t *testing.T) {
	clk := newTestClock()
	// 1000 req/s, burst=1, 1s window → emissionInterval=1ms
	g := gcra.New(1000, 1, time.Second, gcra.WithClock(clk))
	defer g.Close()
	ctx := context.Background()

	// Run 1000 requests, advancing 1ms each time
	allowed := 0
	for i := 0; i < 1000; i++ {
		if g.Allow(ctx, "key").Allowed {
			allowed++
		}
		clk.Advance(time.Millisecond)
	}

	// Should allow approximately 1000 requests (1 per ms at 1000/s)
	// Allow 10% tolerance
	if allowed < 900 || allowed > 1100 {
		t.Fatalf("expected ~1000 allowed requests, got %d (potential drift)", allowed)
	}
}

// TestGCRA_MultipleKeys_Isolation verifies keys are independent.
func TestGCRA_MultipleKeys_Isolation(t *testing.T) {
	clk := newTestClock()
	g := gcra.New(5, 1, time.Second, gcra.WithClock(clk))
	defer g.Close()
	ctx := context.Background()

	// Exhaust key "a" (burst=1, so 1 request allowed then denied)
	g.Allow(ctx, "a") //nolint:errcheck
	if g.Allow(ctx, "a").Allowed {
		t.Fatal("key 'a' should be rate limited")
	}

	// Key "b" should be independent — first request always allowed
	if !g.Allow(ctx, "b").Allowed {
		t.Fatal("key 'b' should not be affected by key 'a'")
	}
}

// TestGCRA_Reset_ClearsState verifies Reset restores key to initial state.
func TestGCRA_Reset_ClearsState(t *testing.T) {
	clk := newTestClock()
	g := gcra.New(5, 1, time.Second, gcra.WithClock(clk))
	defer g.Close()
	ctx := context.Background()

	g.Allow(ctx, "key") //nolint:errcheck
	if g.Allow(ctx, "key").Allowed {
		t.Fatal("should be rate limited before reset")
	}

	if err := g.Reset(ctx, "key"); err != nil {
		t.Fatalf("Reset: %v", err)
	}

	// After reset, first request should be allowed again
	if !g.Allow(ctx, "key").Allowed {
		t.Fatal("should be allowed after reset")
	}
}

// TestGCRA_Close_StopsCleanup verifies Close() stops background goroutine.
func TestGCRA_Close_StopsCleanup(t *testing.T) {
	lc := testutil.NewLeakChecker(t)
	defer lc.Check()

	g := gcra.New(10, 1, time.Second, gcra.WithIdleCleanup(time.Second))
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestGCRA_AllowN_ExceedsBurst verifies AllowN(n > burst) always fails.
func TestGCRA_AllowN_ExceedsBurst(t *testing.T) {
	clk := newTestClock()
	g := gcra.New(10, 3, time.Second, gcra.WithClock(clk))
	defer g.Close()
	ctx := context.Background()

	result := g.AllowN(ctx, "key", 4) // exceeds burst=3
	if result.Allowed {
		t.Fatal("AllowN(n > burst) must always return Allowed=false")
	}
}

// TestGCRA_BurstZeroCoercedToOne verifies burst < 1 is treated as burst = 1.
func TestGCRA_BurstZeroCoercedToOne(t *testing.T) {
	clk := newTestClock()
	// burst=0 should be coerced to 1
	g := gcra.New(10, 0, time.Second, gcra.WithClock(clk))
	defer g.Close()
	ctx := context.Background()

	// Should allow exactly 1 (burst=1) consecutive request
	r1 := g.Allow(ctx, "key")
	if !r1.Allowed {
		t.Fatal("first request must be allowed with burst=0 (coerced to 1)")
	}

	// Second immediate request should be denied (no burst headroom)
	r2 := g.Allow(ctx, "key")
	if r2.Allowed {
		t.Fatal("second immediate request must be denied with burst=1")
	}
}

// TestGCRA_AllowN_LargeN_SafelyDenied verifies AllowN with n larger than burst
// is denied safely without arithmetic overflow.
func TestGCRA_AllowN_LargeN_SafelyDenied(t *testing.T) {
	clk := newTestClock()
	// burst=10, so anything n>10 must be denied
	g := gcra.New(1000, 10, time.Second, gcra.WithClock(clk))
	defer g.Close()
	ctx := context.Background()

	largeCases := []int{11, 100, 1000, 100_000}
	for _, n := range largeCases {
		r := g.AllowN(ctx, "key", n)
		if r.Allowed {
			t.Fatalf("AllowN(%d) with burst=10 must be denied", n)
		}
	}
}

// TestGCRA_RetryAfterIsPositiveWhenDenied verifies RetryAfter > 0 on denial.
func TestGCRA_RetryAfterIsPositiveWhenDenied(t *testing.T) {
	clk := newTestClock()
	// 1 req/s, burst=1
	g := gcra.New(1, 1, time.Second, gcra.WithClock(clk))
	defer g.Close()
	ctx := context.Background()

	g.Allow(ctx, "key") // consume the only slot
	r := g.Allow(ctx, "key")
	if r.Allowed {
		t.Fatal("second request should be denied")
	}
	if r.RetryAfter <= 0 {
		t.Fatalf("RetryAfter should be positive when denied, got %s", r.RetryAfter)
	}
}

func TestGCRA_Peek(t *testing.T) {
	clk := newTestClock()
	g := gcra.New(10, 5, time.Second, gcra.WithClock(clk))
	defer g.Close()
	ctx := context.Background()

	g.Allow(ctx, "key") //nolint:errcheck

	state := g.Peek(ctx, "key")
	if state.Remaining != 4 {
		t.Fatalf("expected remaining=4, got %d", state.Remaining)
	}
}

func TestGCRA_Peek_DifferentKeys(t *testing.T) {
	clk := newTestClock()
	g := gcra.New(10, 5, time.Second, gcra.WithClock(clk))
	defer g.Close()
	ctx := context.Background()

	g.Allow(ctx, "key1") //nolint:errcheck

	state1 := g.Peek(ctx, "key1")
	state2 := g.Peek(ctx, "key2")

	if state1.Remaining != 4 {
		t.Fatalf("key1 remaining should be 4, got %d", state1.Remaining)
	}
	if state2.Remaining != 5 {
		t.Fatalf("key2 remaining should be 5, got %d", state2.Remaining)
	}
}

func TestGCRA_Peek_DoesNotConsume(t *testing.T) {
	clk := newTestClock()
	g := gcra.New(10, 5, time.Second, gcra.WithClock(clk))
	defer g.Close()
	ctx := context.Background()

	state1 := g.Peek(ctx, "key")
	state2 := g.Peek(ctx, "key")

	if state1.Remaining != state2.Remaining {
		t.Fatal("Peek should not consume tokens")
	}
}

func TestGCRA_Close_Idempotent(t *testing.T) {
	g := gcra.New(10, 1, time.Second)
	g.Close() //nolint:errcheck
	g.Close() //nolint:errcheck
	g.Close() //nolint:errcheck
}

func TestGCRA_String(t *testing.T) {
	g := gcra.New(100, 10, time.Minute)
	defer g.Close()
	str := g.String()
	if str == "" {
		t.Fatal("String should not be empty")
	}
	t.Logf("String(): %s", str)
}

func TestGCRA_InvalidKey(t *testing.T) {
	clk := newTestClock()
	g := gcra.New(10, 1, time.Second, gcra.WithClock(clk))
	defer g.Close()
	ctx := context.Background()

	result := g.Allow(ctx, "")
	if result.Allowed {
		t.Fatal("empty key should be denied")
	}
}

func TestGCRA_InvalidN(t *testing.T) {
	clk := newTestClock()
	g := gcra.New(10, 5, time.Second, gcra.WithClock(clk))
	defer g.Close()
	ctx := context.Background()

	result := g.AllowN(ctx, "key", 0)
	if result.Allowed {
		t.Fatal("n=0 should be denied")
	}

	result = g.AllowN(ctx, "key", -1)
	if result.Allowed {
		t.Fatal("n=-1 should be denied")
	}
}

func TestGCRA_Wait_Success(t *testing.T) {
	clk := newTestClock()
	g := gcra.New(10, 1, time.Second, gcra.WithClock(clk))
	defer g.Close()

	err := g.Wait(context.Background(), "key")
	if err != nil {
		t.Fatalf("Wait should succeed: %v", err)
	}
}

func TestGCRA_WaitN(t *testing.T) {
	clk := newTestClock()
	g := gcra.New(10, 5, time.Second, gcra.WithClock(clk))
	defer g.Close()

	err := g.WaitN(context.Background(), "key", 3)
	if err != nil {
		t.Fatalf("WaitN(3) should succeed: %v", err)
	}

	state := g.Peek(context.Background(), "key")
	if state.Remaining != 2 {
		t.Fatalf("expected remaining=2, got %d", state.Remaining)
	}
}

// TestGCRA_WaitN_ImpossibleN_ReturnsQuickly is the regression test for M-4:
// WaitN with n > burst must return an error immediately instead of looping
// forever against context.Background().
func TestGCRA_WaitN_ImpossibleN_ReturnsQuickly(t *testing.T) {
	clk := newTestClock()
	g := gcra.New(10, 5, time.Second, gcra.WithClock(clk))
	defer g.Close()

	done := make(chan error, 1)
	go func() {
		// n=6 > burst=5 → impossible.
		done <- g.WaitN(context.Background(), "key", 6)
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("WaitN(n > burst) should return an error, got nil")
		}
		if !errors.Is(err, ratelimit.ErrLimitExceeded) {
			t.Fatalf("expected ErrLimitExceeded, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WaitN(n > burst) did not return — looped forever (M-4)")
	}
}

// TestGCRA_New_InvalidConfig_Panics is the regression test for M-5:
// limit <= 0 must panic with a clear message instead of an integer
// divide-by-zero; window <= 0 must also panic.
func TestGCRA_New_InvalidConfig_Panics(t *testing.T) {
	cases := []struct {
		name   string
		limit  int
		window time.Duration
	}{
		{"zero limit (divide-by-zero)", 0, time.Second},
		{"negative limit", -5, time.Second},
		{"zero window", 10, 0},
		{"negative window", 10, -time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("New(limit=%d, window=%s) should panic on invalid config", tc.limit, tc.window)
				}
			}()
			gcra.New(tc.limit, 1, tc.window)
		})
	}
}
