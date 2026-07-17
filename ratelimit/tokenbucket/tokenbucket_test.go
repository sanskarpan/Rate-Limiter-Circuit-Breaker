package tokenbucket_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskarpan/resilience/internal/clock"
	"github.com/sanskarpan/resilience/internal/testutil"
	"github.com/sanskarpan/resilience/ratelimit"
	"github.com/sanskarpan/resilience/ratelimit/tokenbucket"
)

func newTestClock() *clock.ManualClock {
	return clock.NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
}

func TestTokenBucket_BasicAllow(t *testing.T) {
	clk := newTestClock()
	tb := tokenbucket.New(5, 1, tokenbucket.WithClock(clk))
	defer tb.Close()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		result := tb.Allow(ctx, "key")
		if !result.Allowed {
			t.Fatalf("request %d should be allowed (bucket has %d tokens)", i+1, 5-i)
		}
		if result.Remaining != 5-i-1 {
			t.Fatalf("request %d: expected remaining=%d, got %d", i+1, 5-i-1, result.Remaining)
		}
	}
}

func TestTokenBucket_DenyWhenEmpty(t *testing.T) {
	clk := newTestClock()
	tb := tokenbucket.New(3, 1, tokenbucket.WithClock(clk))
	defer tb.Close()
	ctx := context.Background()

	// Exhaust the bucket
	for i := 0; i < 3; i++ {
		tb.Allow(ctx, "key") //nolint:errcheck
	}
	// Next should be denied
	result := tb.Allow(ctx, "key")
	if result.Allowed {
		t.Fatal("4th request should be denied")
	}
	if result.RetryAfter <= 0 {
		t.Fatal("RetryAfter should be positive when denied")
	}
}

func TestTokenBucket_RefillAfterWait(t *testing.T) {
	clk := newTestClock()
	tb := tokenbucket.New(5, 2, tokenbucket.WithClock(clk)) // 2 tokens/sec
	defer tb.Close()
	ctx := context.Background()

	// Exhaust
	for i := 0; i < 5; i++ {
		tb.Allow(ctx, "key") //nolint:errcheck
	}
	// No tokens
	if tb.Allow(ctx, "key").Allowed {
		t.Fatal("should be denied after exhaustion")
	}

	// Advance 1 second → 2 tokens added
	clk.Advance(time.Second)
	result := tb.Allow(ctx, "key")
	if !result.Allowed {
		t.Fatal("should be allowed after 1 second refill (2 tokens/sec)")
	}
}

func TestTokenBucket_AllowN_AtomicConsume(t *testing.T) {
	clk := newTestClock()
	tb := tokenbucket.New(10, 1, tokenbucket.WithClock(clk))
	defer tb.Close()
	ctx := context.Background()

	result := tb.AllowN(ctx, "key", 5)
	if !result.Allowed {
		t.Fatal("AllowN(5) should succeed with 10 tokens")
	}
	if result.Remaining != 5 {
		t.Fatalf("expected remaining=5, got %d", result.Remaining)
	}

	// Next AllowN(6) should fail atomically — remaining is 5
	result = tb.AllowN(ctx, "key", 6)
	if result.Allowed {
		t.Fatal("AllowN(6) should fail with only 5 tokens (atomic — no partial consume)")
	}
	// Remaining should still be 5 (nothing consumed)
	peek := tb.Peek(ctx, "key")
	if peek.Remaining != 5 {
		t.Fatalf("after failed AllowN(6), remaining should still be 5, got %d", peek.Remaining)
	}
}

func TestTokenBucket_AllowN_ExceedsCapacity(t *testing.T) {
	clk := newTestClock()
	tb := tokenbucket.New(10, 1, tokenbucket.WithClock(clk))
	defer tb.Close()
	ctx := context.Background()

	// AllowN(11) always fails — exceeds capacity
	result := tb.AllowN(ctx, "key", 11)
	if result.Allowed {
		t.Fatal("AllowN(n > capacity) must always return Allowed=false")
	}
}

func TestTokenBucket_Wait_RespectsContext(t *testing.T) {
	clk := newTestClock()
	tb := tokenbucket.New(1, 0.001, tokenbucket.WithClock(clk)) // capacity 1, very slow refill
	defer tb.Close()

	ctx, cancel := context.WithCancel(context.Background())
	// Drain the single token so Wait must block on the (glacial) refill.
	tb.Allow(context.Background(), "key") //nolint:errcheck
	done := make(chan error, 1)
	go func() {
		done <- tb.Wait(ctx, "key")
	}()

	// Cancel context — Wait should return error
	time.Sleep(5 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Wait should return error when context cancelled")
		}
		if !errors.Is(err, ratelimit.ErrContextDone) {
			t.Fatalf("expected ErrContextDone, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not return after context cancellation")
	}
}

func TestTokenBucket_Wait_UnblocksAfterRefill(t *testing.T) {
	// 1 token capacity, 20 tokens/sec → 50ms to refill 1 token from empty
	tb := tokenbucket.New(1, 20, tokenbucket.WithClock(clock.RealClock{}))
	defer tb.Close()
	ctx := context.Background()

	// Consume the 1 token
	tb.Allow(ctx, "key") //nolint:errcheck

	done := make(chan error, 1)
	go func() {
		done <- tb.Wait(ctx, "key") // should unblock after ~50ms
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Wait should succeed: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Wait did not return within 500ms")
	}
}

func TestTokenBucket_BurstBehavior(t *testing.T) {
	clk := newTestClock()
	// capacity=10, refillRate=1/sec → can burst 10 at once
	tb := tokenbucket.New(10, 1, tokenbucket.WithClock(clk))
	defer tb.Close()
	ctx := context.Background()

	// Burst: send 10 requests at once
	for i := 0; i < 10; i++ {
		if !tb.Allow(ctx, "key").Allowed {
			t.Fatalf("burst request %d should be allowed", i+1)
		}
	}
	// 11th denied
	if tb.Allow(ctx, "key").Allowed {
		t.Fatal("11th request should be denied — burst exhausted")
	}

	// After 5 seconds, 5 more allowed
	clk.Advance(5 * time.Second)
	for i := 0; i < 5; i++ {
		if !tb.Allow(ctx, "key").Allowed {
			t.Fatalf("refilled request %d should be allowed", i+1)
		}
	}
}

func TestTokenBucket_Concurrent_NoRace(t *testing.T) {
	tb := tokenbucket.New(1000, 1000, tokenbucket.WithClock(clock.RealClock{}))
	defer tb.Close()
	ctx := context.Background()

	var wg sync.WaitGroup
	allowed := atomic.Int64{}
	denied := atomic.Int64{}

	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result := tb.Allow(ctx, "key")
			if result.Allowed {
				allowed.Add(1)
			} else {
				denied.Add(1)
			}
		}()
	}
	wg.Wait()

	total := allowed.Load() + denied.Load()
	if total != 500 {
		t.Fatalf("expected 500 total results, got %d", total)
	}
}

func TestTokenBucket_Concurrent_RemainingNonNeg(t *testing.T) {
	clk := newTestClock()
	tb := tokenbucket.New(100, 50, tokenbucket.WithClock(clk))
	defer tb.Close()
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result := tb.Allow(ctx, "key")
			if result.Remaining < 0 {
				t.Errorf("invariant violated: remaining=%d must be >= 0", result.Remaining)
			}
		}()
	}
	wg.Wait()
}

func TestTokenBucket_Close_StopsCleanup(t *testing.T) {
	lc := testutil.NewLeakChecker(t)
	defer lc.Check()

	tb := tokenbucket.New(10, 1, tokenbucket.WithIdleCleanup(time.Second))
	if err := tb.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestTokenBucket_MultipleKeys_Isolation(t *testing.T) {
	clk := newTestClock()
	tb := tokenbucket.New(2, 1, tokenbucket.WithClock(clk))
	defer tb.Close()
	ctx := context.Background()

	// Exhaust key "a"
	tb.Allow(ctx, "a") //nolint:errcheck
	tb.Allow(ctx, "a") //nolint:errcheck
	if tb.Allow(ctx, "a").Allowed {
		t.Fatal("key 'a' should be exhausted")
	}

	// Key "b" should be independent
	if !tb.Allow(ctx, "b").Allowed {
		t.Fatal("key 'b' should still be available")
	}
}

func TestTokenBucket_Reset_ClearsState(t *testing.T) {
	clk := newTestClock()
	tb := tokenbucket.New(3, 1, tokenbucket.WithClock(clk))
	defer tb.Close()
	ctx := context.Background()

	// Exhaust
	for i := 0; i < 3; i++ {
		tb.Allow(ctx, "key") //nolint:errcheck
	}
	if tb.Allow(ctx, "key").Allowed {
		t.Fatal("should be denied after exhaustion")
	}

	// Reset
	if err := tb.Reset(ctx, "key"); err != nil {
		t.Fatalf("Reset: %v", err)
	}

	// Should be allowed again
	if !tb.Allow(ctx, "key").Allowed {
		t.Fatal("should be allowed after reset")
	}
}

func TestTokenBucket_Metadata(t *testing.T) {
	clk := newTestClock()
	tb := tokenbucket.New(10, 2, tokenbucket.WithClock(clk))
	defer tb.Close()
	ctx := context.Background()

	// Metadata is available via Peek (not on every Allow call for performance)
	state := tb.Peek(ctx, "key")
	if state.Extra == nil {
		t.Fatal("Peek.Extra should not be nil")
	}
	if _, ok := state.Extra["tokens"]; !ok {
		t.Error("Peek.Extra should contain 'tokens'")
	}
	if _, ok := state.Extra["capacity"]; !ok {
		t.Error("Peek.Extra should contain 'capacity'")
	}
	if _, ok := state.Extra["refill_rate_per_s"]; !ok {
		t.Error("Peek.Extra should contain 'refill_rate_per_s'")
	}
	// Allow is still correct
	result := tb.Allow(ctx, "key")
	if !result.Allowed {
		t.Fatal("first Allow should succeed")
	}
	if result.Algorithm != "token_bucket" {
		t.Fatalf("expected algorithm 'token_bucket', got %q", result.Algorithm)
	}
}

func TestTokenBucket_Peek_DoesNotConsume(t *testing.T) {
	clk := newTestClock()
	tb := tokenbucket.New(5, 1, tokenbucket.WithClock(clk))
	defer tb.Close()
	ctx := context.Background()

	state1 := tb.Peek(ctx, "key")
	state2 := tb.Peek(ctx, "key")
	if state1.Remaining != state2.Remaining {
		t.Fatal("Peek should not change state")
	}
	// Allow should see same remaining
	result := tb.Allow(ctx, "key")
	if result.Remaining != state1.Remaining-1 {
		t.Fatalf("after Allow, remaining=%d should be peek.remaining-1=%d", result.Remaining, state1.Remaining-1)
	}
}

func TestTokenBucket_InvalidKey(t *testing.T) {
	tb := tokenbucket.New(10, 1)
	defer tb.Close()
	ctx := context.Background()

	result := tb.Allow(ctx, "")
	if result.Allowed {
		t.Fatal("empty key should not be allowed")
	}

	result = tb.Allow(ctx, "key\x00hack")
	if result.Allowed {
		t.Fatal("key with null byte should not be allowed")
	}
}

func TestTokenBucket_InvalidN(t *testing.T) {
	tb := tokenbucket.New(10, 1)
	defer tb.Close()
	ctx := context.Background()

	result := tb.AllowN(ctx, "key", 0)
	if result.Allowed {
		t.Fatal("n=0 should not be allowed")
	}
}

func TestTokenBucket_RefillCappedAtCapacity(t *testing.T) {
	clk := newTestClock()
	tb := tokenbucket.New(10, 5, tokenbucket.WithClock(clk)) // 5 tokens/sec, cap 10
	defer tb.Close()
	ctx := context.Background()

	// Wait 5 seconds → would add 25, but capped at 10
	clk.Advance(5 * time.Second)
	peek := tb.Peek(ctx, "key")
	if peek.Remaining > 10 {
		t.Fatalf("tokens should not exceed capacity %d, got %d", 10, peek.Remaining)
	}
}

// TestTokenBucket_TokensNeverNegative verifies that under heavy concurrent load
// the token count never drops below zero (preventing silent over-allowing).
func TestTokenBucket_TokensNeverNegative(t *testing.T) {
	clk := newTestClock()
	// capacity=5, very slow refill
	tb := tokenbucket.New(5, 0.001, tokenbucket.WithClock(clk))
	defer tb.Close()
	ctx := context.Background()

	// Prime the bucket: consume all 5 tokens
	for i := 0; i < 5; i++ {
		r := tb.Allow(ctx, "key")
		if !r.Allowed {
			t.Fatalf("request %d should be allowed (bucket not yet empty)", i)
		}
	}

	// Now fire 100 concurrent requests against the empty bucket.
	// None should be allowed, and Remaining should always be >= 0.
	var wg sync.WaitGroup
	var negativeCount atomic.Int64
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := tb.Allow(ctx, "key")
			if r.Remaining < 0 {
				negativeCount.Add(1)
			}
		}()
	}
	wg.Wait()

	if negativeCount.Load() > 0 {
		t.Fatalf("token Remaining went negative in %d goroutines", negativeCount.Load())
	}
}

// TestTokenBucket_RefillExactCapacity verifies tokens are capped exactly at capacity.
func TestTokenBucket_RefillExactCapacity(t *testing.T) {
	clk := newTestClock()
	tb := tokenbucket.New(10, 10, tokenbucket.WithClock(clk))
	defer tb.Close()
	ctx := context.Background()

	// Consume 1 token, then advance 1 second (refills 10, but capacity caps at 10).
	tb.Allow(ctx, "key") //nolint:errcheck
	clk.Advance(2 * time.Second)

	peek := tb.Peek(ctx, "key")
	if peek.Remaining != 10 {
		t.Fatalf("expected exactly capacity=10 after over-refill, got %d", peek.Remaining)
	}
}

// TestTokenBucket_AllowN_LargerThanCapacityDenied verifies AllowN(n>capacity) is always denied.
func TestTokenBucket_AllowN_LargerThanCapacity(t *testing.T) {
	clk := newTestClock()
	tb := tokenbucket.New(5, 100, tokenbucket.WithClock(clk))
	defer tb.Close()
	ctx := context.Background()

	result := tb.AllowN(ctx, "key", 6)
	if result.Allowed {
		t.Fatal("AllowN(6) with capacity=5 should always be denied regardless of tokens")
	}
}

func TestTokenBucket_String(t *testing.T) {
	clk := newTestClock()
	tb := tokenbucket.New(100, 50, tokenbucket.WithClock(clk))
	defer tb.Close()

	str := tb.String()
	if str == "" {
		t.Fatal("String should not be empty")
	}
	t.Logf("String(): %s", str)
}

func TestTokenBucket_Close_Idempotent(t *testing.T) {
	tb := tokenbucket.New(10, 10, tokenbucket.WithClock(clock.RealClock{}))
	tb.Close() //nolint:errcheck
	tb.Close() //nolint:errcheck
	tb.Close() //nolint:errcheck
}

func TestTokenBucket_AllowN_ExceedsTokens(t *testing.T) {
	clk := newTestClock()
	tb := tokenbucket.New(5, 10, tokenbucket.WithClock(clk))
	defer tb.Close()
	ctx := context.Background()

	// Consume 8 tokens
	for i := 0; i < 8; i++ {
		tb.Allow(ctx, "key") //nolint:errcheck
	}

	// Try to allow 3 more - only 2 tokens left
	result := tb.AllowN(ctx, "key", 3)
	if result.Allowed {
		t.Fatal("AllowN(3) should be denied when only 2 tokens available")
	}
}

func TestTokenBucket_WaitN_Success(t *testing.T) {
	clk := newTestClock()
	tb := tokenbucket.New(5, 5, tokenbucket.WithClock(clk))
	defer tb.Close()

	err := tb.WaitN(context.Background(), "key", 3)
	if err != nil {
		t.Fatalf("WaitN(3) should succeed: %v", err)
	}

	state := tb.Peek(context.Background(), "key")
	if state.Remaining != 2 {
		t.Fatalf("expected remaining=2, got %d", state.Remaining)
	}
}

// TestTokenBucket_WaitN_ImpossibleN_ReturnsQuickly is the regression test for M-4:
// WaitN with n > capacity must return an error immediately instead of looping
// forever against context.Background().
func TestTokenBucket_WaitN_ImpossibleN_ReturnsQuickly(t *testing.T) {
	tb := tokenbucket.New(5, 10, tokenbucket.WithClock(clock.RealClock{}))
	defer tb.Close()

	done := make(chan error, 1)
	go func() {
		// n=10 > capacity=5 → impossible, must not block forever.
		done <- tb.WaitN(context.Background(), "key", 10)
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("WaitN(impossible n) should return an error, got nil")
		}
		if !errors.Is(err, ratelimit.ErrLimitExceeded) {
			t.Fatalf("expected ErrLimitExceeded, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WaitN(impossible n) did not return — looped forever (M-4)")
	}
}

// TestTokenBucket_New_InvalidConfig_Panics is the regression test for M-5:
// non-positive capacity or refillRate must panic with a clear message instead
// of producing a broken limiter.
func TestTokenBucket_New_InvalidConfig_Panics(t *testing.T) {
	cases := []struct {
		name       string
		capacity   float64
		refillRate float64
	}{
		{"negative capacity", -1, 10},
		{"zero refillRate", 10, 0},
		{"negative refillRate", 10, -5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("New(%v, %v) should panic on invalid config", tc.capacity, tc.refillRate)
				}
			}()
			tokenbucket.New(tc.capacity, tc.refillRate)
		})
	}
}

// TestTokenBucket_SetLimit_PreservesState is the regression test for H-12:
// SetLimit must mutate capacity/refillRate in place, preserving per-key token
// state (a drained key stays drained) rather than rebuilding the bucket.
func TestTokenBucket_SetLimit_PreservesState(t *testing.T) {
	clk := newTestClock()
	tb := tokenbucket.New(10, 1, tokenbucket.WithClock(clk))
	defer tb.Close()
	ctx := context.Background()

	// Drain the key completely.
	for i := 0; i < 10; i++ {
		if r := tb.Allow(ctx, "key"); !r.Allowed {
			t.Fatalf("drain: request %d should be allowed", i)
		}
	}
	if r := tb.Allow(ctx, "key"); r.Allowed {
		t.Fatal("key should be drained before SetLimit")
	}

	// Adjust the limit. The key must remain drained (not refilled to full).
	tb.SetLimit(20, 2)

	if r := tb.Allow(ctx, "key"); r.Allowed {
		t.Fatal("after SetLimit the drained key was refilled to full — bucket state wiped (H-12)")
	}
	if r := tb.Allow(ctx, "key"); r.Limit != 20 {
		t.Fatalf("expected new limit 20 to take effect, got %d", r.Limit)
	}
}

// TestTokenBucket_SetLimit_ClampsTokens verifies shrinking capacity clamps a
// full bucket's tokens down to the new (smaller) capacity.
func TestTokenBucket_SetLimit_ClampsTokens(t *testing.T) {
	clk := newTestClock()
	tb := tokenbucket.New(100, 1, tokenbucket.WithClock(clk))
	defer tb.Close()
	ctx := context.Background()

	// Materialize a full bucket for the key.
	tb.Allow(ctx, "key") //nolint:errcheck

	tb.SetLimit(5, 1)

	state := tb.Peek(ctx, "key")
	if state.Remaining > 5 {
		t.Fatalf("tokens not clamped to new capacity: remaining=%d > 5", state.Remaining)
	}
}
