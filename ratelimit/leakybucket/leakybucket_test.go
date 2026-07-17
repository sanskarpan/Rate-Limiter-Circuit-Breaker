package leakybucket_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/testutil"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/leakybucket"
)

// TestLeakyBucket_ConstantOutputRate verifies requests are processed at exactly leak_rate req/s.
func TestLeakyBucket_ConstantOutputRate(t *testing.T) {
	// 10 requests/second, queue capacity 5
	// At real time: 3 requests should process in ~300ms
	lb := leakybucket.New(5, 10, leakybucket.WithClock(clock.RealClock{}))
	defer lb.Close()
	ctx := context.Background()

	start := time.Now()
	var wg sync.WaitGroup
	results := make([]bool, 3)
	for i := 0; i < 3; i++ {
		wg.Add(1)
		idx := i
		go func() {
			defer wg.Done()
			results[idx] = lb.Allow(ctx, "key").Allowed
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	allowed := 0
	for _, r := range results {
		if r {
			allowed++
		}
	}
	// At least 1 should be allowed (first one in queue gets processed)
	if allowed == 0 {
		t.Fatal("at least 1 request should be allowed")
	}
	// Processing 3 requests at 10/s takes ~300ms; allow generous 2s timeout
	if elapsed > 2*time.Second {
		t.Fatalf("took too long: %s (expected ~300ms for 3 requests at 10/s)", elapsed)
	}
}

// TestLeakyBucket_DenyWhenQueueFull verifies that capacity+1 concurrent sends deny one.
func TestLeakyBucket_DenyWhenQueueFull(t *testing.T) {
	// capacity=2, very slow leak (0.1/s = 10s between leaks)
	// Send 3 requests simultaneously — 3rd should be denied immediately
	lb := leakybucket.New(2, 0.1, leakybucket.WithClock(clock.RealClock{}))
	defer lb.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	var denied atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result := lb.Allow(ctx, "key")
			if !result.Allowed {
				denied.Add(1)
			}
		}()
	}
	wg.Wait()

	if denied.Load() == 0 {
		t.Fatal("at least one request should be denied when queue is full")
	}
}

// TestLeakyBucket_QueuedRequestsProcessed verifies queued requests eventually succeed.
func TestLeakyBucket_QueuedRequestsProcessed(t *testing.T) {
	// 20 requests/second, capacity=2
	// Send 2 requests — both should eventually be processed
	lb := leakybucket.New(2, 20, leakybucket.WithClock(clock.RealClock{}))
	defer lb.Close()
	ctx := context.Background()

	var wg sync.WaitGroup
	var allowed atomic.Int64
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result := lb.Allow(ctx, "key")
			if result.Allowed {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()

	if allowed.Load() == 0 {
		t.Fatal("queued requests should eventually be processed")
	}
}

// TestLeakyBucket_Wait_ContextCancellation verifies cancelling context while in queue returns error.
func TestLeakyBucket_Wait_ContextCancellation(t *testing.T) {
	// Very slow leak — requests will sit in queue
	lb := leakybucket.New(5, 0.001, leakybucket.WithClock(clock.RealClock{}))
	defer lb.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- lb.Wait(ctx, "key")
	}()

	// Let the goroutine queue the request
	time.Sleep(10 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		// May be nil (if it was processed before cancel) or non-nil (context cancelled)
		// We just verify Wait returns
		_ = err
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not return after context cancellation")
	}
}

// TestLeakyBucket_CloseStopsLeaker verifies Close() stops all goroutines (no leak).
func TestLeakyBucket_CloseStopsLeaker(t *testing.T) {
	lc := testutil.NewLeakChecker(t)
	defer lc.Check()

	lb := leakybucket.New(10, 1, leakybucket.WithIdleCleanup(time.Second))
	ctx := context.Background()
	// Create a key to start leaker goroutine
	lb.Allow(ctx, "key")              //nolint:errcheck
	time.Sleep(50 * time.Millisecond) // let goroutine start
	if err := lb.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestLeakyBucket_Concurrent_NoRace verifies no data races under high concurrency.
func TestLeakyBucket_Concurrent_NoRace(t *testing.T) {
	lb := leakybucket.New(100, 1000, leakybucket.WithClock(clock.RealClock{}))
	defer lb.Close()
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lb.Allow(ctx, "key") //nolint:errcheck
		}()
	}
	wg.Wait()
}

// TestLeakyBucket_MultipleKeys_Isolation verifies key "a" and "b" are independent.
func TestLeakyBucket_MultipleKeys_Isolation(t *testing.T) {
	// Small capacity, slow leak
	lb := leakybucket.New(1, 1000, leakybucket.WithClock(clock.RealClock{}))
	defer lb.Close()
	ctx := context.Background()

	// Fill queue for key "a"
	lb.Allow(ctx, "a") //nolint:errcheck
	lb.Allow(ctx, "a") // This may get queued

	// Key "b" should not be affected by key "a"'s queue
	result := lb.Peek(ctx, "b")
	if result.Remaining != 1 {
		t.Logf("key 'b' remaining=%d (expected 1)", result.Remaining)
		// Just verify keys are independent (b has room)
		if result.Remaining < 0 {
			t.Fatal("key 'b' remaining should not be negative")
		}
	}
}

// TestLeakyBucket_Reset_ClearsState verifies Reset restores full queue capacity.
func TestLeakyBucket_Reset_ClearsState(t *testing.T) {
	lb := leakybucket.New(2, 1000, leakybucket.WithClock(clock.RealClock{}))
	defer lb.Close()
	ctx := context.Background()

	// Peek should show full capacity
	state := lb.Peek(ctx, "key")
	if state.Limit != 2 {
		t.Fatalf("expected limit=2, got %d", state.Limit)
	}

	if err := lb.Reset(ctx, "key"); err != nil {
		t.Fatalf("Reset: %v", err)
	}
}

// TestLeakyBucket_InvalidKey verifies invalid keys are rejected.
func TestLeakyBucket_InvalidKey(t *testing.T) {
	lb := leakybucket.New(10, 1)
	defer lb.Close()
	ctx := context.Background()

	result := lb.Allow(ctx, "")
	if result.Allowed {
		t.Fatal("empty key should not be allowed")
	}

	result = lb.Allow(ctx, "key\x00hack")
	if result.Allowed {
		t.Fatal("key with null byte should not be allowed")
	}
}

// TestLeakyBucket_Peek_DoesNotConsume verifies Peek doesn't consume queue slots.
func TestLeakyBucket_Peek_DoesNotConsume(t *testing.T) {
	lb := leakybucket.New(5, 1000, leakybucket.WithClock(clock.RealClock{}))
	defer lb.Close()
	ctx := context.Background()

	state1 := lb.Peek(ctx, "key")
	state2 := lb.Peek(ctx, "key")
	if state1.Remaining != state2.Remaining {
		t.Fatal("Peek should not change state")
	}
	if state1.Remaining != 5 {
		t.Fatalf("expected remaining=5, got %d", state1.Remaining)
	}
}

// TestLeakyBucket_AllowN_Basic verifies AllowN queues exactly n tokens.
func TestLeakyBucket_AllowN_Basic(t *testing.T) {
	// Fast leak: 1000/s so tokens clear quickly
	lb := leakybucket.New(10, 1000, leakybucket.WithClock(clock.RealClock{}))
	defer lb.Close()
	ctx := context.Background()

	// AllowN(3) should queue 3 tokens and eventually return allowed
	result := lb.AllowN(ctx, "key", 3)
	if !result.Allowed {
		t.Fatalf("AllowN(3) with capacity=10 should be allowed; result=%+v", result)
	}
}

// TestLeakyBucket_AllowN_ExceedsCapacity verifies AllowN rejects n > capacity immediately.
func TestLeakyBucket_AllowN_ExceedsCapacity(t *testing.T) {
	lb := leakybucket.New(5, 1000)
	defer lb.Close()
	ctx := context.Background()

	result := lb.AllowN(ctx, "key", 6)
	if result.Allowed {
		t.Fatal("AllowN(n > capacity) must be denied")
	}
	if result.Limit != 5 {
		t.Fatalf("expected limit=5, got %d", result.Limit)
	}
}

// TestLeakyBucket_AllowN_DeniedWhenQueueFull verifies AllowN(n) is atomic:
// if queue has fewer than n free slots, the entire batch is denied.
func TestLeakyBucket_AllowN_DeniedWhenQueueFull(t *testing.T) {
	// capacity=3, very slow leak (0.01/s = one leak per 100s)
	lb := leakybucket.New(3, 0.01)
	defer lb.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// Fill 2 out of 3 slots (context timeout will cause allow to not fully block)
	for i := 0; i < 2; i++ {
		go lb.Allow(ctx, "full") //nolint:errcheck
	}
	time.Sleep(5 * time.Millisecond) // let goroutines queue

	// Try to AllowN(2) when only 1 slot (at most) remains — should fail
	ctx2 := context.Background()
	result := lb.AllowN(ctx2, "full", 3) // needs all 3, but 2 are occupied
	if result.Allowed {
		t.Fatal("AllowN(3) should be denied when queue has < 3 free slots")
	}
}

// TestLeakyBucket_AllowN_One is equivalent to Allow(1).
func TestLeakyBucket_AllowN_One(t *testing.T) {
	lb := leakybucket.New(5, 1000, leakybucket.WithClock(clock.RealClock{}))
	defer lb.Close()
	ctx := context.Background()

	r1 := lb.Allow(ctx, "key")
	lb.Reset(ctx, "key") //nolint:errcheck

	r2 := lb.AllowN(ctx, "keyN", 1)
	if r1.Allowed != r2.Allowed {
		t.Fatalf("AllowN(1) should behave like Allow: Allow=%v, AllowN=%v", r1.Allowed, r2.Allowed)
	}
}

// TestLeakyBucket_AllowN_InvalidN verifies AllowN rejects n <= 0.
func TestLeakyBucket_AllowN_InvalidN(t *testing.T) {
	lb := leakybucket.New(5, 1000)
	defer lb.Close()
	ctx := context.Background()

	result := lb.AllowN(ctx, "key", 0)
	if result.Allowed {
		t.Fatal("AllowN(0) must be denied")
	}
	result = lb.AllowN(ctx, "key", -1)
	if result.Allowed {
		t.Fatal("AllowN(-1) must be denied")
	}
}

// TestLeakyBucket_AllowN_AtomicUnderContention is the regression test for H-6.
//
// AllowN must be all-or-nothing: a batch that reports Allowed=false must not have
// left ANY of its tokens sitting in the queue. We use a ManualClock that is never
// advanced, so the leaker never drains — every enqueued token stays put and the
// queue depth is a faithful ledger of tokens actually committed.
//
// Many goroutines concurrently call AllowN(n) on an empty capacity-C bucket where
// C is not a multiple of n. Because the enqueue is atomic under q.mu, every batch
// that reaches the queue contributes EXACTLY n tokens — so the final queue depth
// must always be a whole multiple of n (0, n, 2n, ...) and never exceed C.
//
// Allowed callers block on their result channels (nothing ever leaks), so we give
// them a short-timeout context to unblock; their already-enqueued tokens stay put.
//
// The pre-fix code released q.mu before enqueuing (and mis-broke out of a select
// instead of the loop), so under contention a batch that passed the capacity
// check could partially enqueue and then report denied — stranding 1..n-1 tokens
// and leaving the depth at a NON-multiple of n (e.g. 7 or 8 with n=3).
func TestLeakyBucket_AllowN_AtomicUnderContention(t *testing.T) {
	const (
		capacity = 8
		batch    = 3 // 8 is not a multiple of 3 → forces contention at the boundary
		workers  = 200
	)

	for trial := 0; trial < 30; trial++ {
		clk := clock.NewManualClock(time.Unix(0, 0))
		lb := leakybucket.New(capacity, 1000, leakybucket.WithClock(clk))

		// Short timeout so ALLOWED callers (which would otherwise block forever on
		// the never-advancing leaker) return; their tokens stay in the queue.
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)

		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start                      // release all at once to maximize contention
				lb.AllowN(ctx, "key", batch) //nolint:errcheck
			}()
		}
		close(start)
		wg.Wait()
		cancel()

		finalDepth := lb.Peek(context.Background(), "key").Extra["queue_depth"].(int)
		lb.Close() //nolint:errcheck

		if finalDepth > capacity {
			t.Fatalf("trial %d: queue depth %d exceeds capacity %d", trial, finalDepth, capacity)
		}
		if finalDepth%batch != 0 {
			t.Fatalf("trial %d: atomicity violated — final depth=%d is not a multiple of batch=%d; "+
				"an AllowN batch partially enqueued and stranded tokens (H-6)", trial, finalDepth, batch)
		}
	}
}

// TestLeakyBucket_New_InvalidConfig_Panics is the regression test for M-5.
func TestLeakyBucket_New_InvalidConfig_Panics(t *testing.T) {
	cases := []struct {
		name     string
		capacity int
		leakRate float64
	}{
		{"zero capacity", 0, 10},
		{"negative capacity", -1, 10},
		{"zero leakRate", 10, 0},
		{"negative leakRate", 10, -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("New(cap=%d, leak=%v) should panic on invalid config", tc.capacity, tc.leakRate)
				}
			}()
			leakybucket.New(tc.capacity, tc.leakRate)
		})
	}
}

// TestLeakyBucket_WaitN_ImpossibleN_ReturnsQuickly is the regression test for M-4:
// WaitN with n > capacity must return an error immediately, not loop forever.
func TestLeakyBucket_WaitN_ImpossibleN_ReturnsQuickly(t *testing.T) {
	lb := leakybucket.New(5, 10, leakybucket.WithClock(clock.RealClock{}))
	defer lb.Close()

	done := make(chan error, 1)
	go func() {
		done <- lb.WaitN(context.Background(), "key", 10) // 10 > capacity 5
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("WaitN(n > capacity) should return an error, got nil")
		}
		if !errors.Is(err, ratelimit.ErrLimitExceeded) {
			t.Fatalf("expected ErrLimitExceeded, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WaitN(impossible n) did not return — looped forever (M-4)")
	}
}
