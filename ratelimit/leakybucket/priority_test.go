package leakybucket_test

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/leakybucket"
)

// waitAllQueued spins until the bucket's Peek reports the expected queue depth,
// so a test can be sure all N priority waiters have parked in the heap before it
// starts advancing the manual clock (otherwise a tick could fire against a
// partially-populated heap and the ordering would be non-deterministic).
func waitAllQueued(t *testing.T, lb *leakybucket.LeakyBucket, key string, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		st := lb.Peek(context.Background(), key)
		if qd, ok := st.Extra["queue_depth"].(int); ok && qd >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d queued waiters on %q", want, key)
}

// TestPriority_HigherServedFirst enqueues waiters with mixed priorities and
// asserts the leaker serves them strictly in descending priority order.
func TestPriority_HigherServedFirst(t *testing.T) {
	// leak rate 1/s → one leak per advanced second, driven by a ManualClock.
	mc := clock.NewManualClock(time.Unix(0, 0))
	lb := leakybucket.New(10, 1, leakybucket.WithClock(mc), leakybucket.WithIdleCleanup(0))
	defer lb.Close()

	ctx := context.Background()
	priorities := []int{1, 5, 3, 5, 2} // note the two 5s to check intra-level FIFO
	type done struct {
		priority int
	}
	results := make(chan done, len(priorities))

	var wg sync.WaitGroup
	for i, p := range priorities {
		wg.Add(1)
		p := p
		i := i
		go func() {
			defer wg.Done()
			r := lb.AllowP(ctx, "k", p)
			if !r.Allowed {
				t.Errorf("waiter %d (prio %d) was denied unexpectedly", i, p)
				return
			}
			results <- done{priority: p}
		}()
	}

	// Ensure every waiter is parked before we start leaking.
	waitAllQueued(t, lb, "k", len(priorities))

	// Advance one interval per served waiter and collect completion order.
	served := make([]int, 0, len(priorities))
	for range priorities {
		mc.Advance(time.Second)
		select {
		case d := <-results:
			served = append(served, d.priority)
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for a served waiter (got %d so far)", len(served))
		}
	}
	wg.Wait()

	// Served priorities must be in non-increasing order (higher first).
	if !sort.SliceIsSorted(served, func(i, j int) bool { return served[i] > served[j] }) {
		t.Fatalf("waiters not served in descending priority order: %v", served)
	}
	// The first two served must be the two priority-5 waiters.
	if served[0] != 5 || served[1] != 5 {
		t.Fatalf("expected the two priority-5 waiters served first, got %v", served)
	}
}

// TestPriority_CancellationRemovesWaiter verifies a cancelled priority waiter is
// removed from the heap (never served) and does not occupy a slot, so the
// remaining waiters are all served.
func TestPriority_CancellationRemovesWaiter(t *testing.T) {
	mc := clock.NewManualClock(time.Unix(0, 0))
	lb := leakybucket.New(10, 1, leakybucket.WithClock(mc), leakybucket.WithIdleCleanup(0))
	defer lb.Close()

	// One waiter we will cancel, two that must complete.
	cancelCtx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	cancelledResult := make(chan ratelimit.Result, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		cancelledResult <- lb.AllowP(cancelCtx, "k", 100) // highest priority
	}()

	okResults := make(chan ratelimit.Result, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			okResults <- lb.AllowP(context.Background(), "k", 1)
		}()
	}

	waitAllQueued(t, lb, "k", 3)

	// Cancel the high-priority waiter before any leak fires.
	cancel()
	select {
	case r := <-cancelledResult:
		if r.Allowed {
			t.Fatal("cancelled waiter must not be allowed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled waiter did not return")
	}

	// The two remaining waiters must both be served over two leaks; the cancelled
	// one must never consume a leak slot.
	allowed := 0
	for i := 0; i < 2; i++ {
		mc.Advance(time.Second)
		select {
		case r := <-okResults:
			if r.Allowed {
				allowed++
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for remaining waiter %d", i)
		}
	}
	wg.Wait()
	if allowed != 2 {
		t.Fatalf("expected both remaining waiters allowed, got %d", allowed)
	}
}

// TestPriority_WaitP_Cancellation verifies WaitP returns a context error wrapping
// ErrContextDone when its context is cancelled while queued.
func TestPriority_WaitP_Cancellation(t *testing.T) {
	mc := clock.NewManualClock(time.Unix(0, 0))
	lb := leakybucket.New(5, 1, leakybucket.WithClock(mc), leakybucket.WithIdleCleanup(0))
	defer lb.Close()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- lb.WaitP(ctx, "k", 0) }()

	waitAllQueued(t, lb, "k", 1)
	cancel()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected an error after cancellation")
		}
		if !errors.Is(err, ratelimit.ErrContextDone) {
			t.Fatalf("expected ErrContextDone, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WaitP did not return after cancellation")
	}
}

// TestPriority_QueueFullDenies verifies a priority request is denied immediately
// (never blocks) when the shared queue is already at capacity.
func TestPriority_QueueFullDenies(t *testing.T) {
	mc := clock.NewManualClock(time.Unix(0, 0))
	lb := leakybucket.New(2, 1, leakybucket.WithClock(mc), leakybucket.WithIdleCleanup(0))
	defer lb.Close()

	ctx := context.Background()
	// Fill the queue with two priority waiters that will park.
	for i := 0; i < 2; i++ {
		go func() { lb.AllowP(ctx, "k", 0) }()
	}
	waitAllQueued(t, lb, "k", 2)

	// A third priority request must be denied immediately without blocking.
	deniedCh := make(chan bool, 1)
	go func() { deniedCh <- lb.AllowP(ctx, "k", 10).Allowed }()
	select {
	case allowed := <-deniedCh:
		if allowed {
			t.Fatal("request against a full queue must be denied")
		}
	case <-time.After(time.Second):
		t.Fatal("AllowP against a full queue blocked instead of denying immediately")
	}
}

// TestPriority_DefaultWaitUnaffected sanity-checks that the plain (channel) Wait
// path still works alongside priority waiters and is not starved forever: with a
// ManualClock, both a heap waiter and a channel waiter eventually get served
// (heap first, then channel).
func TestPriority_HeapBeforeChannel(t *testing.T) {
	mc := clock.NewManualClock(time.Unix(0, 0))
	lb := leakybucket.New(10, 1, leakybucket.WithClock(mc), leakybucket.WithIdleCleanup(0))
	defer lb.Close()
	ctx := context.Background()

	chanDone := make(chan struct{}, 1)
	heapDone := make(chan struct{}, 1)

	// Enqueue a plain channel waiter first, then a heap waiter.
	go func() {
		if lb.Allow(ctx, "k").Allowed {
			chanDone <- struct{}{}
		}
	}()
	// Wait for the channel waiter to be queued (depth 1).
	waitAllQueued(t, lb, "k", 1)
	go func() {
		if lb.AllowP(ctx, "k", 1).Allowed {
			heapDone <- struct{}{}
		}
	}()
	waitAllQueued(t, lb, "k", 2)

	// First leak must serve the heap waiter (priority path wins even though the
	// channel waiter arrived earlier).
	mc.Advance(time.Second)
	select {
	case <-heapDone:
	case <-time.After(2 * time.Second):
		t.Fatal("heap waiter not served first")
	}
	select {
	case <-chanDone:
		t.Fatal("channel waiter served before heap waiter")
	default:
	}

	// Second leak serves the channel waiter.
	mc.Advance(time.Second)
	select {
	case <-chanDone:
	case <-time.After(2 * time.Second):
		t.Fatal("channel waiter never served")
	}
}
