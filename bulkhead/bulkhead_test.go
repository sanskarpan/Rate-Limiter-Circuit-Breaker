package bulkhead

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Bulkhead tests
// ---------------------------------------------------------------------------

// TestBulkhead_AllowUpToConcurrencyLimit verifies that exactly maxConcurrency
// goroutines can be in flight simultaneously, and the (maxConcurrency+1)th
// is rejected when maxWait is 0.
func TestBulkhead_AllowUpToConcurrencyLimit(t *testing.T) {
	const limit = 3
	b := New(limit, 0)

	// Barrier keeps the goroutines inside Execute while we count inflight.
	barrier := make(chan struct{})
	var started atomic.Int64
	var wg sync.WaitGroup

	for i := 0; i < limit; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = b.Execute(context.Background(), func(ctx context.Context) error {
				started.Add(1)
				<-barrier // hold the slot
				return nil
			})
		}()
	}

	// Wait until all limit slots are occupied.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if b.Inflight() == int64(limit) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := b.Inflight(); got != int64(limit) {
		t.Fatalf("expected %d inflight, got %d", limit, got)
	}

	// An additional request must be rejected immediately (maxWait=0).
	err := b.Execute(context.Background(), func(ctx context.Context) error {
		return nil
	})
	if !errors.Is(err, ErrBulkheadFull) {
		t.Fatalf("expected ErrBulkheadFull, got %v", err)
	}

	// Release all goroutines.
	close(barrier)
	wg.Wait()

	if got := b.Inflight(); got != 0 {
		t.Fatalf("expected 0 inflight after release, got %d", got)
	}
}

// TestBulkhead_RejectWhenFull_NoWait verifies non-blocking rejection (maxWait=0).
func TestBulkhead_RejectWhenFull_NoWait(t *testing.T) {
	b := New(1, 0)

	// Fill the single slot.
	hold := make(chan struct{})
	var slotHeld sync.WaitGroup
	slotHeld.Add(1)
	go func() {
		_ = b.Execute(context.Background(), func(ctx context.Context) error {
			slotHeld.Done()
			<-hold
			return nil
		})
	}()
	slotHeld.Wait()

	// Second call must be rejected immediately.
	start := time.Now()
	err := b.Execute(context.Background(), func(ctx context.Context) error { return nil })
	elapsed := time.Since(start)

	if !errors.Is(err, ErrBulkheadFull) {
		t.Fatalf("expected ErrBulkheadFull, got %v", err)
	}
	if elapsed > 50*time.Millisecond {
		t.Fatalf("non-blocking reject took too long: %v", elapsed)
	}
	if b.Rejected() != 1 {
		t.Fatalf("expected 1 rejected, got %d", b.Rejected())
	}

	close(hold)
}

// TestBulkhead_QueueWithWait verifies that a call can wait for a slot when
// maxWait > 0 and a slot becomes free within that window.
func TestBulkhead_QueueWithWait(t *testing.T) {
	b := New(1, 500*time.Millisecond)

	hold := make(chan struct{})
	var firstIn sync.WaitGroup
	firstIn.Add(1)

	// Occupy the single slot.
	go func() {
		_ = b.Execute(context.Background(), func(ctx context.Context) error {
			firstIn.Done()
			<-hold
			return nil
		})
	}()
	firstIn.Wait()

	// Second caller waits; release the slot after a short delay.
	go func() {
		time.Sleep(50 * time.Millisecond)
		close(hold)
	}()

	err := b.Execute(context.Background(), func(ctx context.Context) error { return nil })
	if err != nil {
		t.Fatalf("expected nil error after slot freed, got %v", err)
	}
	if b.Rejected() != 0 {
		t.Fatalf("expected 0 rejected, got %d", b.Rejected())
	}
}

// TestBulkhead_ContextCancellationWhileWaiting verifies that a waiting caller
// is unblocked and returns ctx.Err() when its context is cancelled.
func TestBulkhead_ContextCancellationWhileWaiting(t *testing.T) {
	b := New(1, 5*time.Second) // long wait so only ctx cancellation fires

	hold := make(chan struct{})
	var firstIn sync.WaitGroup
	firstIn.Add(1)

	// Occupy the single slot.
	go func() {
		_ = b.Execute(context.Background(), func(ctx context.Context) error {
			firstIn.Done()
			<-hold
			return nil
		})
	}()
	firstIn.Wait()

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel the context after a short delay.
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := b.Execute(ctx, func(ctx context.Context) error { return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if b.Rejected() != 1 {
		t.Fatalf("expected 1 rejected, got %d", b.Rejected())
	}

	close(hold) // release the first goroutine
}

// TestBulkhead_SlotReleasedAfterPanic verifies that the concurrency slot is
// released even when fn panics, so subsequent calls can still proceed.
func TestBulkhead_SlotReleasedAfterPanic(t *testing.T) {
	b := New(1, 0)

	// Call a function that panics; catch the panic in the caller.
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected a panic to propagate")
			}
		}()
		_ = b.Execute(context.Background(), func(ctx context.Context) error {
			panic("intentional panic for test")
		})
	}()

	// The slot must have been released by defer in Execute.
	// Poll briefly to allow the defer to run (it is synchronous, so this
	// should be immediate).
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if b.Inflight() == 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := b.Inflight(); got != 0 {
		t.Fatalf("slot not released after panic: inflight=%d", got)
	}

	// A subsequent call should succeed.
	err := b.Execute(context.Background(), func(ctx context.Context) error { return nil })
	if err != nil {
		t.Fatalf("expected nil after slot released, got %v", err)
	}
}

// TestBulkhead_MetricsAccurate verifies that Inflight and Rejected counters
// stay consistent across concurrent executions and rejections.
func TestBulkhead_MetricsAccurate(t *testing.T) {
	const limit = 5
	const total = 20
	b := New(limit, 0)

	hold := make(chan struct{})
	var started sync.WaitGroup
	started.Add(limit)

	// Fill all slots.
	for i := 0; i < limit; i++ {
		go func() {
			_ = b.Execute(context.Background(), func(ctx context.Context) error {
				started.Done()
				<-hold
				return nil
			})
		}()
	}
	started.Wait()

	if got := b.Inflight(); got != int64(limit) {
		t.Fatalf("expected %d inflight, got %d", limit, got)
	}

	// Submit the rest; all should be rejected.
	var rejected atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < total-limit; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := b.Execute(context.Background(), func(ctx context.Context) error { return nil })
			if errors.Is(err, ErrBulkheadFull) {
				rejected.Add(1)
			}
		}()
	}
	wg.Wait()

	expectedRejected := int64(total - limit)
	if got := b.Rejected(); got != expectedRejected {
		t.Fatalf("expected %d rejected, got %d", expectedRejected, got)
	}
	if got := rejected.Load(); got != expectedRejected {
		t.Fatalf("locally counted %d rejections, expected %d", got, expectedRejected)
	}

	// Release all slots and verify inflight drops to zero.
	close(hold)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if b.Inflight() == 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := b.Inflight(); got != 0 {
		t.Fatalf("expected 0 inflight after release, got %d", got)
	}
}

// ---------------------------------------------------------------------------
// ThreadPool tests
// ---------------------------------------------------------------------------

// TestThreadPool_AsyncExecution verifies that submitted tasks run and their
// results are delivered on the returned channel.
func TestThreadPool_AsyncExecution(t *testing.T) {
	tp := NewThreadPool(2, 10)
	defer tp.Close()

	sentinel := errors.New("result from task")
	ch, err := tp.Submit(context.Background(), func(ctx context.Context) error {
		return sentinel
	})
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}

	select {
	case got := <-ch:
		if !errors.Is(got, sentinel) {
			t.Fatalf("expected sentinel error, got %v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for task result")
	}
}

// TestThreadPool_QueueFull_Rejected verifies that Submit returns ErrQueueFull
// immediately when the queue is saturated.
func TestThreadPool_QueueFull_Rejected(t *testing.T) {
	// Single worker, zero-depth queue so tasks have nowhere to buffer.
	tp := NewThreadPool(1, 0)
	defer tp.Close()

	// Block the sole worker indefinitely so the queue stays full.
	hold := make(chan struct{})
	// The worker will pick this up directly from the queue channel.
	ch, err := tp.Submit(context.Background(), func(ctx context.Context) error {
		<-hold
		return nil
	})
	// The first submit might succeed (worker picks it up) or queue it;
	// either way we need the worker busy. Give it a moment.
	if err == nil && ch != nil {
		// Worker grabbed it; try again to fill the queue.
		// With queueSize=0 the channel has no buffer, so subsequent Submit
		// calls will always see the queue full when the worker is busy.
	}

	// Now try to submit more tasks. With a 0-depth queue these must be
	// rejected once the worker is occupied.
	rejected := false
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		_, subErr := tp.Submit(context.Background(), func(ctx context.Context) error { return nil })
		if errors.Is(subErr, ErrQueueFull) {
			rejected = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !rejected {
		t.Fatal("expected ErrQueueFull when queue is saturated, but never got it")
	}

	close(hold)
}

// TestThreadPool_Close_StopsWorkers verifies that Close waits for the workers
// to finish and that the pool stops processing after Close returns.
func TestThreadPool_Close_StopsWorkers(t *testing.T) {
	tp := NewThreadPool(2, 10)

	var executed atomic.Int64
	const n = 5

	var wg sync.WaitGroup
	channels := make([]<-chan error, 0, n)
	for i := 0; i < n; i++ {
		ch, err := tp.Submit(context.Background(), func(ctx context.Context) error {
			executed.Add(1)
			return nil
		})
		if err != nil {
			t.Fatalf("Submit %d returned unexpected error: %v", i, err)
		}
		wg.Add(1)
		channels = append(channels, ch)
	}

	// Collect results in background.
	for _, ch := range channels {
		ch := ch
		go func() {
			defer wg.Done()
			select {
			case <-ch:
			case <-time.After(5 * time.Second):
				t.Errorf("timed out waiting for task result")
			}
		}()
	}

	tp.Close() // blocks until all workers have exited
	wg.Wait()

	if got := executed.Load(); got != int64(n) {
		t.Fatalf("expected %d executions, got %d", n, got)
	}
}

// TestThreadPool_SubmitAfterClose_ReturnsErrorNoHang is a regression test for
// H-16. Before the fix, Close did not close the queue nor track closed state,
// so a Submit after Close with free buffer space returned (ch, nil) while no
// worker remained to consume the task — the caller would block on <-ch forever.
// After the fix Submit must return ErrPoolClosed promptly without hanging.
func TestThreadPool_SubmitAfterClose_ReturnsErrorNoHang(t *testing.T) {
	tp := NewThreadPool(2, 10) // buffered queue: has free space after Close
	tp.Close()

	done := make(chan struct{})
	var ch <-chan error
	var err error
	go func() {
		ch, err = tp.Submit(context.Background(), func(context.Context) error { return nil })
		close(done)
	}()

	select {
	case <-done:
		// Submit returned promptly.
	case <-time.After(2 * time.Second):
		t.Fatal("Submit after Close hung (watchdog fired)")
	}

	if !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("expected ErrPoolClosed, got %v", err)
	}
	if ch != nil {
		t.Fatalf("expected nil result channel on closed pool, got %v", ch)
	}
}

// TestThreadPool_DoubleClose_NoPanic verifies Close is idempotent after the fix
// (it now guards against closing the done channel twice).
func TestThreadPool_DoubleClose_NoPanic(t *testing.T) {
	tp := NewThreadPool(1, 1)
	tp.Close()
	tp.Close() // must not panic
}
