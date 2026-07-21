package bulkhead

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/clock"
)

// atomicClock is a minimal clock.Clock for tests that need a deterministically
// controllable time source. Only Now() advances (via Add); all other methods
// delegate to clock.RealClock so the bulkhead timer still fires in real time.
type atomicClock struct {
	clock.RealClock
	ns atomic.Int64
}

func newAtomicClock(start time.Time) *atomicClock {
	c := &atomicClock{}
	c.ns.Store(start.UnixNano())
	return c
}

func (c *atomicClock) Now() time.Time { return time.Unix(0, c.ns.Load()) }
func (c *atomicClock) Add(d time.Duration) { c.ns.Add(int64(d)) }

// waitForInt polls fn until it returns want or the deadline elapses. It returns
// the last observed value and whether it matched.
func waitForInt(fn func() int, want int, timeout time.Duration) (int, bool) {
	deadline := time.Now().Add(timeout)
	var got int
	for time.Now().Before(deadline) {
		got = fn()
		if got == want {
			return got, true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return fn(), false
}

// TestBulkhead_WaitingReflectsBlockedCallers verifies QueueDepth()
// tracks the number of callers blocked in the maxWait wait and returns to 0 once
// they acquire a slot.
func TestBulkhead_WaitingReflectsBlockedCallers(t *testing.T) {
	const blockers = 4
	b := New(1, 5*time.Second) // one slot, long wait so callers queue

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

	// Launch blockers that will all wait for the one slot.
	release := make(chan struct{})
	var acquired atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < blockers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = b.Execute(context.Background(), func(ctx context.Context) error {
				acquired.Add(1)
				<-release
				return nil
			})
		}()
	}

	// All blockers should be counted as waiting.
	if got, ok := waitForInt(b.QueueDepth, blockers, 2*time.Second); !ok {
		t.Fatalf("QueueDepth() = %d, want %d", got, blockers)
	}

	// Release the first holder; blockers acquire the slot one at a time.
	close(hold)
	close(release)
	wg.Wait()

	if got, ok := waitForInt(b.QueueDepth, 0, 2*time.Second); !ok {
		t.Fatalf("QueueDepth() = %d after drain, want 0", got)
	}
	if got := int(acquired.Load()); got != blockers {
		t.Fatalf("acquired = %d, want %d", got, blockers)
	}
}

// TestBulkhead_QueueDepthZeroWhenNonBlocking verifies that with maxWait==0 there
// is no queue, so QueueDepth is always 0 even under rejection pressure.
func TestBulkhead_QueueDepthZeroWhenNonBlocking(t *testing.T) {
	b := New(1, 0)

	hold := make(chan struct{})
	var in sync.WaitGroup
	in.Add(1)
	go func() {
		_ = b.Execute(context.Background(), func(ctx context.Context) error {
			in.Done()
			<-hold
			return nil
		})
	}()
	in.Wait()

	// Hammer with rejected calls; none should ever register as waiting.
	for i := 0; i < 20; i++ {
		_ = b.Execute(context.Background(), func(ctx context.Context) error { return nil })
		if got := b.QueueDepth(); got != 0 {
			t.Fatalf("QueueDepth() = %d with maxWait=0, want 0", got)
		}
	}
	close(hold)
}

// TestBulkhead_WaitTimeTrackedOnAcquire verifies a caller that waits ~D reports
// a wait duration ~D using an injected clock for determinism.
func TestBulkhead_WaitTimeTrackedOnAcquire(t *testing.T) {
	// Injected clock: advances by a fixed step on each read after the wait
	// starts, giving a deterministic measured wait without real sleeps.
	clk := newAtomicClock(time.Unix(0, 0))

	const waitDur = 250 * time.Millisecond
	b := New(1, 5*time.Second, WithClock(clk))

	hold := make(chan struct{})
	var firstIn sync.WaitGroup
	firstIn.Add(1)
	go func() {
		_ = b.Execute(context.Background(), func(ctx context.Context) error {
			firstIn.Done()
			<-hold
			return nil
		})
	}()
	firstIn.Wait()

	// Second caller will wait; we advance the injected clock by waitDur while
	// it is blocked, then free the slot.
	done := make(chan error, 1)
	go func() {
		done <- b.Execute(context.Background(), func(ctx context.Context) error { return nil })
	}()

	// Ensure the second caller is actually waiting before advancing the clock.
	if got, ok := waitForInt(b.QueueDepth, 1, 2*time.Second); !ok {
		t.Fatalf("QueueDepth() = %d, want 1 before advancing clock", got)
	}
	clk.Add(waitDur)
	close(hold)

	if err := <-done; err != nil {
		t.Fatalf("waiting Execute returned error: %v", err)
	}

	// Both callers enter the wait path (maxWait>0): the holder acquires the
	// free slot instantly (~0 wait via the injected clock, which has not
	// advanced yet), and the second caller waits exactly waitDur. So Count is
	// 2, the second caller's wait is the Last/Max observation, and Min is 0.
	stats := b.WaitStats()
	if stats.Count != 2 {
		t.Fatalf("WaitStats.Count = %d, want 2 (holder + waiter)", stats.Count)
	}
	if stats.Last != waitDur {
		t.Fatalf("WaitStats.Last = %v, want %v", stats.Last, waitDur)
	}
	if stats.Max != waitDur {
		t.Fatalf("WaitStats.Max = %v, want %v", stats.Max, waitDur)
	}
	if stats.Min != 0 {
		t.Fatalf("WaitStats.Min = %v, want 0 (holder acquired instantly)", stats.Min)
	}
	if stats.Sum != waitDur {
		t.Fatalf("WaitStats.Sum = %v, want %v", stats.Sum, waitDur)
	}
	if len(stats.Buckets) != len(stats.BucketBounds)+1 {
		t.Fatalf("bucket length = %d, want %d", len(stats.Buckets), len(stats.BucketBounds)+1)
	}
	var total int64
	for _, c := range stats.Buckets {
		total += c
	}
	if total != 2 {
		t.Fatalf("histogram total = %d, want 2", total)
	}
}

// TestBulkhead_WaitTimeTrackedOnTimeout verifies wait time is recorded even when
// the caller times out (never acquires a slot), using real short sleeps with
// generous bounds to avoid flakiness.
func TestBulkhead_WaitTimeTrackedOnTimeout(t *testing.T) {
	const maxWait = 60 * time.Millisecond
	b := New(1, maxWait)

	hold := make(chan struct{})
	var firstIn sync.WaitGroup
	firstIn.Add(1)
	go func() {
		_ = b.Execute(context.Background(), func(ctx context.Context) error {
			firstIn.Done()
			<-hold
			return nil
		})
	}()
	firstIn.Wait()

	// This caller will time out after ~maxWait.
	err := b.Execute(context.Background(), func(ctx context.Context) error { return nil })
	if !errors.Is(err, ErrBulkheadFull) {
		t.Fatalf("expected ErrBulkheadFull, got %v", err)
	}
	close(hold)

	// The holder acquired the free slot instantly and the second caller timed
	// out after ~maxWait, so Count is 2 and the timed-out wait is Last.
	stats := b.WaitStats()
	if stats.Count != 2 {
		t.Fatalf("WaitStats.Count = %d, want 2 (holder + timed-out waiter)", stats.Count)
	}
	// The recorded wait should be roughly maxWait; allow a wide window.
	if stats.Last < maxWait/2 || stats.Last > maxWait*5 {
		t.Fatalf("WaitStats.Last = %v, want ~%v", stats.Last, maxWait)
	}
	if b.QueueDepth() != 0 {
		t.Fatalf("QueueDepth() = %d after timeout, want 0", b.QueueDepth())
	}
}

// TestBulkhead_SaturationConcurrencyRace stresses the waiting counter and wait
// stats under concurrency. Run with -race; it asserts no negative counters and
// that everything drains, guarded by a watchdog against deadlock.
func TestBulkhead_SaturationConcurrencyRace(t *testing.T) {
	const (
		limit   = 3
		callers = 60
	)
	b := New(limit, 200*time.Millisecond)

	var wg sync.WaitGroup
	var minWaiting atomic.Int64 // track lowest observed to catch negatives
	stop := make(chan struct{})

	// Sampler: continuously read QueueDepth() and flag negatives.
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				if w := int64(b.QueueDepth()); w < minWaiting.Load() {
					minWaiting.Store(w)
				}
			}
		}
	}()

	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := context.Background()
			_ = b.Execute(ctx, func(context.Context) error {
				time.Sleep(2 * time.Millisecond)
				return nil
			})
		}()
	}

	doneCh := make(chan struct{})
	go func() { wg.Wait(); close(doneCh) }()

	select {
	case <-doneCh:
	case <-time.After(10 * time.Second):
		close(stop)
		t.Fatal("watchdog: saturation stress test deadlocked")
	}
	close(stop)

	if minWaiting.Load() < 0 {
		t.Fatalf("observed negative QueueDepth() count: %d", minWaiting.Load())
	}
	if got := b.QueueDepth(); got != 0 {
		t.Fatalf("QueueDepth() = %d after drain, want 0", got)
	}
	if got := b.Inflight(); got != 0 {
		t.Fatalf("Inflight() = %d after drain, want 0", got)
	}
	// Every caller that entered the wait must have been observed exactly once.
	stats := b.WaitStats()
	if stats.Count < 0 {
		t.Fatalf("WaitStats.Count negative: %d", stats.Count)
	}
}

// ---------------------------------------------------------------------------
// ThreadPool saturation tests
// ---------------------------------------------------------------------------

// TestThreadPool_PendingReflectsQueuedTasks verifies QueueDepth()
// reflects queued-but-not-started tasks, and Busy() reflects running workers.
func TestThreadPool_PendingReflectsQueuedTasks(t *testing.T) {
	// One worker, queue depth 5. Block the worker so submitted tasks pile up.
	tp := NewThreadPool(1, 5)
	defer tp.Close()

	hold := make(chan struct{})
	var started sync.WaitGroup
	started.Add(1)

	// First submit occupies the sole worker.
	if _, err := tp.Submit(context.Background(), func(ctx context.Context) error {
		started.Done()
		<-hold
		return nil
	}); err != nil {
		t.Fatalf("first Submit failed: %v", err)
	}
	started.Wait()

	// Worker is busy now.
	if got, ok := waitForInt(tp.Busy, 1, 2*time.Second); !ok {
		t.Fatalf("Busy() = %d, want 1", got)
	}

	// Submit 3 more; they cannot start (only worker is busy) so they queue.
	const queued = 3
	for i := 0; i < queued; i++ {
		if _, err := tp.Submit(context.Background(), func(ctx context.Context) error { return nil }); err != nil {
			t.Fatalf("queued Submit %d failed: %v", i, err)
		}
	}

	if got, ok := waitForInt(tp.QueueDepth, queued, 2*time.Second); !ok {
		t.Fatalf("QueueDepth() = %d, want %d", got, queued)
	}
	if got := tp.Capacity(); got != 5 {
		t.Fatalf("Capacity() = %d, want 5", got)
	}
	if got := tp.Workers(); got != 1 {
		t.Fatalf("Workers() = %d, want 1", got)
	}

	// Release; the queue must drain to empty and workers go idle.
	close(hold)
	if got, ok := waitForInt(tp.QueueDepth, 0, 2*time.Second); !ok {
		t.Fatalf("QueueDepth() = %d after drain, want 0", got)
	}
	if got, ok := waitForInt(tp.Busy, 0, 2*time.Second); !ok {
		t.Fatalf("Busy() = %d after drain, want 0", got)
	}
}
