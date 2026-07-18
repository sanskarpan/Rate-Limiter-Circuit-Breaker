package debounce

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
)

var epoch = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

// recorder counts invocations and lets a test wait until the count reaches a
// target, since ManualClock.AfterFunc fires callbacks on a separate goroutine.
type recorder struct {
	mu    sync.Mutex
	cond  *sync.Cond
	count int
	last  int // value passed to the most recent invocation
}

func newRecorder() *recorder {
	r := &recorder{}
	r.cond = sync.NewCond(&r.mu)
	return r
}

// fn returns a work function tagged with id; the id of the last-run function is
// recorded so trailing-edge "latest wins" behaviour can be asserted.
func (r *recorder) fn(id int) func() {
	return func() {
		r.mu.Lock()
		r.count++
		r.last = id
		r.cond.Broadcast()
		r.mu.Unlock()
	}
}

// waitFor blocks until count >= n or the deadline elapses, returning false on
// timeout. It uses real wall-clock time only as a test-safety net.
func (r *recorder) waitFor(t *testing.T, n int) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		r.mu.Lock()
		for r.count < n {
			r.cond.Wait()
		}
		r.mu.Unlock()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		r.mu.Lock()
		got := r.count
		r.mu.Unlock()
		t.Fatalf("timed out waiting for %d invocations, got %d", n, got)
	}
}

func (r *recorder) counts() (int, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.count, r.last
}

// expectCount asserts the count equals want, after allowing pending callbacks a
// brief window to (not) arrive. Used to assert that nothing fired.
func (r *recorder) expectCount(t *testing.T, want int) {
	t.Helper()
	r.mu.Lock()
	got := r.count
	r.mu.Unlock()
	if got != want {
		t.Fatalf("count = %d, want %d", got, want)
	}
}

func TestDebounce_TrailingCoalescesBurst(t *testing.T) {
	clk := clock.NewManualClock(epoch)
	r := newRecorder()
	d := New(100*time.Millisecond, WithClock(clk))
	defer d.Stop()

	// Five rapid calls, each within the delay window: only the last should run.
	for i := 1; i <= 5; i++ {
		d.Trigger(r.fn(i))
		clk.Advance(10 * time.Millisecond) // well under the 100ms delay
	}
	r.expectCount(t, 0) // nothing fires until the quiet period elapses

	clk.Advance(100 * time.Millisecond)
	r.waitFor(t, 1)

	count, last := r.counts()
	if count != 1 {
		t.Fatalf("count = %d, want 1 (burst should coalesce)", count)
	}
	if last != 5 {
		t.Fatalf("last id = %d, want 5 (latest call should win)", last)
	}
}

func TestDebounce_SeparateBurstsEachFire(t *testing.T) {
	clk := clock.NewManualClock(epoch)
	r := newRecorder()
	d := New(50*time.Millisecond, WithClock(clk))
	defer d.Stop()

	d.Trigger(r.fn(1))
	clk.Advance(50 * time.Millisecond)
	r.waitFor(t, 1)

	// After the quiet period, a new call starts a fresh burst.
	d.Trigger(r.fn(2))
	clk.Advance(50 * time.Millisecond)
	r.waitFor(t, 2)

	count, last := r.counts()
	if count != 2 {
		t.Fatalf("count = %d, want 2", count)
	}
	if last != 2 {
		t.Fatalf("last id = %d, want 2", last)
	}
}

func TestDebounce_Leading(t *testing.T) {
	clk := clock.NewManualClock(epoch)
	r := newRecorder()
	d := New(100*time.Millisecond, WithClock(clk), WithLeading())
	defer d.Stop()

	// First call fires immediately on the leading edge.
	d.Trigger(r.fn(1))
	r.waitFor(t, 1)
	if c, _ := r.counts(); c != 1 {
		t.Fatalf("after leading call, count = %d, want 1", c)
	}

	// A second call in the same burst schedules a trailing call.
	clk.Advance(10 * time.Millisecond)
	d.Trigger(r.fn(2))
	clk.Advance(100 * time.Millisecond)
	r.waitFor(t, 2)

	count, last := r.counts()
	if count != 2 {
		t.Fatalf("count = %d, want 2 (leading + trailing)", count)
	}
	if last != 2 {
		t.Fatalf("last id = %d, want 2", last)
	}
}

func TestDebounce_LeadingLoneCallFiresOnce(t *testing.T) {
	clk := clock.NewManualClock(epoch)
	r := newRecorder()
	d := New(100*time.Millisecond, WithClock(clk), WithLeading())
	defer d.Stop()

	// A single call followed by silence should fire exactly once (leading only),
	// not a duplicate trailing call.
	d.Trigger(r.fn(1))
	r.waitFor(t, 1)
	clk.Advance(200 * time.Millisecond)
	// Give any erroneous trailing callback a chance to run.
	time.Sleep(20 * time.Millisecond)

	r.expectCount(t, 1)
}

func TestDebounce_MaxWait(t *testing.T) {
	clk := clock.NewManualClock(epoch)
	r := newRecorder()
	// delay 50ms, maxWait 120ms: a sustained stream of calls every 30ms would
	// keep resetting the 50ms trailing timer forever, but maxWait forces a run.
	d := New(50*time.Millisecond, WithClock(clk), WithMaxWait(120*time.Millisecond))
	defer d.Stop()

	id := 0
	// 5 calls at 30ms spacing = 120ms of sustained activity.
	for i := 0; i < 5; i++ {
		id++
		d.Trigger(r.fn(id))
		clk.Advance(30 * time.Millisecond)
	}
	// By now 150ms elapsed; maxWait (120ms) must have forced exactly one run.
	r.waitFor(t, 1)
	count, _ := r.counts()
	if count != 1 {
		t.Fatalf("count = %d, want 1 (maxWait should force one run mid-burst)", count)
	}
}

func TestDebounce_MaxWaitClampedToDelay(t *testing.T) {
	// maxWait < delay is clamped up to delay.
	d := New(100*time.Millisecond, WithMaxWait(10*time.Millisecond))
	if d.maxWait != d.delay {
		t.Fatalf("maxWait = %v, want clamped to delay %v", d.maxWait, d.delay)
	}
}

func TestDebounce_Stop(t *testing.T) {
	clk := clock.NewManualClock(epoch)
	r := newRecorder()
	d := New(50*time.Millisecond, WithClock(clk))

	d.Trigger(r.fn(1))
	d.Stop()
	clk.Advance(100 * time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	r.expectCount(t, 0) // pending call was cancelled

	// Trigger after Stop is a no-op.
	d.Trigger(r.fn(2))
	clk.Advance(100 * time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	r.expectCount(t, 0)

	// Stop is idempotent.
	d.Stop()
}

func TestDebounce_NilFnIgnored(t *testing.T) {
	clk := clock.NewManualClock(epoch)
	d := New(50*time.Millisecond, WithClock(clk))
	defer d.Stop()
	d.Trigger(nil) // must not panic or schedule anything
	clk.Advance(100 * time.Millisecond)
}

func TestNew_PanicsOnNonPositiveDelay(t *testing.T) {
	tests := []time.Duration{0, -1 * time.Second}
	for _, delay := range tests {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("New(%v) did not panic", delay)
				}
			}()
			New(delay)
		}()
	}
}

func TestWithClock_NilIgnored(t *testing.T) {
	d := New(50*time.Millisecond, WithClock(nil))
	if _, ok := d.clk.(clock.RealClock); !ok {
		t.Fatalf("nil clock should be ignored, leaving RealClock; got %T", d.clk)
	}
}

// TestDebounce_ConcurrentTriggers is a -race-sensitive test: many goroutines
// hammer Trigger while the clock advances. It asserts no data races and that
// the debouncer eventually fires.
func TestDebounce_ConcurrentTriggers(t *testing.T) {
	clk := clock.NewManualClock(epoch)
	var fired int64
	d := New(20*time.Millisecond, WithClock(clk))
	defer d.Stop()

	work := func() { atomic.AddInt64(&fired, 1) }

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				d.Trigger(work)
			}
		}()
	}
	// Concurrently advance the clock to fire timers.
	stop := make(chan struct{})
	var adv sync.WaitGroup
	adv.Add(1)
	go func() {
		defer adv.Done()
		for {
			select {
			case <-stop:
				return
			default:
				clk.Advance(5 * time.Millisecond)
			}
		}
	}()

	wg.Wait()
	// Let the clock advance well past the delay to flush any pending call.
	for i := 0; i < 10; i++ {
		clk.Advance(20 * time.Millisecond)
	}
	close(stop)
	adv.Wait()

	// Drain one final quiet period deterministically.
	clk.Advance(20 * time.Millisecond)
	time.Sleep(50 * time.Millisecond)

	if atomic.LoadInt64(&fired) == 0 {
		t.Fatal("expected at least one invocation under concurrent load")
	}
}
