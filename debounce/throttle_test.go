package debounce

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
)

func TestThrottle_LeadingFiresImmediately(t *testing.T) {
	clk := clock.NewManualClock(epoch)
	r := newRecorder()
	th := NewThrottler(100*time.Millisecond, WithThrottleClock(clk))
	defer th.Stop()

	th.Trigger(r.fn(1))
	r.waitFor(t, 1)

	count, last := r.counts()
	if count != 1 || last != 1 {
		t.Fatalf("leading edge: count=%d last=%d, want 1/1", count, last)
	}
}

func TestThrottle_CoalescesWithTrailing(t *testing.T) {
	clk := clock.NewManualClock(epoch)
	r := newRecorder()
	th := NewThrottler(100*time.Millisecond, WithThrottleClock(clk))
	defer th.Stop()

	// Leading call runs immediately (id 1).
	th.Trigger(r.fn(1))
	r.waitFor(t, 1)

	// Several calls within the interval are coalesced into one trailing call
	// carrying the latest id.
	clk.Advance(20 * time.Millisecond)
	th.Trigger(r.fn(2))
	clk.Advance(20 * time.Millisecond)
	th.Trigger(r.fn(3))
	r.expectCount(t, 1) // still only the leading call so far

	// Close the interval: the trailing call (id 3) runs.
	clk.Advance(100 * time.Millisecond)
	r.waitFor(t, 2)

	count, last := r.counts()
	if count != 2 {
		t.Fatalf("count = %d, want 2 (leading + trailing)", count)
	}
	if last != 3 {
		t.Fatalf("trailing id = %d, want 3 (latest wins)", last)
	}
}

func TestThrottle_RateCap(t *testing.T) {
	clk := clock.NewManualClock(epoch)
	r := newRecorder()
	th := NewThrottler(100*time.Millisecond, WithThrottleClock(clk))
	defer th.Stop()

	// A dense stream over 350ms. With a 100ms interval and trailing enabled, we
	// expect a leading call plus a trailing call per closed interval — not one
	// per Trigger.
	for i := 0; i < 35; i++ {
		th.Trigger(r.fn(i))
		clk.Advance(10 * time.Millisecond)
	}
	// Flush the final trailing interval.
	clk.Advance(100 * time.Millisecond)
	time.Sleep(50 * time.Millisecond)

	count, _ := r.counts()
	// 350ms of activity at one-per-100ms is roughly 4-5 runs; assert it's well
	// under the 35 Triggers, confirming throttling.
	if count == 0 || count > 6 {
		t.Fatalf("count = %d, want a small number (throttled), not ~35", count)
	}
}

func TestThrottle_WithoutTrailing(t *testing.T) {
	clk := clock.NewManualClock(epoch)
	r := newRecorder()
	th := NewThrottler(100*time.Millisecond, WithThrottleClock(clk), WithoutTrailing())
	defer th.Stop()

	th.Trigger(r.fn(1)) // leading, runs
	r.waitFor(t, 1)

	// Calls during the interval are dropped (no trailing).
	clk.Advance(20 * time.Millisecond)
	th.Trigger(r.fn(2))
	clk.Advance(100 * time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	r.expectCount(t, 1)

	// After the interval closes, a new call fires on the leading edge again.
	th.Trigger(r.fn(3))
	r.waitFor(t, 2)
	count, last := r.counts()
	if count != 2 || last != 3 {
		t.Fatalf("count=%d last=%d, want 2/3", count, last)
	}
}

func TestThrottle_Stop(t *testing.T) {
	clk := clock.NewManualClock(epoch)
	r := newRecorder()
	th := NewThrottler(100*time.Millisecond, WithThrottleClock(clk))

	th.Trigger(r.fn(1)) // leading fires
	r.waitFor(t, 1)
	th.Trigger(r.fn(2)) // queued trailing
	th.Stop()           // cancels the trailing call

	clk.Advance(200 * time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	r.expectCount(t, 1) // trailing was cancelled

	// No-op after Stop.
	th.Trigger(r.fn(3))
	r.expectCount(t, 1)
	th.Stop() // idempotent
}

func TestThrottle_NilFnIgnored(t *testing.T) {
	clk := clock.NewManualClock(epoch)
	th := NewThrottler(50*time.Millisecond, WithThrottleClock(clk))
	defer th.Stop()
	th.Trigger(nil)
	clk.Advance(100 * time.Millisecond)
}

func TestNewThrottler_PanicsOnNonPositiveInterval(t *testing.T) {
	for _, iv := range []time.Duration{0, -time.Second} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("NewThrottler(%v) did not panic", iv)
				}
			}()
			NewThrottler(iv)
		}()
	}
}

func TestThrottle_DoAlias(t *testing.T) {
	clk := clock.NewManualClock(epoch)
	r := newRecorder()
	th := NewThrottler(50*time.Millisecond, WithThrottleClock(clk))
	defer th.Stop()
	th.Do(r.fn(1))
	r.waitFor(t, 1)
}

// TestThrottle_Concurrent is a -race-sensitive test.
func TestThrottle_Concurrent(t *testing.T) {
	clk := clock.NewManualClock(epoch)
	var fired int64
	th := NewThrottler(20*time.Millisecond, WithThrottleClock(clk))
	defer th.Stop()

	work := func() { atomic.AddInt64(&fired, 1) }

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				th.Trigger(work)
			}
		}()
	}
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
	for i := 0; i < 10; i++ {
		clk.Advance(20 * time.Millisecond)
	}
	close(stop)
	adv.Wait()
	clk.Advance(20 * time.Millisecond)
	time.Sleep(50 * time.Millisecond)

	if atomic.LoadInt64(&fired) == 0 {
		t.Fatal("expected at least one invocation under concurrent load")
	}
}
