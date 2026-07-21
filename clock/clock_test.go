package clock_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/clock"
)

func TestManualClock_Now(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	c := clock.NewManualClock(start)
	if !c.Now().Equal(start) {
		t.Fatalf("expected %v, got %v", start, c.Now())
	}
}

func TestManualClock_Advance(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	c := clock.NewManualClock(start)
	c.Advance(5 * time.Second)
	expected := start.Add(5 * time.Second)
	if !c.Now().Equal(expected) {
		t.Fatalf("expected %v, got %v", expected, c.Now())
	}
}

func TestManualClock_Timer_FiresOnAdvance(t *testing.T) {
	c := clock.NewManualClock(time.Now())
	timer := c.NewTimer(100 * time.Millisecond)
	var fired atomic.Bool

	go func() {
		<-timer.C()
		fired.Store(true)
	}()

	c.Advance(100 * time.Millisecond)
	// Give the goroutine a moment to process
	deadline := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(deadline) {
		if fired.Load() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	if !fired.Load() {
		t.Fatal("timer should have fired")
	}
}

func TestManualClock_Timer_FiresInOrder(t *testing.T) {
	c := clock.NewManualClock(time.Now())

	// Create timers in order: 100ms, 200ms, 300ms
	t100 := c.NewTimer(100 * time.Millisecond)
	t200 := c.NewTimer(200 * time.Millisecond)
	t300 := c.NewTimer(300 * time.Millisecond)

	c.Advance(300 * time.Millisecond)
	time.Sleep(20 * time.Millisecond)

	// Verify channels received values
	select {
	case <-t100.C():
	default:
		t.Error("100ms timer should have fired")
	}
	select {
	case <-t200.C():
	default:
		t.Error("200ms timer should have fired")
	}
	select {
	case <-t300.C():
	default:
		t.Error("300ms timer should have fired")
	}
}

func TestManualClock_Sleep(t *testing.T) {
	c := clock.NewManualClock(time.Now())
	done := make(chan struct{})
	go func() {
		c.Sleep(500 * time.Millisecond)
		close(done)
	}()

	// Give goroutine time to start sleeping
	time.Sleep(5 * time.Millisecond)

	// Not done yet
	select {
	case <-done:
		t.Fatal("sleep should not have completed before Advance")
	default:
	}

	c.Advance(500 * time.Millisecond)
	select {
	case <-done:
		// OK
	case <-time.After(200 * time.Millisecond):
		t.Fatal("sleep should have completed after advance")
	}
}

func TestManualClock_Ticker(t *testing.T) {
	c := clock.NewManualClock(time.Now())
	ticker := c.NewTicker(100 * time.Millisecond)
	var count atomic.Int32

	go func() {
		for range ticker.C() {
			count.Add(1)
		}
	}()

	// Advance 5 ticks
	for i := 0; i < 5; i++ {
		c.Advance(100 * time.Millisecond)
		time.Sleep(5 * time.Millisecond) // let goroutine process
	}
	ticker.Stop()

	time.Sleep(10 * time.Millisecond)
	got := count.Load()
	if got < 4 || got > 5 { // allow slight race on last tick
		t.Errorf("expected 4-5 ticks, got %d", got)
	}
}

func TestManualClock_Timer_Stop(t *testing.T) {
	c := clock.NewManualClock(time.Now())
	timer := c.NewTimer(100 * time.Millisecond)
	if !timer.Stop() {
		t.Fatal("timer.Stop() should return true for active timer")
	}
	c.Advance(100 * time.Millisecond)
	select {
	case <-timer.C():
		t.Fatal("stopped timer should not fire")
	default:
	}
}

// TestManualClock_AdvanceResetNoDeadlock is a regression test for H-15:
// a lock-order inversion between Advance (c.mu -> tick.mu) and
// manualTicker.Reset (tick.mu -> c.mu) could deadlock. Two goroutines hammer
// Advance and Reset concurrently; a watchdog fails the test if they hang.
func TestManualClock_AdvanceResetNoDeadlock(t *testing.T) {
	c := clock.NewManualClock(time.Now())
	ticker := c.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	// Drain the ticker channel so sends never block.
	go func() {
		for range ticker.C() {
		}
	}()

	const iters = 5000
	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				c.Advance(10 * time.Millisecond)
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				ticker.Reset(10 * time.Millisecond)
			}
		}()
		wg.Wait()
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Error("deadlock: Advance/Reset did not complete within 5s")
	}
}

// TestManualClock_Ticker_NoDroppedTicks is a regression test for M-17(a):
// advancing N intervals on a drained ticker must deliver N ticks, not 1.
func TestManualClock_Ticker_NoDroppedTicks(t *testing.T) {
	c := clock.NewManualClock(time.Now())
	ticker := c.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	var count atomic.Int32
	go func() {
		for range ticker.C() {
			count.Add(1)
		}
	}()

	// Single advance spanning 5 intervals should yield 5 ticks.
	c.Advance(5 * 100 * time.Millisecond)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if count.Load() >= 5 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if got := count.Load(); got != 5 {
		t.Errorf("expected 5 ticks from 5x interval advance, got %d", got)
	}
}

// TestManualClock_Ticker_ResetAfterStop is a regression test for M-17(b):
// Reset after Stop must re-register the ticker so it fires again.
func TestManualClock_Ticker_ResetAfterStop(t *testing.T) {
	c := clock.NewManualClock(time.Now())
	ticker := c.NewTicker(100 * time.Millisecond)

	ticker.Stop()
	ticker.Reset(100 * time.Millisecond)

	var fired atomic.Bool
	go func() {
		<-ticker.C()
		fired.Store(true)
	}()

	c.Advance(100 * time.Millisecond)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if fired.Load() {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !fired.Load() {
		t.Error("ticker should fire after Reset following Stop")
	}
	ticker.Stop()
}

func TestRealClock_Now(t *testing.T) {
	c := clock.RealClock{}
	before := time.Now()
	got := c.Now()
	after := time.Now()
	if got.Before(before) || got.After(after) {
		t.Fatalf("RealClock.Now() not in expected range")
	}
}

func TestRealClock_Since(t *testing.T) {
	c := clock.RealClock{}
	past := time.Now().Add(-time.Second)
	elapsed := c.Since(past)
	if elapsed < time.Second || elapsed > 2*time.Second {
		t.Errorf("Since() returned unexpected value: %v", elapsed)
	}
}

// TestManualClock_Advance_NoDrainerNoDeadlock is a regression test for
// CB-CLOCK-1: Advance spanning multiple ticker intervals must NOT block when
// no consumer is draining the ticker channel (previously a blocking send
// deadlocked). Guarded by a watchdog.
func TestManualClock_Advance_NoDrainerNoDeadlock(t *testing.T) {
	c := clock.NewManualClock(time.Now())
	tk := c.NewTicker(10 * time.Millisecond)
	defer tk.Stop()

	done := make(chan struct{})
	go func() {
		// No one drains tk.C(); advancing 50 intervals must still return.
		c.Advance(50 * 10 * time.Millisecond)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Advance deadlocked with no ticker drainer (CB-CLOCK-1)")
	}
}
