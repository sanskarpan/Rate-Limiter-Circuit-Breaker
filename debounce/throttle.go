package debounce

import (
	"fmt"
	"sync"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
)

// Throttler ensures a function runs at most once per interval. The first call
// (the leading edge) runs immediately; further calls within the same interval
// are coalesced. When trailing is enabled (the default), a single trailing call
// runs at the end of the interval using the most recently supplied function, so
// the latest request in a burst is not lost.
//
// The zero value is not usable; construct a Throttler with NewThrottler. All
// methods are safe for concurrent use.
type Throttler struct {
	interval time.Duration
	trailing bool
	clk      clock.Clock

	mu       sync.Mutex
	timer    clock.Timer // interval timer, non-nil while cooling down
	pending  func()      // most recent function awaiting the trailing edge
	hasTrail bool        // a trailing call is queued
	stopped  bool
}

// ThrottleOption configures a Throttler.
type ThrottleOption func(*Throttler)

// WithThrottleClock sets a custom clock for deterministic testing. Defaults to
// clock.RealClock. A nil clock is ignored.
func WithThrottleClock(c clock.Clock) ThrottleOption {
	return func(t *Throttler) {
		if c != nil {
			t.clk = c
		}
	}
}

// WithoutTrailing disables the trailing-edge call. With trailing disabled, calls
// that arrive during an interval after the leading call are dropped entirely
// (leading-only throttling). Trailing is enabled by default.
func WithoutTrailing() ThrottleOption {
	return func(t *Throttler) { t.trailing = false }
}

// NewThrottler creates a Throttler that runs the wrapped work at most once per
// interval.
//
// It panics if interval <= 0, mirroring the standard library convention (e.g.
// time.NewTicker panics on a non-positive interval).
func NewThrottler(interval time.Duration, opts ...ThrottleOption) *Throttler {
	if interval <= 0 {
		panic(fmt.Sprintf("debounce.NewThrottler: interval must be > 0, got %v", interval))
	}
	t := &Throttler{
		interval: interval,
		trailing: true,
		clk:      clock.RealClock{},
	}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

// Trigger requests that fn run, subject to the throttle. If no invocation has
// happened in the current interval, fn runs immediately (leading edge) and the
// interval begins. Otherwise fn is recorded as the pending trailing call
// (overwriting any earlier pending call) and, if trailing is enabled, runs once
// when the interval elapses. fn runs on a separate goroutine, so Trigger never
// blocks on the work. A nil fn is ignored. After Stop, Trigger is a no-op.
func (t *Throttler) Trigger(fn func()) {
	if fn == nil {
		return
	}
	t.mu.Lock()
	if t.stopped {
		t.mu.Unlock()
		return
	}

	var runNow func()
	if t.timer == nil {
		// Not cooling down: fire the leading edge and start the interval.
		runNow = fn
		t.timer = t.clk.AfterFunc(t.interval, t.onInterval)
	} else if t.trailing {
		// Cooling down: record the latest call for the trailing edge.
		t.pending = fn
		t.hasTrail = true
	}
	t.mu.Unlock()

	if runNow != nil {
		go runNow()
	}
}

// Do is an alias for Trigger.
func (t *Throttler) Do(fn func()) { t.Trigger(fn) }

// onInterval fires when an interval elapses.
func (t *Throttler) onInterval() {
	t.mu.Lock()
	if t.stopped {
		t.mu.Unlock()
		return
	}
	if t.hasTrail {
		// Run the trailing call and open a fresh interval so a call arriving
		// during this trailing run is itself throttled rather than firing
		// immediately.
		fn := t.pending
		t.pending = nil
		t.hasTrail = false
		t.timer = t.clk.AfterFunc(t.interval, t.onInterval)
		t.mu.Unlock()
		if fn != nil {
			fn()
		}
		return
	}
	// No trailing call queued: the interval closes and the next Trigger fires
	// immediately on the leading edge.
	t.timer = nil
	t.mu.Unlock()
}

// Stop cancels any pending trailing invocation and disables the Throttler.
// Subsequent Trigger/Do calls are no-ops. Stop is idempotent and safe for
// concurrent use. It does not wait for an already-running invocation to finish.
func (t *Throttler) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.stopped = true
	t.pending = nil
	t.hasTrail = false
	if t.timer != nil {
		t.timer.Stop()
		t.timer = nil
	}
}
