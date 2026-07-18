package debounce

import (
	"fmt"
	"sync"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
)

// Debouncer coalesces a burst of rapid Trigger/Do calls into a single
// invocation of a function. By default it fires on the trailing edge: the most
// recently supplied function runs once the caller has been quiet for the
// configured delay. See the package documentation for leading-edge and
// max-wait behaviour.
//
// The zero value is not usable; construct a Debouncer with New. All methods are
// safe for concurrent use.
type Debouncer struct {
	delay   time.Duration
	maxWait time.Duration // 0 means disabled
	leading bool
	clk     clock.Clock

	mu          sync.Mutex
	timer       clock.Timer // trailing-edge timer, nil when idle
	maxTimer    clock.Timer // max-wait timer, nil when disabled/idle
	pending     func()      // most recent function awaiting trailing invocation
	leadingDone bool        // leading edge already fired for the current burst
	callCount   int         // number of Trigger calls in the current burst
	stopped     bool
}

// Option configures a Debouncer.
type Option func(*Debouncer)

// WithClock sets a custom clock for deterministic testing. Defaults to
// clock.RealClock. A nil clock is ignored.
func WithClock(c clock.Clock) Option {
	return func(d *Debouncer) {
		if c != nil {
			d.clk = c
		}
	}
}

// WithLeading enables leading-edge firing: the first call of a burst runs its
// function immediately, in addition to the trailing-edge call at the end of the
// quiet period. When only a single call occurs, the trailing call still runs
// (mirroring Lodash's default leading+trailing behaviour) unless it is the same
// pending function that already fired on the leading edge — a lone leading call
// followed by silence fires exactly once. Disabled by default.
func WithLeading() Option {
	return func(d *Debouncer) { d.leading = true }
}

// WithMaxWait guarantees the pending function runs at least once every maxWait,
// even under a sustained stream of calls that keeps resetting the trailing
// timer. A non-positive maxWait disables the guarantee (the default). If maxWait
// is smaller than the debounce delay it is treated as equal to the delay.
func WithMaxWait(maxWait time.Duration) Option {
	return func(d *Debouncer) {
		if maxWait > 0 {
			d.maxWait = maxWait
		}
	}
}

// New creates a Debouncer with the given quiet-period delay.
//
// It panics if delay <= 0: a debouncer with no delay has no burst to coalesce.
// This mirrors the standard library convention (e.g. time.NewTicker panics on a
// non-positive interval).
func New(delay time.Duration, opts ...Option) *Debouncer {
	if delay <= 0 {
		panic(fmt.Sprintf("debounce.New: delay must be > 0, got %v", delay))
	}
	d := &Debouncer{
		delay: delay,
		clk:   clock.RealClock{},
	}
	for _, opt := range opts {
		opt(d)
	}
	if d.maxWait > 0 && d.maxWait < d.delay {
		d.maxWait = d.delay
	}
	return d
}

// Trigger schedules fn to run after the quiet period, coalescing with any
// earlier pending call in the current burst. Only the most recently supplied fn
// runs on the trailing edge. If leading-edge firing is enabled, the first fn of
// a burst also runs immediately. fn runs on a separate goroutine, so Trigger
// never blocks on the work. A nil fn is ignored. After Stop, Trigger is a no-op.
func (d *Debouncer) Trigger(fn func()) {
	if fn == nil {
		return
	}
	d.mu.Lock()
	if d.stopped {
		d.mu.Unlock()
		return
	}

	var runNow func()

	if d.timer == nil {
		// Start of a new burst.
		d.callCount = 0
		d.leadingDone = false
		if d.leading {
			runNow = fn
			d.leadingDone = true
		}
		if d.maxWait > 0 {
			d.maxTimer = d.clk.AfterFunc(d.maxWait, d.onMaxWait)
		}
	}

	d.callCount++
	d.pending = fn

	// (Re)arm the trailing-edge timer.
	if d.timer == nil {
		d.timer = d.clk.AfterFunc(d.delay, d.onTrailing)
	} else {
		d.timer.Reset(d.delay)
	}
	d.mu.Unlock()

	if runNow != nil {
		go runNow()
	}
}

// Do is an alias for Trigger.
func (d *Debouncer) Do(fn func()) { d.Trigger(fn) }

// onTrailing fires when the quiet period elapses.
func (d *Debouncer) onTrailing() {
	d.mu.Lock()
	fn := d.takeAndResetLocked()
	d.mu.Unlock()
	if fn != nil {
		fn()
	}
}

// onMaxWait fires when the max-wait interval elapses during a sustained burst.
func (d *Debouncer) onMaxWait() {
	d.mu.Lock()
	// If the trailing timer already fired (burst ended), nothing to do.
	if d.timer == nil {
		d.mu.Unlock()
		return
	}
	d.timer.Stop()
	fn := d.takeAndResetLocked()
	d.mu.Unlock()
	if fn != nil {
		fn()
	}
}

// takeAndResetLocked ends the current burst, returning the function to run (or
// nil if none should run) and clearing all per-burst state. Must hold d.mu.
func (d *Debouncer) takeAndResetLocked() func() {
	fn := d.pending
	// If leading already fired this burst and no newer call arrived after it,
	// the trailing call would be a duplicate of the leading one — suppress it so
	// a lone leading call fires exactly once.
	if d.leadingDone && d.leadingSuppressesTrailing() {
		fn = nil
	}
	d.pending = nil
	d.timer = nil
	if d.maxTimer != nil {
		d.maxTimer.Stop()
		d.maxTimer = nil
	}
	d.leadingDone = false
	return fn
}

// leadingSuppressesTrailing reports whether the trailing call should be
// suppressed because it would duplicate the leading call. This is true only for
// the degenerate single-call burst: leading fired and exactly one Trigger
// occurred, so a trailing call would re-run the same lone function.
func (d *Debouncer) leadingSuppressesTrailing() bool {
	return d.callCount <= 1
}

// Stop cancels any pending invocation and disables the Debouncer. Subsequent
// Trigger/Do calls are no-ops. Stop is idempotent and safe for concurrent use.
// It does not wait for an already-running invocation to finish.
func (d *Debouncer) Stop() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.stopped = true
	d.pending = nil
	if d.timer != nil {
		d.timer.Stop()
		d.timer = nil
	}
	if d.maxTimer != nil {
		d.maxTimer.Stop()
		d.maxTimer = nil
	}
}
