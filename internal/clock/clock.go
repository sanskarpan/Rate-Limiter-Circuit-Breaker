// Package clock provides a mockable time source for deterministic testing.
// All time-dependent components accept a Clock interface rather than calling
// time.Now() directly. This is the most important architectural decision
// in this library — it makes every algorithm fully testable without time.Sleep.
package clock

import (
	"sort"
	"sync"
	"time"
)

// tickerChanBuffer is the buffer depth of a ManualClock ticker channel. It lets
// a single Advance spanning many intervals deposit one tick per interval
// without blocking, while still coalescing (dropping) once a consumer falls
// this far behind — matching real time.Ticker behaviour.
const tickerChanBuffer = 1024

// Clock is the time source interface used by all rate limiting algorithms.
// Use RealClock in production and ManualClock in tests.
type Clock interface {
	Now() time.Time
	Sleep(d time.Duration)
	Since(t time.Time) time.Duration
	Until(t time.Time) time.Duration
	NewTimer(d time.Duration) Timer
	NewTicker(d time.Duration) Ticker
	AfterFunc(d time.Duration, f func()) Timer
}

// Timer is the interface wrapping time.Timer.
type Timer interface {
	C() <-chan time.Time
	Stop() bool
	Reset(d time.Duration) bool
}

// Ticker is the interface wrapping time.Ticker.
type Ticker interface {
	C() <-chan time.Time
	Stop()
	Reset(d time.Duration)
}

// RealClock is the production implementation using the stdlib time package.
// All methods are safe for concurrent use.
type RealClock struct{}

// Now returns the current local time.
func (RealClock) Now() time.Time { return time.Now() }

// Sleep pauses the goroutine for duration d.
func (RealClock) Sleep(d time.Duration) { time.Sleep(d) }

// Since returns the elapsed time since t.
func (RealClock) Since(t time.Time) time.Duration { return time.Since(t) }

// Until returns the duration until t.
func (RealClock) Until(t time.Time) time.Duration { return time.Until(t) }

// NewTimer creates a new Timer that fires after duration d.
func (RealClock) NewTimer(d time.Duration) Timer { return &realTimer{t: time.NewTimer(d)} }

// NewTicker creates a new Ticker that sends the time on its channel at every d.
func (RealClock) NewTicker(d time.Duration) Ticker { return &realTicker{t: time.NewTicker(d)} }

// AfterFunc waits for duration d and then calls f in its own goroutine.
func (RealClock) AfterFunc(d time.Duration, f func()) Timer {
	return &realTimer{t: time.AfterFunc(d, f)}
}

type realTimer struct{ t *time.Timer }

func (r *realTimer) C() <-chan time.Time      { return r.t.C }
func (r *realTimer) Stop() bool               { return r.t.Stop() }
func (r *realTimer) Reset(d time.Duration) bool { return r.t.Reset(d) }

type realTicker struct{ t *time.Ticker }

func (r *realTicker) C() <-chan time.Time    { return r.t.C }
func (r *realTicker) Stop()                  { r.t.Stop() }
func (r *realTicker) Reset(d time.Duration)  { r.t.Reset(d) }

// manualTimer is a timer controlled by ManualClock.
type manualTimer struct {
	fireAt  time.Time
	ch      chan time.Time
	stopped bool
	fn      func() // for AfterFunc
	clock   *ManualClock
	mu      sync.Mutex
}

func (m *manualTimer) C() <-chan time.Time { return m.ch }

func (m *manualTimer) Stop() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped {
		return false
	}
	m.stopped = true
	m.clock.removeTimer(m)
	return true
}

func (m *manualTimer) Reset(d time.Duration) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	wasActive := !m.stopped
	m.stopped = false
	m.clock.mu.Lock()
	m.fireAt = m.clock.now.Add(d)
	m.clock.addTimerLocked(m)
	m.clock.mu.Unlock()
	return wasActive
}

// manualTicker is a ticker controlled by ManualClock.
type manualTicker struct {
	interval time.Duration
	nextFire time.Time
	ch       chan time.Time
	stopped  bool
	clock    *ManualClock
	mu       sync.Mutex
}

func (m *manualTicker) C() <-chan time.Time { return m.ch }

func (m *manualTicker) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopped = true
	m.clock.removeTicker(m)
}

func (m *manualTicker) Reset(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	wasStopped := m.stopped
	m.interval = d
	m.stopped = false
	m.clock.mu.Lock()
	m.nextFire = m.clock.now.Add(d)
	// If the ticker was previously stopped it was removed from the clock's
	// list and would never fire again. Re-register it so Reset revives it
	// (M-17b). Guard against duplicates.
	if wasStopped {
		m.clock.addTickerLocked(m)
	}
	m.clock.mu.Unlock()
}

// ManualClock is a test double whose time only advances when Advance() is called.
// All goroutines blocked on Sleep or timers are unblocked deterministically.
// All methods are safe for concurrent use.
type ManualClock struct {
	mu      sync.Mutex
	now     time.Time
	timers  []*manualTimer
	tickers []*manualTicker
}

// NewManualClock creates a ManualClock starting at the given time.
func NewManualClock(start time.Time) *ManualClock {
	return &ManualClock{now: start}
}

// Now returns the current manual time.
func (c *ManualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Sleep blocks the caller until the clock is advanced past the sleep duration.
func (c *ManualClock) Sleep(d time.Duration) {
	if d <= 0 {
		return
	}
	timer := c.NewTimer(d)
	<-timer.C()
}

// Since returns the elapsed time since t according to the manual clock.
func (c *ManualClock) Since(t time.Time) time.Duration {
	return c.Now().Sub(t)
}

// Until returns the duration until t according to the manual clock.
func (c *ManualClock) Until(t time.Time) time.Duration {
	return t.Sub(c.Now())
}

// NewTimer creates a timer that fires after duration d when the clock is advanced.
func (c *ManualClock) NewTimer(d time.Duration) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &manualTimer{
		fireAt: c.now.Add(d),
		ch:     make(chan time.Time, 1),
		clock:  c,
	}
	if d <= 0 {
		t.ch <- c.now
	} else {
		c.addTimerLocked(t)
	}
	return t
}

// NewTicker creates a ticker that fires every d when the clock is advanced.
func (c *ManualClock) NewTicker(d time.Duration) Ticker {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &manualTicker{
		interval: d,
		nextFire: c.now.Add(d),
		// Generously buffered so a single Advance spanning many intervals can
		// deposit one tick per interval without blocking the advancer (M-17a),
		// while still coalescing (dropping) once the consumer falls this far
		// behind — matching real time.Ticker semantics and, crucially, never
		// deadlocking Advance when no consumer is draining concurrently.
		ch:    make(chan time.Time, tickerChanBuffer),
		clock: c,
	}
	c.tickers = append(c.tickers, t)
	return t
}

// AfterFunc fires f after duration d.
func (c *ManualClock) AfterFunc(d time.Duration, f func()) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &manualTimer{
		fireAt: c.now.Add(d),
		ch:     make(chan time.Time, 1),
		fn:     f,
		clock:  c,
	}
	c.addTimerLocked(t)
	return t
}

// Advance moves the clock forward by d, firing all timers/tickers in order.
func (c *ManualClock) Advance(d time.Duration) {
	c.mu.Lock()
	target := c.now.Add(d)
	c.now = target
	// Fire timers in order
	for {
		idx := c.nextTimerLocked(target)
		if idx < 0 {
			break
		}
		t := c.timers[idx]
		c.timers = append(c.timers[:idx], c.timers[idx+1:]...)
		fireAt := t.fireAt
		c.mu.Unlock()
		t.mu.Lock()
		if !t.stopped {
			if t.fn != nil {
				go t.fn()
			} else {
				select {
				case t.ch <- fireAt:
				default:
				}
			}
		}
		t.mu.Unlock()
		c.mu.Lock()
	}
	// Snapshot the tickers and release c.mu before touching any tick.mu.
	// This enforces a consistent lock order (tick.mu is always acquired
	// before c.mu, matching manualTicker.Reset) and prevents the H-15
	// lock-order-inversion deadlock between Advance and Reset.
	tickers := make([]*manualTicker, len(c.tickers))
	copy(tickers, c.tickers)
	c.mu.Unlock()

	// Fire tickers. For each interval crossed we deliver one tick (into the
	// generously-buffered channel) so that advancing N intervals yields N
	// ticks (M-17a). The send is strictly non-blocking: if the buffer is full
	// (a consumer that has fallen tickerChanBuffer ticks behind) we drop the
	// tick, exactly as a real time.Ticker coalesces under a slow consumer.
	// This must never block, otherwise Advance would deadlock whenever no
	// consumer is draining concurrently (CB-CLOCK-1).
	for _, tick := range tickers {
		tick.mu.Lock()
		if tick.stopped {
			tick.mu.Unlock()
			continue
		}
		for !tick.nextFire.After(target) {
			fireAt := tick.nextFire
			tick.nextFire = tick.nextFire.Add(tick.interval)
			select {
			case tick.ch <- fireAt:
			default:
				// Buffer full: coalesce (drop), like a real ticker. Never block.
			}
		}
		tick.mu.Unlock()
	}
}

// addTimerLocked adds timer to the sorted slice. Must hold c.mu.
func (c *ManualClock) addTimerLocked(t *manualTimer) {
	c.timers = append(c.timers, t)
	sort.Slice(c.timers, func(i, j int) bool {
		return c.timers[i].fireAt.Before(c.timers[j].fireAt)
	})
}

// addTickerLocked adds a ticker to the clock's list if not already present.
// Must hold c.mu.
func (c *ManualClock) addTickerLocked(t *manualTicker) {
	for _, tk := range c.tickers {
		if tk == t {
			return
		}
	}
	c.tickers = append(c.tickers, t)
}

// nextTimerLocked returns the index of the next timer to fire at or before target.
// Returns -1 if none. Must hold c.mu.
func (c *ManualClock) nextTimerLocked(target time.Time) int {
	for i, t := range c.timers {
		if !t.fireAt.After(target) {
			return i
		}
	}
	return -1
}

// removeTimer removes a timer from the clock's list.
func (c *ManualClock) removeTimer(t *manualTimer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, tm := range c.timers {
		if tm == t {
			c.timers = append(c.timers[:i], c.timers[i+1:]...)
			return
		}
	}
}

// removeTicker removes a ticker from the clock's list.
func (c *ManualClock) removeTicker(t *manualTicker) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, tk := range c.tickers {
		if tk == t {
			c.tickers = append(c.tickers[:i], c.tickers[i+1:]...)
			return
		}
	}
}
