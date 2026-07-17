// Package fixedwindow implements the Fixed Window Counter rate limiting algorithm.
//
// Theory: Divide time into fixed windows (e.g., every 60 seconds). Count requests
// per window. Reset counter at window boundary. Simple and fast but has a
// "boundary burst" problem — up to 2x limit requests are possible straddling
// a window boundary.
//
// Properties:
//   - O(1) time and space per key
//   - Simple and predictable behavior
//   - Known limitation: boundary burst allows ~2x limit at boundary
//   - Zero external dependencies
//
// Time complexity: O(1) per Allow call.
// Space complexity: O(keys).
//
// Reference: https://konghq.com/blog/how-to-design-a-scalable-rate-limiting-algorithm
//
// All methods on FixedWindowCounter are safe for concurrent use.
package fixedwindow

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/metric"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
)

const algorithmName = "fixed_window"

type counter struct {
	mu          sync.Mutex
	count       int64
	windowStart time.Time
	lastAccess  time.Time
}

// FixedWindowCounter implements Limiter using fixed window counting.
// All methods are safe for concurrent use.
type FixedWindowCounter struct {
	limit     int
	window    time.Duration
	clock     clock.Clock
	rec       metric.Recorder
	idleClean time.Duration

	mu       sync.RWMutex
	counters map[string]*counter

	done   chan struct{}
	wg     sync.WaitGroup
	closed bool

	// onDecision, when non-nil, is fired synchronously after every Allow/AllowN
	// decision. nil by default so the hot path stays a single nil check.
	onDecision func(key string, r ratelimit.Result)
}

// Option configures a FixedWindowCounter.
type Option func(*FixedWindowCounter)

// WithOnDecision registers a hook fired after every Allow/AllowN decision
// (both allow and deny), receiving the key and the resulting Result. The
// default is nil (a cheap no-op guarded by a nil check on the hot path). The
// hook runs synchronously on the calling goroutine before the decision is
// returned, so keep it fast and non-blocking. A nil hook is ignored.
func WithOnDecision(fn func(key string, r ratelimit.Result)) Option {
	return func(fw *FixedWindowCounter) {
		if fn != nil {
			fw.onDecision = fn
		}
	}
}

// WithClock sets a custom clock for deterministic testing.
func WithClock(c clock.Clock) Option {
	return func(fw *FixedWindowCounter) { fw.clock = c }
}

// WithIdleCleanup sets how long to keep inactive counters before eviction.
func WithIdleCleanup(d time.Duration) Option {
	return func(fw *FixedWindowCounter) { fw.idleClean = d }
}

// WithRecorder wires a metric.Recorder so allow/deny decisions and decision
// latency are emitted. Defaults to metric.Default() (a no-op) when unset. A nil
// recorder is ignored.
func WithRecorder(rec metric.Recorder) Option {
	return func(fw *FixedWindowCounter) {
		if rec != nil {
			fw.rec = rec
		}
	}
}

// New creates a new FixedWindowCounter allowing limit requests per window duration.
//
// It panics if limit or window is not positive, following the same
// panic-on-nonpositive convention as time.NewTicker. A non-positive window
// would otherwise cause an integer divide-by-zero (ns/windowNs) on the first
// Allow.
func New(limit int, window time.Duration, opts ...Option) *FixedWindowCounter {
	if limit <= 0 {
		panic(fmt.Sprintf("fixedwindow: New limit must be positive, got %d", limit))
	}
	if window <= 0 {
		panic(fmt.Sprintf("fixedwindow: New window must be positive, got %s", window))
	}
	fw := &FixedWindowCounter{
		limit:     limit,
		window:    window,
		clock:     clock.RealClock{},
		rec:       metric.Default(),
		idleClean: 5 * time.Minute,
		counters:  make(map[string]*counter),
		done:      make(chan struct{}),
	}
	for _, opt := range opts {
		opt(fw)
	}
	fw.wg.Add(1)
	go fw.cleanupLoop()
	return fw
}

// Allow checks if 1 request is allowed within the window.
func (fw *FixedWindowCounter) Allow(ctx context.Context, key string) ratelimit.Result {
	return fw.AllowN(ctx, key, 1)
}

// AllowN checks if n requests are allowed (atomically, all or none).
func (fw *FixedWindowCounter) AllowN(_ context.Context, key string, n int) (res ratelimit.Result) {
	start := fw.clock.Now()
	defer func() {
		if n != 1 {
			setCost(&res, n)
		}
		fw.record(res, start)
		if fw.onDecision != nil {
			fw.onDecision(key, res)
		}
	}()

	if err := ratelimit.ValidateKey(key); err != nil {
		return ratelimit.Result{Allowed: false, Algorithm: algorithmName}
	}
	if err := ratelimit.ValidateN(n); err != nil {
		return ratelimit.Result{Allowed: false, Algorithm: algorithmName}
	}

	c := fw.getOrCreate(key)
	return fw.consume(c, n)
}

// setCost records the consumed cost in res.Metadata under the "cost" key,
// allocating the map lazily so the n==1 hot path stays allocation-free.
func setCost(res *ratelimit.Result, cost int) {
	if res.Metadata == nil {
		res.Metadata = ratelimit.Metadata{}
	}
	res.Metadata["cost"] = cost
}

// record fires the configured metric.Recorder for a completed decision. With
// the default Nop recorder every call is an empty inlined method, so this stays
// allocation-free on the hot path.
func (fw *FixedWindowCounter) record(res ratelimit.Result, start time.Time) {
	if res.Allowed {
		fw.rec.IncAllowed(algorithmName)
	} else {
		fw.rec.IncDenied(algorithmName)
	}
	fw.rec.ObserveDecision(algorithmName, fw.clock.Now().Sub(start))
}

// Wait blocks until 1 request is allowed or ctx is cancelled.
func (fw *FixedWindowCounter) Wait(ctx context.Context, key string) error {
	return fw.WaitN(ctx, key, 1)
}

// WaitN blocks until n requests are allowed or ctx is cancelled.
func (fw *FixedWindowCounter) WaitN(ctx context.Context, key string, n int) error {
	if err := ratelimit.ValidateKey(key); err != nil {
		return err
	}
	if err := ratelimit.ValidateN(n); err != nil {
		return err
	}
	c := fw.getOrCreate(key)
	for {
		result := fw.consume(c, n)
		if result.Allowed {
			return nil
		}
		timer := fw.clock.NewTimer(result.RetryAfter)
		select {
		case <-ctx.Done():
			timer.Stop()
			return &ratelimit.RateLimitError{
				Algorithm:  algorithmName,
				Key:        key,
				Limit:      fw.limit,
				RetryAfter: result.RetryAfter,
				Err:        ratelimit.ErrContextDone,
			}
		case <-timer.C():
		}
	}
}

// Peek returns current state without consuming anything.
func (fw *FixedWindowCounter) Peek(_ context.Context, key string) ratelimit.State {
	c := fw.getOrCreate(key)
	c.mu.Lock()
	defer c.mu.Unlock()
	now := fw.clock.Now()
	windowStart := fw.windowStart(now)
	count := c.count
	if !c.windowStart.Equal(windowStart) {
		count = 0
	}
	remaining := fw.limit - int(count)
	if remaining < 0 {
		remaining = 0
	}
	resetAt := windowStart.Add(fw.window)
	return ratelimit.State{
		Key:         key,
		Algorithm:   algorithmName,
		Limit:       fw.limit,
		Remaining:   remaining,
		ResetAt:     resetAt,
		WindowStart: windowStart,
		Extra: map[string]any{
			"count":        count,
			"window_start": windowStart,
			"window":       fw.window.String(),
		},
	}
}

// Reset clears state for the given key.
func (fw *FixedWindowCounter) Reset(_ context.Context, key string) error {
	fw.mu.Lock()
	delete(fw.counters, key)
	fw.mu.Unlock()
	return nil
}

// Close stops the cleanup goroutine.
func (fw *FixedWindowCounter) Close() error {
	fw.mu.Lock()
	if fw.closed {
		fw.mu.Unlock()
		return nil
	}
	fw.closed = true
	fw.mu.Unlock()
	// Signal shutdown AFTER releasing the mutex so cleanupLoop is never
	// blocked on fw.mu.Lock() while Close holds it.
	close(fw.done)
	fw.wg.Wait()
	return nil
}

func (fw *FixedWindowCounter) getOrCreate(key string) *counter {
	fw.mu.RLock()
	c, ok := fw.counters[key]
	fw.mu.RUnlock()
	if ok {
		return c
	}
	fw.mu.Lock()
	defer fw.mu.Unlock()
	if c, ok = fw.counters[key]; ok {
		return c
	}
	now := fw.clock.Now()
	c = &counter{windowStart: fw.windowStart(now), lastAccess: now}
	fw.counters[key] = c
	return c
}

func (fw *FixedWindowCounter) windowStart(now time.Time) time.Time {
	ns := now.UnixNano()
	windowNs := fw.window.Nanoseconds()
	return time.Unix(0, (ns/windowNs)*windowNs).UTC()
}

func (fw *FixedWindowCounter) consume(c *counter, n int) ratelimit.Result {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := fw.clock.Now()
	c.lastAccess = now
	windowStart := fw.windowStart(now)

	// Reset if window has changed
	if !c.windowStart.Equal(windowStart) {
		c.count = 0
		c.windowStart = windowStart
	}

	resetAt := windowStart.Add(fw.window)
	retryAfter := resetAt.Sub(now)

	if int(c.count)+n > fw.limit {
		remaining := fw.limit - int(c.count)
		if remaining < 0 {
			remaining = 0
		}
		return ratelimit.Result{
			Allowed:    false,
			Limit:      fw.limit,
			Remaining:  remaining,
			ResetAfter: retryAfter,
			RetryAfter: retryAfter,
			Algorithm:  algorithmName,
		}
	}

	c.count += int64(n)
	remaining := fw.limit - int(c.count)
	return ratelimit.Result{
		Allowed:    true,
		Limit:      fw.limit,
		Remaining:  remaining,
		ResetAfter: retryAfter,
		Algorithm:  algorithmName,
	}
}

func (fw *FixedWindowCounter) cleanupLoop() {
	defer fw.wg.Done()
	if fw.idleClean <= 0 {
		return
	}
	ticker := fw.clock.NewTicker(fw.idleClean)
	defer ticker.Stop()
	for {
		select {
		case <-fw.done:
			return
		case <-ticker.C():
			cutoff := fw.clock.Now().Add(-fw.idleClean)
			fw.mu.Lock()
			for k, c := range fw.counters {
				c.mu.Lock()
				idle := c.lastAccess.Before(cutoff)
				c.mu.Unlock()
				if idle {
					delete(fw.counters, k)
				}
			}
			fw.mu.Unlock()
		}
	}
}

// String returns a human-readable description.
func (fw *FixedWindowCounter) String() string {
	return fmt.Sprintf("FixedWindow(limit=%d, window=%s)", fw.limit, fw.window)
}
