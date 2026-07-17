package slidingwindow

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
)

const counterAlgorithmName = "sliding_window_counter"

// windowBucket holds count and start time for a single window bucket.
type windowBucket struct {
	count       int64
	windowStart time.Time
}

// keyCounter holds current and previous window buckets for a key.
type keyCounter struct {
	mu         sync.Mutex
	current    windowBucket
	previous   windowBucket
	lastAccess time.Time
}

// SlidingWindowCounter implements an approximate sliding window using two fixed windows.
//
// Formula: effective_count = previous_count * (1 - elapsed/window) + current_count
//
// Pros: O(1) memory per key — extremely efficient.
// Cons: Approximate — can allow up to limit * (1 + 1/N) requests at boundaries.
//
//	Maximum approximation error: limit * 1/N where N is window periods.
//
// Compared to SlidingWindowLog:
//   - 99% accuracy with far less memory
//   - Same performance characteristics
//
// Time complexity: O(1) per Allow call.
// Space complexity: O(keys) — just two counters per key.
//
// All methods on SlidingWindowCounter are safe for concurrent use.
type SlidingWindowCounter struct {
	limit     int
	window    time.Duration
	idleClean time.Duration
	clock     clock.Clock

	mu       sync.RWMutex
	counters map[string]*keyCounter

	done   chan struct{}
	wg     sync.WaitGroup
	closed bool
}

// CounterOption configures a SlidingWindowCounter.
type CounterOption func(*SlidingWindowCounter)

// WithCounterClock sets the clock for testing.
func WithCounterClock(c clock.Clock) CounterOption {
	return func(swc *SlidingWindowCounter) { swc.clock = c }
}

// NewCounter creates a SlidingWindowCounter allowing limit requests per window.
//
// It panics if limit or window is not positive, following the same
// panic-on-nonpositive convention as time.NewTicker. A non-positive window
// leads to a panicking cleanup goroutine (NewTicker) and a non-positive limit
// leads to a divide-by-zero in WaitN (window/limit).
func NewCounter(limit int, window time.Duration, opts ...CounterOption) *SlidingWindowCounter {
	if limit <= 0 {
		panic(fmt.Sprintf("slidingwindow: NewCounter limit must be positive, got %d", limit))
	}
	if window <= 0 {
		panic(fmt.Sprintf("slidingwindow: NewCounter window must be positive, got %s", window))
	}
	swc := &SlidingWindowCounter{
		limit:     limit,
		window:    window,
		idleClean: window * 5,
		clock:     clock.RealClock{},
		counters:  make(map[string]*keyCounter),
		done:      make(chan struct{}),
	}
	for _, opt := range opts {
		opt(swc)
	}
	swc.wg.Add(1)
	go swc.cleanupLoop()
	return swc
}

// Allow checks if 1 request is allowed.
func (swc *SlidingWindowCounter) Allow(ctx context.Context, key string) ratelimit.Result {
	return swc.AllowN(ctx, key, 1)
}

// AllowN checks if n requests are allowed (approximately).
func (swc *SlidingWindowCounter) AllowN(_ context.Context, key string, n int) ratelimit.Result {
	if err := ratelimit.ValidateKey(key); err != nil {
		return ratelimit.Result{Allowed: false, Algorithm: counterAlgorithmName}
	}
	if err := ratelimit.ValidateN(n); err != nil {
		return ratelimit.Result{Allowed: false, Algorithm: counterAlgorithmName}
	}
	kc := swc.getOrCreate(key)
	return swc.consume(kc, n)
}

// Wait blocks until allowed or ctx is cancelled.
func (swc *SlidingWindowCounter) Wait(ctx context.Context, key string) error {
	return swc.WaitN(ctx, key, 1)
}

// WaitN blocks until n requests are allowed or ctx is cancelled.
func (swc *SlidingWindowCounter) WaitN(ctx context.Context, key string, n int) error {
	if err := ratelimit.ValidateKey(key); err != nil {
		return err
	}
	if err := ratelimit.ValidateN(n); err != nil {
		return err
	}
	kc := swc.getOrCreate(key)
	for {
		result := swc.consume(kc, n)
		if result.Allowed {
			return nil
		}
		wait := result.RetryAfter
		if wait <= 0 {
			wait = swc.window / time.Duration(swc.limit)
		}
		timer := swc.clock.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return &ratelimit.RateLimitError{
				Algorithm:  counterAlgorithmName,
				Key:        key,
				Limit:      swc.limit,
				RetryAfter: wait,
				Err:        ratelimit.ErrContextDone,
			}
		case <-timer.C():
		}
	}
}

// Peek returns current approximate state without side effects.
func (swc *SlidingWindowCounter) Peek(_ context.Context, key string) ratelimit.State {
	kc := swc.getOrCreate(key)
	kc.mu.Lock()
	defer kc.mu.Unlock()
	now := swc.clock.Now()
	effectiveCount := swc.effectiveCountLocked(kc, now)
	remaining := float64(swc.limit) - effectiveCount
	if remaining < 0 {
		remaining = 0
	}
	windowStart := swc.windowStart(now)
	return ratelimit.State{
		Key:         key,
		Algorithm:   counterAlgorithmName,
		Limit:       swc.limit,
		Remaining:   int(remaining),
		ResetAt:     windowStart.Add(swc.window),
		WindowStart: windowStart,
		Extra: map[string]any{
			"current_count":        kc.current.count,
			"previous_count":       kc.previous.count,
			"effective_count":      effectiveCount,
			"approximation_method": "sliding_window_counter",
			"max_error_bound":      float64(swc.limit) / float64(swc.window),
		},
	}
}

// Reset clears state for the given key.
func (swc *SlidingWindowCounter) Reset(_ context.Context, key string) error {
	swc.mu.Lock()
	delete(swc.counters, key)
	swc.mu.Unlock()
	return nil
}

// Close stops the cleanup goroutine.
func (swc *SlidingWindowCounter) Close() error {
	swc.mu.Lock()
	if swc.closed {
		swc.mu.Unlock()
		return nil
	}
	swc.closed = true
	swc.mu.Unlock()
	// Signal shutdown AFTER releasing the mutex so cleanupLoop is never
	// blocked on swc.mu.Lock() while Close holds it.
	close(swc.done)
	swc.wg.Wait()
	return nil
}

func (swc *SlidingWindowCounter) windowStart(now time.Time) time.Time {
	ns := now.UnixNano()
	windowNs := swc.window.Nanoseconds()
	return time.Unix(0, (ns/windowNs)*windowNs).UTC()
}

func (swc *SlidingWindowCounter) effectiveCountLocked(kc *keyCounter, now time.Time) float64 {
	windowStart := swc.windowStart(now)
	elapsed := now.Sub(windowStart)
	elapsedFrac := float64(elapsed) / float64(swc.window)

	if !kc.current.windowStart.Equal(windowStart) {
		// Window has advanced
		if kc.current.windowStart.Equal(windowStart.Add(-swc.window)) {
			kc.previous = kc.current
		} else {
			kc.previous = windowBucket{}
		}
		kc.current = windowBucket{windowStart: windowStart}
	}

	return float64(kc.previous.count)*(1.0-elapsedFrac) + float64(kc.current.count)
}

func (swc *SlidingWindowCounter) consume(kc *keyCounter, n int) ratelimit.Result {
	kc.mu.Lock()
	defer kc.mu.Unlock()
	now := swc.clock.Now()
	kc.lastAccess = now
	effectiveCount := swc.effectiveCountLocked(kc, now)

	if effectiveCount+float64(n) > float64(swc.limit) {
		remaining := float64(swc.limit) - effectiveCount
		if remaining < 0 {
			remaining = 0
		}
		// Compute how long until the request could succeed.
		//
		// The effective count within the current window is
		//   previous.count*(1 - elapsed/window) + current.count
		// As time advances within the current window, the previous-window
		// contribution decays linearly to 0. If, even with the previous window
		// fully rolled off, current.count + n still exceeds the limit, then no
		// amount of waiting within the current window helps — the caller must
		// wait until the current window itself rolls over. (SWC-2 / L-3)
		windowStart := swc.windowStart(now)
		elapsed := float64(now.Sub(windowStart))
		windowF := float64(swc.window)
		var retryNs time.Duration
		if float64(kc.current.count)+float64(n) > float64(swc.limit) || kc.previous.count == 0 {
			// Previous rolloff cannot help; wait for the current window to roll.
			retryNs = swc.window - time.Duration(elapsed)
		} else {
			// Wait until enough of the previous window has decayed. We need the
			// previous contribution to drop to (limit - n - current.count):
			//   previous.count*(1 - f) = limit - n - current.count
			// where f is the elapsed fraction at the retry instant. Solving for
			// the required elapsed fraction and capping it to [0, 1] guarantees
			// RetryAfter never exceeds a single window.
			allowedPrev := float64(swc.limit) - float64(n) - float64(kc.current.count)
			neededFrac := 1.0 - allowedPrev/float64(kc.previous.count)
			if neededFrac < 0 {
				neededFrac = 0
			}
			if neededFrac > 1 {
				neededFrac = 1
			}
			retryNs = time.Duration(neededFrac*windowF - elapsed)
		}
		if retryNs < 0 {
			retryNs = time.Millisecond
		}
		return ratelimit.Result{
			Allowed:    false,
			Limit:      swc.limit,
			Remaining:  int(remaining),
			RetryAfter: retryNs,
			ResetAfter: swc.window - time.Duration(elapsed),
			Algorithm:  counterAlgorithmName,
		}
	}

	kc.current.count += int64(n)
	remaining := float64(swc.limit) - effectiveCount - float64(n)
	if remaining < 0 {
		remaining = 0
	}
	return ratelimit.Result{
		Allowed:   true,
		Limit:     swc.limit,
		Remaining: int(remaining),
		Algorithm: counterAlgorithmName,
	}
}

func (swc *SlidingWindowCounter) getOrCreate(key string) *keyCounter {
	swc.mu.RLock()
	kc, ok := swc.counters[key]
	swc.mu.RUnlock()
	if ok {
		return kc
	}
	swc.mu.Lock()
	defer swc.mu.Unlock()
	if kc, ok = swc.counters[key]; ok {
		return kc
	}
	now := swc.clock.Now()
	kc = &keyCounter{
		current:    windowBucket{windowStart: swc.windowStart(now)},
		lastAccess: now,
	}
	swc.counters[key] = kc
	return kc
}

func (swc *SlidingWindowCounter) cleanupLoop() {
	defer swc.wg.Done()
	ticker := swc.clock.NewTicker(swc.window)
	defer ticker.Stop()
	for {
		select {
		case <-swc.done:
			return
		case <-ticker.C():
			cutoff := swc.clock.Now().Add(-swc.idleClean)
			swc.mu.Lock()
			for k, kc := range swc.counters {
				kc.mu.Lock()
				idle := kc.lastAccess.Before(cutoff)
				kc.mu.Unlock()
				if idle {
					delete(swc.counters, k)
				}
			}
			swc.mu.Unlock()
		}
	}
}

// String returns a human-readable description.
func (swc *SlidingWindowCounter) String() string {
	return fmt.Sprintf("SlidingWindowCounter(limit=%d, window=%s)", swc.limit, swc.window)
}
