// Package slidingwindow implements two sliding window rate limiting variants.
//
// SlidingWindowLog: maintains exact per-request timestamps. O(requests) memory per key.
// SlidingWindowCounter: approximates using two fixed windows. O(1) memory per key.
//
// All methods are safe for concurrent use.
package slidingwindow

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/metric"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/internal/shardmap"
)

const logAlgorithmName = "sliding_window_log"

// keyLog stores per-key timestamp log.
type keyLog struct {
	mu         sync.Mutex
	timestamps []time.Time
	lastAccess time.Time
}

// SlidingWindowLog implements the Limiter interface using an exact timestamp log.
// Memory usage: O(limit) timestamps per key.
//
// Pros: Exact — never over-allows, even at window boundaries.
// Cons: Memory-intensive for high-traffic keys.
//
// Time complexity: O(requests_in_window) per Allow call (for cleanup).
// Space complexity: O(requests_in_window) per key.
//
// All methods on SlidingWindowLog are safe for concurrent use.
type SlidingWindowLog struct {
	limit     int
	window    time.Duration
	idleClean time.Duration
	clock     clock.Clock
	rec       metric.Recorder

	// logs shards per-key state across GOMAXPROCS-sized stripes so requests for
	// different keys don't serialize on one global lock.
	logs *shardmap.Map[keyLog]

	// mu now guards only limiter lifecycle state (closed), not the key map.
	mu     sync.Mutex
	done   chan struct{}
	wg     sync.WaitGroup
	closed bool

	// onDecision, when non-nil, is fired synchronously after every Allow/AllowN
	// decision. nil by default so the hot path stays a single nil check.
	onDecision func(key string, r ratelimit.Result)
}

// LogOption configures a SlidingWindowLog.
type LogOption func(*SlidingWindowLog)

// WithLogOnDecision registers a hook fired after every Allow/AllowN decision
// (both allow and deny), receiving the key and the resulting Result. The
// default is nil (a cheap no-op guarded by a nil check on the hot path). The
// hook runs synchronously on the calling goroutine before the decision is
// returned, so keep it fast and non-blocking. A nil hook is ignored.
func WithLogOnDecision(fn func(key string, r ratelimit.Result)) LogOption {
	return func(l *SlidingWindowLog) {
		if fn != nil {
			l.onDecision = fn
		}
	}
}

// WithLogClock sets the clock for testing.
func WithLogClock(c clock.Clock) LogOption {
	return func(l *SlidingWindowLog) { l.clock = c }
}

// WithLogRecorder wires a metric.Recorder so allow/deny decisions and decision
// latency are emitted. Defaults to metric.Default() (a no-op) when unset. A nil
// recorder is ignored.
func WithLogRecorder(rec metric.Recorder) LogOption {
	return func(l *SlidingWindowLog) {
		if rec != nil {
			l.rec = rec
		}
	}
}

// NewLog creates a SlidingWindowLog allowing limit requests per window.
//
// It panics if limit or window is not positive, following the same
// panic-on-nonpositive convention as time.NewTicker. A non-positive window or
// limit is a programming error that would otherwise surface as a divide-by-zero
// or a panicking background goroutine.
func NewLog(limit int, window time.Duration, opts ...LogOption) *SlidingWindowLog {
	if limit <= 0 {
		panic(fmt.Sprintf("slidingwindow: NewLog limit must be positive, got %d", limit))
	}
	if window <= 0 {
		panic(fmt.Sprintf("slidingwindow: NewLog window must be positive, got %s", window))
	}
	l := &SlidingWindowLog{
		limit:     limit,
		window:    window,
		idleClean: window * 2,
		clock:     clock.RealClock{},
		rec:       metric.Default(),
		logs:      shardmap.New[keyLog](),
		done:      make(chan struct{}),
	}
	for _, opt := range opts {
		opt(l)
	}
	l.wg.Add(1)
	go l.cleanupLoop()
	return l
}

// Allow checks if 1 request is allowed.
func (l *SlidingWindowLog) Allow(ctx context.Context, key string) ratelimit.Result {
	return l.AllowN(ctx, key, 1)
}

// AllowN checks if n requests are allowed in the current window.
func (l *SlidingWindowLog) AllowN(_ context.Context, key string, n int) (res ratelimit.Result) {
	start := l.clock.Now()
	defer func() {
		if n != 1 {
			setCost(&res, n)
		}
		l.record(res, start)
		if l.onDecision != nil {
			l.onDecision(key, res)
		}
	}()

	if err := ratelimit.ValidateKey(key); err != nil {
		return ratelimit.Result{Allowed: false, Algorithm: logAlgorithmName}
	}
	if err := ratelimit.ValidateN(n); err != nil {
		return ratelimit.Result{Allowed: false, Algorithm: logAlgorithmName}
	}

	kl := l.getOrCreate(key)
	return l.consume(kl, n)
}

// record fires the configured metric.Recorder for a completed decision. With
// the default Nop recorder every call is an empty inlined method, so this stays
// allocation-free on the hot path.
func (l *SlidingWindowLog) record(res ratelimit.Result, start time.Time) {
	if res.Allowed {
		l.rec.IncAllowed(logAlgorithmName)
	} else {
		l.rec.IncDenied(logAlgorithmName)
	}
	l.rec.ObserveDecision(logAlgorithmName, l.clock.Now().Sub(start))
}

// Wait blocks until 1 request is allowed or ctx is cancelled.
func (l *SlidingWindowLog) Wait(ctx context.Context, key string) error {
	return l.WaitN(ctx, key, 1)
}

// WaitN blocks until n requests are allowed or ctx is cancelled.
func (l *SlidingWindowLog) WaitN(ctx context.Context, key string, n int) error {
	if err := ratelimit.ValidateKey(key); err != nil {
		return err
	}
	if err := ratelimit.ValidateN(n); err != nil {
		return err
	}
	kl := l.getOrCreate(key)
	for {
		result := l.consume(kl, n)
		if result.Allowed {
			return nil
		}
		timer := l.clock.NewTimer(result.RetryAfter)
		select {
		case <-ctx.Done():
			timer.Stop()
			return &ratelimit.RateLimitError{
				Algorithm:  logAlgorithmName,
				Key:        key,
				Limit:      l.limit,
				RetryAfter: result.RetryAfter,
				Err:        ratelimit.ErrContextDone,
			}
		case <-timer.C():
		}
	}
}

// Peek returns current state without consuming any capacity.
func (l *SlidingWindowLog) Peek(_ context.Context, key string) ratelimit.State {
	kl := l.getOrCreate(key)
	kl.mu.Lock()
	defer kl.mu.Unlock()
	now := l.clock.Now()
	cutoff := now.Add(-l.window)
	l.pruneLocked(kl, cutoff)
	count := len(kl.timestamps)
	remaining := l.limit - count
	if remaining < 0 {
		remaining = 0
	}
	var retryAfter time.Duration
	if count > 0 {
		retryAfter = kl.timestamps[0].Add(l.window).Sub(now)
	}
	return ratelimit.State{
		Key:         key,
		Algorithm:   logAlgorithmName,
		Limit:       l.limit,
		Remaining:   remaining,
		ResetAt:     now.Add(retryAfter),
		WindowStart: cutoff,
		Extra: map[string]any{
			"log_size": count,
			"window":   l.window.String(),
		},
	}
}

// Reset clears state for the given key.
func (l *SlidingWindowLog) Reset(_ context.Context, key string) error {
	l.logs.Delete(key)
	return nil
}

// Close stops cleanup goroutine.
func (l *SlidingWindowLog) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	l.mu.Unlock()
	// Signal shutdown AFTER releasing the mutex so cleanupLoop is never
	// blocked on l.mu.Lock() while Close holds it.
	close(l.done)
	l.wg.Wait()
	return nil
}

func (l *SlidingWindowLog) getOrCreate(key string) *keyLog {
	return l.logs.GetOrCreate(key, func() *keyLog {
		return &keyLog{lastAccess: l.clock.Now()}
	})
}

func (l *SlidingWindowLog) consume(kl *keyLog, n int) ratelimit.Result {
	kl.mu.Lock()
	defer kl.mu.Unlock()
	now := l.clock.Now()
	kl.lastAccess = now
	cutoff := now.Add(-l.window)
	l.pruneLocked(kl, cutoff)
	count := len(kl.timestamps)

	if count+n > l.limit {
		var retryAfter time.Duration
		if count > 0 {
			retryAfter = kl.timestamps[0].Add(l.window).Sub(now)
		}
		return ratelimit.Result{
			Allowed:    false,
			Limit:      l.limit,
			Remaining:  l.limit - count,
			RetryAfter: retryAfter,
			ResetAfter: retryAfter,
			Algorithm:  logAlgorithmName,
		}
	}

	for i := 0; i < n; i++ {
		kl.timestamps = append(kl.timestamps, now)
	}
	return ratelimit.Result{
		Allowed:   true,
		Limit:     l.limit,
		Remaining: l.limit - count - n,
		Algorithm: logAlgorithmName,
	}
}

// pruneLocked removes timestamps older than cutoff. Must hold kl.mu.
func (l *SlidingWindowLog) pruneLocked(kl *keyLog, cutoff time.Time) {
	idx := sort.Search(len(kl.timestamps), func(i int) bool {
		return !kl.timestamps[i].Before(cutoff)
	})
	if idx > 0 {
		kl.timestamps = kl.timestamps[idx:]
	}
}

func (l *SlidingWindowLog) cleanupLoop() {
	defer l.wg.Done()
	ticker := l.clock.NewTicker(l.window)
	defer ticker.Stop()
	for {
		select {
		case <-l.done:
			return
		case <-ticker.C():
			cutoff := l.clock.Now().Add(-l.window * 2)
			l.logs.DeleteMatching(func(_ string, kl *keyLog) bool {
				kl.mu.Lock()
				idle := kl.lastAccess.Before(cutoff)
				kl.mu.Unlock()
				return idle
			})
		}
	}
}

// String returns a human-readable description.
func (l *SlidingWindowLog) String() string {
	return fmt.Sprintf("SlidingWindowLog(limit=%d, window=%s)", l.limit, l.window)
}
