// Package tokenbucket implements the Token Bucket rate limiting algorithm.
//
// Theory: A bucket holds up to capacity tokens. Tokens are added at refillRate
// tokens/second. Each request consumes one or more tokens. If the bucket is
// empty, the request is denied (or queued via Wait).
//
// Properties:
//   - Allows bursting up to capacity tokens
//   - Refills lazily (no background goroutine needed)
//   - O(1) Allow() time and space
//   - Zero external dependencies
//
// Time complexity: O(1) per Allow call.
// Space complexity: O(keys) — one bucket per unique key.
//
// Reference: https://en.wikipedia.org/wiki/Token_bucket
//
// All methods on TokenBucket are safe for concurrent use.
package tokenbucket

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/atomicx"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/metric"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
)

const algorithmName = "token_bucket"

// bucket holds per-key state.
type bucket struct {
	mu         sync.Mutex
	tokens     float64
	lastRefill time.Time
	lastAccess time.Time
}

// TokenBucket implements the Limiter interface using the token bucket algorithm.
// All methods are safe for concurrent use.
type TokenBucket struct {
	// capacity and refillRate are stored atomically so SetLimit can mutate them
	// in place without a lock on the hot path (H-12).
	capacity   atomicx.Float64
	refillRate atomicx.Float64 // tokens per second
	idleClean  time.Duration

	mu      sync.RWMutex
	buckets map[string]*bucket

	clock  clock.Clock
	rec    metric.Recorder
	done   chan struct{}
	wg     sync.WaitGroup
	closed bool

	// onDecision, when non-nil, is fired synchronously after every Allow/AllowN
	// decision. nil by default so the hot path stays a single branch-predicted
	// nil check. See WithOnDecision.
	onDecision func(key string, r ratelimit.Result)
}

// New creates a new TokenBucket with the given capacity and refill rate (tokens/second).
//
// It panics if capacity < 0 or refillRate <= 0 — an invalid configuration
// cannot produce a working limiter (a zero/negative refill rate never
// replenishes tokens and would cause Wait/WaitN to block forever). A capacity
// of exactly 0 is permitted and yields a deny-all limiter. This mirrors the
// standard library convention (e.g. time.NewTicker panics on a non-positive
// interval).
func New(capacity float64, refillRate float64, opts ...Option) *TokenBucket {
	if capacity < 0 {
		panic(fmt.Sprintf("tokenbucket.New: capacity must be >= 0, got %v", capacity))
	}
	if refillRate <= 0 {
		panic(fmt.Sprintf("tokenbucket.New: refillRate must be > 0, got %v", refillRate))
	}
	tb := &TokenBucket{
		idleClean: 5 * time.Minute,
		clock:     clock.RealClock{},
		rec:       metric.Default(),
		done:      make(chan struct{}),
		buckets:   make(map[string]*bucket),
	}
	tb.capacity.Store(capacity)
	tb.refillRate.Store(refillRate)
	for _, opt := range opts {
		opt(tb)
	}
	tb.wg.Add(1)
	go tb.cleanupLoop()
	return tb
}

// Allow checks if 1 token is available for the given key.
// Non-blocking. Safe for concurrent use.
func (tb *TokenBucket) Allow(ctx context.Context, key string) ratelimit.Result {
	return tb.AllowN(ctx, key, 1)
}

// AllowN checks if n tokens are available. Consumes all n or none (atomic).
// Non-blocking. Safe for concurrent use.
func (tb *TokenBucket) AllowN(_ context.Context, key string, n int) (res ratelimit.Result) {
	start := tb.clock.Now()
	defer func() {
		tb.record(res, start)
		if tb.onDecision != nil {
			tb.onDecision(key, res)
		}
	}()

	if err := ratelimit.ValidateKey(key); err != nil {
		return ratelimit.Result{Allowed: false, Algorithm: algorithmName}
	}
	if err := ratelimit.ValidateN(n); err != nil {
		return ratelimit.Result{Allowed: false, Algorithm: algorithmName}
	}
	b := tb.getOrCreate(key)
	res = tb.consume(b, float64(n))
	if n != 1 {
		setCost(&res, n)
	}
	return res
}

// AllowCost is the fractional-cost variant of AllowN: it consumes cost tokens
// all-or-nothing, where cost may be non-integer (the token bucket stores
// fractional tokens internally). Integer AllowN remains the default; use this
// only when a weighted, non-integer cost model is required.
//
// A cost <= 0 is rejected as an invalid request (Allowed=false) to match the
// n >= 1 contract of AllowN.
func (tb *TokenBucket) AllowCost(_ context.Context, key string, cost float64) (res ratelimit.Result) {
	start := tb.clock.Now()
	defer func() {
		tb.record(res, start)
		if tb.onDecision != nil {
			tb.onDecision(key, res)
		}
	}()

	if err := ratelimit.ValidateKey(key); err != nil {
		return ratelimit.Result{Allowed: false, Algorithm: algorithmName}
	}
	if cost <= 0 {
		return ratelimit.Result{Allowed: false, Algorithm: algorithmName}
	}
	b := tb.getOrCreate(key)
	res = tb.consume(b, cost)
	setCost(&res, cost)
	return res
}

// setCost records the consumed cost in res.Metadata under the "cost" key,
// allocating the map lazily so the n==1 hot path stays allocation-free. The
// integer AllowN path stores an int; the fractional AllowCost path stores a
// float64.
func setCost(res *ratelimit.Result, cost any) {
	if res.Metadata == nil {
		res.Metadata = ratelimit.Metadata{}
	}
	res.Metadata["cost"] = cost
}

// record fires the configured metric.Recorder for a completed decision. With
// the default Nop recorder every method is an empty inlined call, so this stays
// allocation-free on the hot path.
func (tb *TokenBucket) record(res ratelimit.Result, start time.Time) {
	if res.Allowed {
		tb.rec.IncAllowed(algorithmName)
	} else {
		tb.rec.IncDenied(algorithmName)
	}
	tb.rec.ObserveDecision(algorithmName, tb.clock.Now().Sub(start))
}

// Wait blocks until 1 token is available or ctx is cancelled.
func (tb *TokenBucket) Wait(ctx context.Context, key string) error {
	return tb.WaitN(ctx, key, 1)
}

// WaitN blocks until n tokens are available or ctx is cancelled.
func (tb *TokenBucket) WaitN(ctx context.Context, key string, n int) error {
	if err := ratelimit.ValidateKey(key); err != nil {
		return err
	}
	if err := ratelimit.ValidateN(n); err != nil {
		return err
	}
	// Impossible request: n exceeds capacity, so no amount of refilling will ever
	// satisfy it. Return immediately instead of looping forever (M-4).
	capacity := tb.capacity.Load()
	if float64(n) > capacity {
		return &ratelimit.RateLimitError{
			Algorithm: algorithmName,
			Key:       key,
			Limit:     int(capacity),
			Err:       fmt.Errorf("%w: n=%d exceeds capacity=%.0f", ratelimit.ErrLimitExceeded, n, capacity),
		}
	}
	b := tb.getOrCreate(key)
	for {
		result := tb.consume(b, float64(n))
		if result.Allowed {
			return nil
		}
		wait := result.RetryAfter
		if wait <= 0 {
			wait = time.Duration(float64(n)/tb.refillRate.Load()*float64(time.Second)) + time.Millisecond
		}
		timer := tb.clock.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return &ratelimit.RateLimitError{
				Algorithm:  algorithmName,
				Key:        key,
				Limit:      int(tb.capacity.Load()),
				RetryAfter: wait,
				Err:        ratelimit.ErrContextDone,
			}
		case <-timer.C():
			// retry
		}
	}
}

// Peek returns current state without consuming any tokens.
func (tb *TokenBucket) Peek(_ context.Context, key string) ratelimit.State {
	b := tb.getOrCreate(key)
	b.mu.Lock()
	tb.refill(b)
	tokens := b.tokens
	b.mu.Unlock()
	remaining := int(tokens)
	if remaining < 0 {
		remaining = 0
	}
	capacity := tb.capacity.Load()
	refillRate := tb.refillRate.Load()
	resetAfter := time.Duration((capacity - tokens) / refillRate * float64(time.Second))
	if resetAfter < 0 {
		resetAfter = 0
	}
	return ratelimit.State{
		Key:       key,
		Algorithm: algorithmName,
		Limit:     int(capacity),
		Remaining: remaining,
		ResetAt:   tb.clock.Now().Add(resetAfter),
		Extra: map[string]any{
			"tokens":            tokens,
			"refill_rate_per_s": refillRate,
			"capacity":          capacity,
		},
	}
}

// Reset removes all state for the given key, restoring it to full capacity.
func (tb *TokenBucket) Reset(_ context.Context, key string) error {
	tb.mu.Lock()
	delete(tb.buckets, key)
	tb.mu.Unlock()
	return nil
}

// Close stops background cleanup goroutine.
func (tb *TokenBucket) Close() error {
	tb.mu.Lock()
	if tb.closed {
		tb.mu.Unlock()
		return nil
	}
	tb.closed = true
	tb.mu.Unlock()
	// Signal shutdown AFTER releasing the mutex so cleanupLoop is never
	// blocked on tb.mu.Lock() while Close holds it.
	close(tb.done)
	tb.wg.Wait()
	return nil
}

// String returns a human-readable description.
func (tb *TokenBucket) String() string {
	return fmt.Sprintf("TokenBucket(capacity=%.0f, refillRate=%.0f/s)", tb.capacity.Load(), tb.refillRate.Load())
}

// SetLimit updates the bucket capacity and refill rate in place (H-12).
//
// Existing per-key buckets are preserved: their current token counts are kept
// (clamped down to the new capacity if it shrank) rather than being reset to a
// full burst, and no in-flight Wait/WaitN callers are disrupted. This lets an
// adaptive limiter retune the rate without discarding per-client state or
// closing the bucket out from under callers.
//
// It panics if capacity < 0 or refillRate <= 0, consistent with New.
func (tb *TokenBucket) SetLimit(capacity, refillRate float64) {
	if capacity < 0 {
		panic(fmt.Sprintf("tokenbucket.SetLimit: capacity must be >= 0, got %v", capacity))
	}
	if refillRate <= 0 {
		panic(fmt.Sprintf("tokenbucket.SetLimit: refillRate must be > 0, got %v", refillRate))
	}
	tb.capacity.Store(capacity)
	tb.refillRate.Store(refillRate)

	// Clamp any existing bucket's tokens down to the new capacity so a shrink
	// takes effect immediately instead of leaving stale over-capacity balances.
	tb.mu.RLock()
	buckets := make([]*bucket, 0, len(tb.buckets))
	for _, b := range tb.buckets {
		buckets = append(buckets, b)
	}
	tb.mu.RUnlock()
	for _, b := range buckets {
		b.mu.Lock()
		if b.tokens > capacity {
			b.tokens = capacity
		}
		b.mu.Unlock()
	}
}

// getOrCreate returns the bucket for key, creating it full if needed.
func (tb *TokenBucket) getOrCreate(key string) *bucket {
	tb.mu.RLock()
	b, ok := tb.buckets[key]
	tb.mu.RUnlock()
	if ok {
		return b
	}
	tb.mu.Lock()
	defer tb.mu.Unlock()
	// Double-check after write lock
	if b, ok = tb.buckets[key]; ok {
		return b
	}
	now := tb.clock.Now()
	b = &bucket{tokens: tb.capacity.Load(), lastRefill: now, lastAccess: now}
	tb.buckets[key] = b
	return b
}

// refill computes and applies token accumulation since lastRefill.
// Must be called with b.mu held.
func (tb *TokenBucket) refill(b *bucket) {
	now := tb.clock.Now()
	elapsed := now.Sub(b.lastRefill).Seconds()
	if elapsed > 0 {
		capacity := tb.capacity.Load()
		b.tokens += elapsed * tb.refillRate.Load()
		if b.tokens > capacity {
			b.tokens = capacity
		}
		b.lastRefill = now
	}
	b.lastAccess = now
}

// consume atomically checks and deducts n tokens. Returns detailed result.
func (tb *TokenBucket) consume(b *bucket, n float64) ratelimit.Result {
	capacity := tb.capacity.Load()
	refillRate := tb.refillRate.Load()
	if n > capacity {
		return ratelimit.Result{
			Allowed:    false,
			Limit:      int(capacity),
			Remaining:  0,
			Algorithm:  algorithmName,
			RetryAfter: time.Duration((n - capacity) / refillRate * float64(time.Second)),
		}
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	tokensBefore := b.tokens
	tb.refill(b)
	current := b.tokens

	if current < n {
		waitSecs := (n - current) / refillRate
		retryAfter := time.Duration(waitSecs * float64(time.Second))
		timeToFull := time.Duration((capacity - current) / refillRate * float64(time.Second))
		_ = tokensBefore
		return ratelimit.Result{
			Allowed:    false,
			Limit:      int(capacity),
			Remaining:  int(current),
			ResetAfter: timeToFull,
			RetryAfter: retryAfter,
			Algorithm:  algorithmName,
		}
	}

	b.tokens -= n
	newTokens := b.tokens
	timeToFull := time.Duration((capacity - newTokens) / refillRate * float64(time.Second))
	return ratelimit.Result{
		Allowed:    true,
		Limit:      int(capacity),
		Remaining:  int(newTokens),
		ResetAfter: timeToFull,
		RetryAfter: 0,
		Algorithm:  algorithmName,
	}
}

// cleanupLoop periodically evicts buckets that haven't been accessed recently.
func (tb *TokenBucket) cleanupLoop() {
	defer tb.wg.Done()
	if tb.idleClean <= 0 {
		return
	}
	ticker := tb.clock.NewTicker(tb.idleClean)
	defer ticker.Stop()
	for {
		select {
		case <-tb.done:
			return
		case <-ticker.C():
			cutoff := tb.clock.Now().Add(-tb.idleClean)
			tb.mu.Lock()
			for k, b := range tb.buckets {
				b.mu.Lock()
				idle := b.lastAccess.Before(cutoff)
				b.mu.Unlock()
				if idle {
					delete(tb.buckets, k)
				}
			}
			tb.mu.Unlock()
		}
	}
}
