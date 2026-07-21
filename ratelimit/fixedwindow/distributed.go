package fixedwindow

import (
	"context"
	"fmt"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/store"
)
// distributedAlgorithmName is the algorithm label emitted in Result fields.
const distributedAlgorithmName = "distributed_fixed_window"


// DistributedFixedWindow is a Redis-backed fixed window counter.
// Uses atomic INCR + EXPIRE for O(1) performance.
// Key naming: {prefix}:fixedwindow:{key}:{window_start_unix}
//
// Server-time note (ENHANCEMENTS §5.1): the window boundary is derived in the Go
// caller (from time.Now) and encoded into the key, and the Lua script never
// reads a clock, so this limiter does NOT participate in Redis server-time mode.
// The authoritative clock is the calling host's clock.
type DistributedFixedWindow struct {
	limit  int
	window time.Duration
	prefix string
	store  store.Store
}

// NewDistributed creates a Redis-backed fixed window counter.
func NewDistributed(limit int, window time.Duration, s store.Store, prefix string) *DistributedFixedWindow {
	if prefix == "" {
		prefix = "rl"
	}
	return &DistributedFixedWindow{
		limit:  limit,
		window: window,
		prefix: prefix,
		store:  s,
	}
}

func (d *DistributedFixedWindow) windowKey(key string, now time.Time) string {
	windowStart := now.UnixNano() / int64(d.window) * int64(d.window)
	return fmt.Sprintf("%s:fixedwindow:%s:%d", d.prefix, key, windowStart)
}

// Allow checks if 1 request is allowed in the current window.
func (d *DistributedFixedWindow) Allow(ctx context.Context, key string) ratelimit.Result {
	return d.AllowN(ctx, key, 1)
}

// AllowN checks if n requests are allowed and increments the counter.
func (d *DistributedFixedWindow) AllowN(ctx context.Context, key string, n int) ratelimit.Result {
	// Validate inputs to match the local FixedWindowCounter (reject bad keys/n<1).
	if err := ratelimit.ValidateKey(key); err != nil {
		return ratelimit.Result{Allowed: false, Limit: d.limit, Algorithm: distributedAlgorithmName}
	}
	if err := ratelimit.ValidateN(n); err != nil {
		return ratelimit.Result{Allowed: false, Limit: d.limit, Algorithm: distributedAlgorithmName}
	}

	now := time.Now()
	wKey := d.windowKey(key, now)

	// Calculate retry-after (time until next window).
	windowStartNs := now.UnixNano() / int64(d.window) * int64(d.window)
	nextWindowAt := time.Unix(0, windowStartNs+int64(d.window))
	retryAfter := nextWindowAt.Sub(now)

	// Atomic check-before-increment (H-4/FW-D1): the previous code did IncrBy(n)
	// first and then denied if count>limit with NO rollback, so a rejected
	// AllowN(n>limit) permanently poisoned the window and denied every later
	// request. FixedWindowScript only increments when the request fits, so a
	// rejected AllowN leaves the counter untouched.
	ttlMs := (d.window * 2).Milliseconds()
	result, err := d.store.Eval(ctx, store.FixedWindowScriptID,
		[]string{wKey},
		d.limit, n, ttlMs,
	)
	if err != nil {
		return ratelimit.Result{
			Allowed:   false,
			Limit:     d.limit,
			Algorithm: distributedAlgorithmName,
		}
	}

	arr, ok := result.([]any)
	if !ok || len(arr) < 2 {
		return ratelimit.Result{
			Allowed:   false,
			Limit:     d.limit,
			Algorithm: distributedAlgorithmName,
		}
	}
	allowed, _ := arr[0].(int64)
	count, _ := arr[1].(int64)

	if allowed != 1 {
		return ratelimit.Result{
			Allowed:    false,
			Limit:      d.limit,
			Remaining:  0,
			RetryAfter: retryAfter,
			ResetAfter: retryAfter,
			Algorithm:  distributedAlgorithmName,
		}
	}

	remaining := d.limit - int(count)
	if remaining < 0 {
		remaining = 0
	}
	return ratelimit.Result{
		Allowed:    true,
		Limit:      d.limit,
		Remaining:  remaining,
		ResetAfter: retryAfter,
		Algorithm:  distributedAlgorithmName,
	}
}

// Wait blocks until a request is allowed.
func (d *DistributedFixedWindow) Wait(ctx context.Context, key string) error {
	return d.WaitN(ctx, key, 1)
}

// WaitN blocks until n requests are allowed.
func (d *DistributedFixedWindow) WaitN(ctx context.Context, key string, n int) error {
	for {
		result := d.AllowN(ctx, key, n)
		if result.Allowed {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		waitFor := result.RetryAfter
		if waitFor <= 0 {
			waitFor = d.window
		}
		timer := time.NewTimer(waitFor)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}
}

// Peek returns the current window count without incrementing.
func (d *DistributedFixedWindow) Peek(ctx context.Context, key string) ratelimit.State {
	return ratelimit.State{
		Key:       key,
		Algorithm: distributedAlgorithmName,
		Limit:     d.limit,
	}
}

// Reset deletes all window keys for the given key.
func (d *DistributedFixedWindow) Reset(ctx context.Context, key string) error {
	now := time.Now()
	return d.store.Del(ctx, d.windowKey(key, now))
}

// Close is a no-op.
func (d *DistributedFixedWindow) Close() error { return nil }
