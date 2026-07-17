package slidingwindow

import (
	"context"
	"fmt"
	"time"

	"github.com/sanskarpan/resilience/ratelimit"
	"github.com/sanskarpan/resilience/ratelimit/store"
)

// DistributedSlidingWindowCounter approximates a sliding window using two fixed-window
// counters. The formula is:
//
//	estimated = current_window_count + prev_window_count * (1 - elapsed/window)
//
// Key naming: {prefix}:swcnt:{key}:{window_start_unix}
type DistributedSlidingWindowCounter struct {
	limit  int
	window time.Duration
	prefix string
	store  store.Store
}

// NewDistributedCounter creates a Redis-backed sliding window counter.
func NewDistributedCounter(limit int, window time.Duration, s store.Store, prefix string) *DistributedSlidingWindowCounter {
	if prefix == "" {
		prefix = "rl"
	}
	return &DistributedSlidingWindowCounter{
		limit:  limit,
		window: window,
		prefix: prefix,
		store:  s,
	}
}

func (d *DistributedSlidingWindowCounter) windowKey(key string, windowStart int64) string {
	return fmt.Sprintf("%s:swcnt:%s:%d", d.prefix, key, windowStart)
}

func (d *DistributedSlidingWindowCounter) currentAndPrevWindowStart(now time.Time) (current, prev int64) {
	windowNs := int64(d.window)
	current = now.UnixNano() / windowNs * windowNs
	prev = current - windowNs
	return
}

// Allow checks if 1 request is allowed.
func (d *DistributedSlidingWindowCounter) Allow(ctx context.Context, key string) ratelimit.Result {
	return d.AllowN(ctx, key, 1)
}

// AllowN checks if n requests are allowed using the sliding window approximation.
//
// The read-estimate-check-increment sequence is performed in a single atomic
// script (SlidingWindowCounterScript) rather than a Go-side check followed by a
// separate IncrBy. The old split was non-atomic: concurrent callers could all
// pass the check and then all increment, over-admitting past the limit
// (H-3/SWC-D1). The script also uses a plain float compare (estimated+n<=limit)
// to match the local SlidingWindowCounter instead of math.Ceil which denied
// earlier (M-2/SWC-D2), and sizes the current-window TTL so the previous window
// survives long enough (M-3).
func (d *DistributedSlidingWindowCounter) AllowN(ctx context.Context, key string, n int) ratelimit.Result {
	// Validate inputs to match the local SlidingWindowCounter.
	if err := ratelimit.ValidateKey(key); err != nil {
		return ratelimit.Result{Allowed: false, Limit: d.limit, Algorithm: "distributed_sliding_window_counter"}
	}
	if err := ratelimit.ValidateN(n); err != nil {
		return ratelimit.Result{Allowed: false, Limit: d.limit, Algorithm: "distributed_sliding_window_counter"}
	}

	now := time.Now()
	currentWindowStart, prevWindowStart := d.currentAndPrevWindowStart(now)
	currentKey := d.windowKey(key, currentWindowStart)
	prevKey := d.windowKey(key, prevWindowStart)

	// Elapsed fraction in current window, scaled to integer millionths for ARGV.
	currentWindowAt := time.Unix(0, currentWindowStart)
	elapsed := now.Sub(currentWindowAt)
	fraction := float64(elapsed) / float64(d.window)
	if fraction < 0 {
		fraction = 0
	}
	if fraction > 1 {
		fraction = 1
	}
	fracMillionths := int64(fraction * 1000000)

	// Current-window TTL: it must outlive its role as the "previous" window for
	// the NEXT window, i.e. at least 2*window measured from this window's start.
	// The window has already been running for `elapsed`, so the remaining TTL is
	// 2*window - elapsed (M-3).
	currentTTLms := (2*d.window - elapsed).Milliseconds()
	if currentTTLms < d.window.Milliseconds() {
		currentTTLms = d.window.Milliseconds()
	}

	nextWindowAt := time.Unix(0, currentWindowStart+int64(d.window))
	retryAfter := nextWindowAt.Sub(now)

	result, err := d.store.Eval(ctx, store.SlidingWindowCounterScript,
		[]string{currentKey, prevKey},
		d.limit, n, fracMillionths, currentTTLms,
	)
	if err != nil {
		return ratelimit.Result{
			Allowed:   false,
			Limit:     d.limit,
			Algorithm: "distributed_sliding_window_counter",
		}
	}

	arr, ok := result.([]any)
	if !ok || len(arr) < 2 {
		return ratelimit.Result{
			Allowed:   false,
			Limit:     d.limit,
			Algorithm: "distributed_sliding_window_counter",
		}
	}
	allowed, _ := arr[0].(int64)
	newCount, _ := arr[1].(int64)

	if allowed != 1 {
		return ratelimit.Result{
			Allowed:    false,
			Limit:      d.limit,
			Remaining:  0,
			RetryAfter: retryAfter,
			Algorithm:  "distributed_sliding_window_counter",
		}
	}

	// Recover the estimated previous-window contribution for remaining.
	remaining := d.limit - int(newCount)
	if remaining < 0 {
		remaining = 0
	}
	return ratelimit.Result{
		Allowed:    true,
		Limit:      d.limit,
		Remaining:  remaining,
		ResetAfter: retryAfter,
		Algorithm:  "distributed_sliding_window_counter",
	}
}

// Wait blocks until a request is allowed.
func (d *DistributedSlidingWindowCounter) Wait(ctx context.Context, key string) error {
	return d.WaitN(ctx, key, 1)
}

// WaitN blocks until n requests are allowed.
func (d *DistributedSlidingWindowCounter) WaitN(ctx context.Context, key string, n int) error {
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
			waitFor = 10 * time.Millisecond
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

// Peek returns current state without consuming.
func (d *DistributedSlidingWindowCounter) Peek(ctx context.Context, key string) ratelimit.State {
	return ratelimit.State{
		Key:       key,
		Algorithm: "distributed_sliding_window_counter",
		Limit:     d.limit,
	}
}

// Reset deletes the window keys for the given key.
func (d *DistributedSlidingWindowCounter) Reset(ctx context.Context, key string) error {
	now := time.Now()
	currentWindowStart, prevWindowStart := d.currentAndPrevWindowStart(now)
	return d.store.Del(ctx,
		d.windowKey(key, currentWindowStart),
		d.windowKey(key, prevWindowStart),
	)
}

// Close is a no-op.
func (d *DistributedSlidingWindowCounter) Close() error { return nil }
