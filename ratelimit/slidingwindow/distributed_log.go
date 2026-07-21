package slidingwindow

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/store"
)

// distributedLogAlgorithmName is the algorithm label emitted in Result fields.
const distributedLogAlgorithmName = "distributed_sliding_window_log"

// DistributedSlidingWindowLog is a Redis sorted set-backed sliding window log.
// Uses ZADD + ZCOUNT + ZREMRANGEBYSCORE in a Lua script for atomicity.
// Key naming: {prefix}:swlog:{key}
type DistributedSlidingWindowLog struct {
	limit  int
	window time.Duration
	prefix string
	store  store.Store

	// seq gives every AllowN call a process-unique member prefix so two calls
	// in the same nanosecond cannot generate colliding ZSET members (H-2/SWL-D2).
	seq atomic.Uint64

	// useServerTime, when true, tells the Lua script to override the client
	// clock with Redis's own TIME (clock-skew mitigation, ENHANCEMENTS §5.1).
	useServerTime bool
}

// DistributedLogOption configures a distributed sliding-window-log limiter.
type DistributedLogOption func(*DistributedSlidingWindowLog)

// WithServerTime forces server-time mode on (true) or off (false), overriding
// whatever the underlying store reports via ServerTimeMode(). In server-time
// mode the Lua script uses Redis's TIME command as the authoritative clock so
// application clock skew across a fleet cannot corrupt the decision.
func WithServerTime(on bool) DistributedLogOption {
	return func(d *DistributedSlidingWindowLog) { d.useServerTime = on }
}

// NewDistributedLog creates a Redis-backed sliding window log.
//
// By default it inherits server-time mode from the store; pass
// WithServerTime(true|false) to override.
func NewDistributedLog(limit int, window time.Duration, s store.Store, prefix string, opts ...DistributedLogOption) *DistributedSlidingWindowLog {
	if prefix == "" {
		prefix = "rl"
	}
	d := &DistributedSlidingWindowLog{
		limit:  limit,
		window: window,
		prefix: prefix,
		store:  s,
	}
	if stc, ok := s.(store.ServerTimeable); ok {
		d.useServerTime = stc.ServerTimeMode()
	}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

func (d *DistributedSlidingWindowLog) serverTimeArg() int {
	if d.useServerTime {
		return 1
	}
	return 0
}

func (d *DistributedSlidingWindowLog) redisKey(key string) string {
	return fmt.Sprintf("%s:swlog:%s", d.prefix, key)
}

// Allow checks if 1 request is allowed.
func (d *DistributedSlidingWindowLog) Allow(ctx context.Context, key string) ratelimit.Result {
	return d.AllowN(ctx, key, 1)
}

// AllowN checks if n requests are allowed.
func (d *DistributedSlidingWindowLog) AllowN(ctx context.Context, key string, n int) ratelimit.Result {
	// Validate inputs to match the local SlidingWindowLog (reject bad keys / n<1).
	if err := ratelimit.ValidateKey(key); err != nil {
		return ratelimit.Result{Allowed: false, Limit: d.limit, Algorithm: distributedLogAlgorithmName}
	}
	if err := ratelimit.ValidateN(n); err != nil {
		return ratelimit.Result{Allowed: false, Limit: d.limit, Algorithm: distributedLogAlgorithmName}
	}

	now := time.Now()
	nowNs := now.UnixNano()
	ttlMs := d.window.Milliseconds() + 1000
	// Unique per-call prefix: nanosecond timestamp + a monotonically increasing
	// sequence. The script suffixes each of the n members with "-<i>", so all n
	// members admitted by this call are distinct AND distinct from every other
	// call's members (H-2). Passing n as the final arg lets the script admit all
	// n or none and deny on count+n>limit (H-1).
	entryID := fmt.Sprintf("%d-%d", nowNs, d.seq.Add(1))

	result, err := d.store.Eval(ctx, store.SlidingWindowLogScriptID,
		[]string{d.redisKey(key)},
		d.limit, int64(d.window), nowNs, entryID, ttlMs, n, d.serverTimeArg(),
	)
	if err != nil {
		return ratelimit.Result{
			Allowed:   false,
			Limit:     d.limit,
			Algorithm: distributedLogAlgorithmName,
		}
	}

	arr, ok := result.([]any)
	if !ok || len(arr) < 3 {
		return ratelimit.Result{
			Allowed:   false,
			Limit:     d.limit,
			Algorithm: distributedLogAlgorithmName,
		}
	}

	allowed, _ := arr[0].(int64)
	count, _ := arr[1].(int64)
	retryAfterNs, _ := arr[2].(int64)

	remaining := d.limit - int(count)
	if remaining < 0 {
		remaining = 0
	}

	return ratelimit.Result{
		Allowed:    allowed == 1,
		Limit:      d.limit,
		Remaining:  remaining,
		RetryAfter: time.Duration(retryAfterNs),
		Algorithm:  distributedLogAlgorithmName,
	}
}

// Wait blocks until a request is allowed.
func (d *DistributedSlidingWindowLog) Wait(ctx context.Context, key string) error {
	return d.WaitN(ctx, key, 1)
}

// WaitN blocks until n requests are allowed.
func (d *DistributedSlidingWindowLog) WaitN(ctx context.Context, key string, n int) error {
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
func (d *DistributedSlidingWindowLog) Peek(ctx context.Context, key string) ratelimit.State {
	return ratelimit.State{
		Key:       key,
		Algorithm: distributedLogAlgorithmName,
		Limit:     d.limit,
	}
}

// Reset deletes the sorted set for a key.
func (d *DistributedSlidingWindowLog) Reset(ctx context.Context, key string) error {
	return d.store.Del(ctx, d.redisKey(key))
}

// Close is a no-op.
func (d *DistributedSlidingWindowLog) Close() error { return nil }
