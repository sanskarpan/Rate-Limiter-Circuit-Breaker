package tokenbucket

import (
	"context"
	"fmt"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/store"
)

// DistributedTokenBucket is a Redis-backed token bucket.
// It uses a Lua script for atomic read-refill-consume operations.
// Key naming: {prefix}:tokenbucket:{key}
type DistributedTokenBucket struct {
	capacity   float64
	refillRate float64 // tokens per nanosecond
	prefix     string
	store      store.Store
}

// NewDistributed creates a Redis-backed token bucket.
// rate is tokens per second, capacity is the maximum burst size.
func NewDistributed(rate, capacity float64, s store.Store, prefix string) *DistributedTokenBucket {
	if prefix == "" {
		prefix = "rl"
	}
	return &DistributedTokenBucket{
		capacity:   capacity,
		refillRate: rate / float64(time.Second),
		prefix:     prefix,
		store:      s,
	}
}

func (d *DistributedTokenBucket) redisKey(key string) string {
	return fmt.Sprintf("%s:tokenbucket:%s", d.prefix, key)
}

// Allow checks if 1 token is available and consumes it if so.
func (d *DistributedTokenBucket) Allow(ctx context.Context, key string) ratelimit.Result {
	return d.AllowN(ctx, key, 1)
}

// AllowN checks if n tokens are available and consumes them atomically.
func (d *DistributedTokenBucket) AllowN(ctx context.Context, key string, n int) ratelimit.Result {
	// Validate inputs the same way the in-memory TokenBucket does (H-7/TB-1):
	// reject empty/oversized/injection keys and n < 1 rather than silently
	// allowing an n=0 no-op or refunding on n<0, and never forward an unvalidated
	// key into Redis.
	if err := ratelimit.ValidateKey(key); err != nil {
		return ratelimit.Result{Allowed: false, Limit: int(d.capacity), Algorithm: "distributed_token_bucket"}
	}
	if err := ratelimit.ValidateN(n); err != nil {
		return ratelimit.Result{Allowed: false, Limit: int(d.capacity), Algorithm: "distributed_token_bucket"}
	}

	nowNs := time.Now().UnixNano()
	// Time to fully refill the bucket plus a 1s safety margin. This is now passed
	// into the script's PEXPIRE (L-1/TB-2) instead of being computed and discarded.
	ttlMs := int64(d.capacity/d.refillRate/float64(time.Millisecond)) + 1000

	result, err := d.store.Eval(ctx, store.TokenBucketScript,
		[]string{d.redisKey(key)},
		d.capacity, d.refillRate, n, nowNs, ttlMs,
	)
	if err != nil {
		// On error, deny to be safe
		return ratelimit.Result{
			Allowed:   false,
			Limit:     int(d.capacity),
			Algorithm: "distributed_token_bucket",
		}
	}

	arr, ok := result.([]any)
	if !ok || len(arr) < 2 {
		return ratelimit.Result{
			Allowed:   false,
			Limit:     int(d.capacity),
			Algorithm: "distributed_token_bucket",
		}
	}

	allowed, _ := arr[0].(int64)
	remaining, _ := arr[1].(int64)

	return ratelimit.Result{
		Allowed:   allowed == 1,
		Limit:     int(d.capacity),
		Remaining: int(remaining),
		Algorithm: "distributed_token_bucket",
	}
}

// Wait blocks until a token is available or context is cancelled.
func (d *DistributedTokenBucket) Wait(ctx context.Context, key string) error {
	return d.WaitN(ctx, key, 1)
}

// WaitN blocks until n tokens are available or context is cancelled.
func (d *DistributedTokenBucket) WaitN(ctx context.Context, key string, n int) error {
	for {
		result := d.AllowN(ctx, key, n)
		if result.Allowed {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Wait for estimated refill time
		waitFor := time.Duration(float64(n)/d.refillRate) + time.Millisecond
		timer := time.NewTimer(waitFor)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}
}

// Peek returns current state without consuming a token.
func (d *DistributedTokenBucket) Peek(ctx context.Context, key string) ratelimit.State {
	return ratelimit.State{
		Key:       key,
		Algorithm: "distributed_token_bucket",
		Limit:     int(d.capacity),
	}
}

// Reset deletes the state for a key.
func (d *DistributedTokenBucket) Reset(ctx context.Context, key string) error {
	return d.store.Del(ctx, d.redisKey(key))
}

// Close is a no-op — the underlying store is managed by the caller.
func (d *DistributedTokenBucket) Close() error { return nil }
