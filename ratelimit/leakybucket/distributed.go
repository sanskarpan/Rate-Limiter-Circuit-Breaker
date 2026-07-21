package leakybucket

import (
	"context"
	"fmt"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/store"
)

// DistributedLeakyBucket is a Redis-backed leaky bucket (ENHANCEMENTS §1.8).
//
// It uses the store.LeakyBucketScriptID Lua script for an atomic read-check-store
// of the bucket's TAT, exploiting the leaky-bucket ⇄ GCRA duality: a request is
// admitted only when the virtual queue would not exceed `capacity` pending
// slots, and the queue drains one slot every emission interval (1/leakRate).
//
// Unlike the in-process LeakyBucket, this type does NOT block or maintain a
// per-key leaker goroutine: Allow/AllowN are non-blocking atomic decisions
// against the shared store, and Wait/WaitN poll with a back-off derived from the
// script's retry_after (matching DistributedGCRA/DistributedTokenBucket). This
// makes the admission decision consistent across a fleet sharing one Redis.
//
// Key naming: {prefix}:leakybucket:{key}
//
// All methods are safe for concurrent use.
type DistributedLeakyBucket struct {
	emissionInterval time.Duration // 1s / leakRate
	capacity         int
	leakRate         float64 // requests per second
	prefix           string
	store            store.Store

	// useServerTime, when true, tells the Lua script to override the client
	// clock with Redis's own TIME (clock-skew mitigation, ENHANCEMENTS §5.1).
	useServerTime bool
}

// DistributedOption configures a distributed leaky bucket.
type DistributedOption func(*DistributedLeakyBucket)

// WithServerTime forces server-time mode on (true) or off (false),
// overriding whatever the underlying store reports via ServerTimeMode(). In
// server-time mode the Lua script uses Redis's TIME command as the authoritative
// clock so application clock skew across a fleet cannot corrupt the decision.
func WithServerTime(on bool) DistributedOption {
	return func(d *DistributedLeakyBucket) { d.useServerTime = on }
}

// WithDistributedServerTime is a deprecated alias for WithServerTime.
//
// Deprecated: Use WithServerTime instead.
var WithDistributedServerTime = WithServerTime

// NewDistributed creates a Redis-backed leaky bucket.
// capacity is the queue depth and leakRate is the constant drain rate in
// requests/second. It panics if capacity <= 0 or leakRate <= 0, mirroring the
// in-process New so misconfiguration fails fast rather than silently building a
// bucket that never drains.
//
// By default it inherits server-time mode from the store
// (store.RedisOptions.UseServerTime); pass WithServerTime(true|false)
// to override.
func NewDistributed(capacity int, leakRate float64, s store.Store, prefix string, opts ...DistributedOption) *DistributedLeakyBucket {
	if capacity <= 0 {
		panic(fmt.Sprintf("leakybucket.NewDistributed: capacity must be > 0, got %d", capacity))
	}
	if leakRate <= 0 {
		panic(fmt.Sprintf("leakybucket.NewDistributed: leakRate must be > 0, got %v", leakRate))
	}
	if prefix == "" {
		prefix = "rl"
	}
	d := &DistributedLeakyBucket{
		emissionInterval: time.Duration(float64(time.Second) / leakRate),
		capacity:         capacity,
		leakRate:         leakRate,
		prefix:           prefix,
		store:            s,
	}
	if stc, ok := s.(store.ServerTimeable); ok {
		d.useServerTime = stc.ServerTimeMode()
	}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

func (d *DistributedLeakyBucket) serverTimeArg() int {
	if d.useServerTime {
		return 1
	}
	return 0
}

func (d *DistributedLeakyBucket) redisKey(key string) string {
	return fmt.Sprintf("%s:leakybucket:%s", d.prefix, key)
}

const distributedAlgorithmName = "distributed_leaky_bucket"

// Allow checks if 1 request can be queued and, if so, records it.
func (d *DistributedLeakyBucket) Allow(ctx context.Context, key string) ratelimit.Result {
	return d.AllowN(ctx, key, 1)
}

// AllowN checks if n requests can be queued atomically, all-or-nothing.
//
// It validates inputs the same way the in-memory limiters do (reject
// empty/injection keys, n < 1, and n exceeding capacity — which can never be
// satisfied) rather than forwarding them to Redis. On a store error it FAILS
// OPEN by denying the request, matching the other distributed limiters.
func (d *DistributedLeakyBucket) AllowN(ctx context.Context, key string, n int) ratelimit.Result {
	if err := ratelimit.ValidateKey(key); err != nil {
		return ratelimit.Result{Allowed: false, Limit: d.capacity, Algorithm: distributedAlgorithmName}
	}
	if err := ratelimit.ValidateN(n); err != nil {
		return ratelimit.Result{Allowed: false, Limit: d.capacity, Algorithm: distributedAlgorithmName}
	}
	if n > d.capacity {
		return ratelimit.Result{Allowed: false, Limit: d.capacity, Remaining: 0, Algorithm: distributedAlgorithmName}
	}

	nowNs := time.Now().UnixNano()
	// Time to drain a full queue plus a 1s safety margin, for the key's PEXPIRE.
	ttlMs := int64(d.emissionInterval*time.Duration(d.capacity))/int64(time.Millisecond) + 1000
	if ttlMs < 1000 {
		ttlMs = 1000
	}

	result, err := d.store.Eval(ctx, store.LeakyBucketScriptID,
		[]string{d.redisKey(key)},
		int64(d.emissionInterval), d.capacity, n, nowNs, ttlMs, d.serverTimeArg(),
	)
	if err != nil {
		// On store error, deny to be safe (consistent with the other distributed
		// limiters' fail-open-to-deny behaviour).
		return ratelimit.Result{Allowed: false, Limit: d.capacity, Algorithm: distributedAlgorithmName}
	}

	arr, ok := result.([]any)
	if !ok || len(arr) < 3 {
		return ratelimit.Result{Allowed: false, Limit: d.capacity, Algorithm: distributedAlgorithmName}
	}

	allowed, _ := arr[0].(int64)
	depth, _ := arr[1].(int64)
	retryAfterNs, _ := arr[2].(int64)

	remaining := d.capacity - int(depth)
	if remaining < 0 {
		remaining = 0
	}
	return ratelimit.Result{
		Allowed:    allowed == 1,
		Limit:      d.capacity,
		Remaining:  remaining,
		RetryAfter: time.Duration(retryAfterNs),
		Algorithm:  distributedAlgorithmName,
	}
}

// Wait blocks until the request can be queued or the context is cancelled.
func (d *DistributedLeakyBucket) Wait(ctx context.Context, key string) error {
	return d.WaitN(ctx, key, 1)
}

// WaitN blocks until n requests can be queued or the context is cancelled.
// It polls AllowN with a back-off derived from the script's retry_after,
// mirroring DistributedGCRA.WaitN.
func (d *DistributedLeakyBucket) WaitN(ctx context.Context, key string, n int) error {
	if err := ratelimit.ValidateKey(key); err != nil {
		return err
	}
	if err := ratelimit.ValidateN(n); err != nil {
		return err
	}
	if n > d.capacity {
		return &ratelimit.RateLimitError{
			Algorithm: distributedAlgorithmName,
			Key:       key,
			Limit:     d.capacity,
			Err:       fmt.Errorf("%w: n=%d exceeds capacity=%d", ratelimit.ErrLimitExceeded, n, d.capacity),
		}
	}
	for {
		result := d.AllowN(ctx, key, n)
		if result.Allowed {
			return nil
		}
		if ctx.Err() != nil {
			return &ratelimit.RateLimitError{
				Algorithm:  distributedAlgorithmName,
				Key:        key,
				Limit:      d.capacity,
				RetryAfter: result.RetryAfter,
				Err:        ratelimit.ErrContextDone,
			}
		}
		waitFor := result.RetryAfter
		if waitFor <= 0 {
			waitFor = d.emissionInterval
		}
		timer := time.NewTimer(waitFor)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return &ratelimit.RateLimitError{
				Algorithm:  distributedAlgorithmName,
				Key:        key,
				Limit:      d.capacity,
				RetryAfter: waitFor,
				Err:        ratelimit.ErrContextDone,
			}
		}
	}
}

// Peek returns current state without consuming a slot. The distributed leaky
// bucket keeps no local state, so this reports only the configured limit.
func (d *DistributedLeakyBucket) Peek(_ context.Context, key string) ratelimit.State {
	return ratelimit.State{
		Key:       key,
		Algorithm: distributedAlgorithmName,
		Limit:     d.capacity,
	}
}

// Reset deletes the stored TAT for a key.
func (d *DistributedLeakyBucket) Reset(ctx context.Context, key string) error {
	return d.store.Del(ctx, d.redisKey(key))
}

// Close is a no-op — the underlying store is managed by the caller.
func (d *DistributedLeakyBucket) Close() error { return nil }
