package gcra

import (
	"context"
	"fmt"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/store"
)
// distributedAlgorithmName is the algorithm label emitted in Result fields.
const distributedAlgorithmName = "distributed_gcra"


// DistributedGCRA is a Redis-backed GCRA (Generic Cell Rate Algorithm) limiter.
// Uses the Lua GCRAScript for atomic TAT updates.
// Key naming: {prefix}:gcra:{key}
type DistributedGCRA struct {
	emissionInterval time.Duration // 1s / rate
	burst            int
	prefix           string
	store            store.Store

	// useServerTime, when true, tells the Lua script to override the client
	// clock with Redis's own TIME (clock-skew mitigation, ENHANCEMENTS §5.1).
	useServerTime bool
}

// DistributedOption configures a distributed GCRA limiter.
type DistributedOption func(*DistributedGCRA)

// WithServerTime forces server-time mode on (true) or off (false), overriding
// whatever the underlying store reports via ServerTimeMode(). In server-time
// mode the Lua script uses Redis's TIME command as the authoritative clock so
// application clock skew across a fleet cannot corrupt the decision.
func WithServerTime(on bool) DistributedOption {
	return func(d *DistributedGCRA) { d.useServerTime = on }
}

// NewDistributed creates a Redis-backed GCRA limiter.
// rate is requests per second, burst is the allowed burst size.
//
// By default it inherits server-time mode from the store; pass
// WithServerTime(true|false) to override.
func NewDistributed(rate float64, burst int, s store.Store, prefix string, opts ...DistributedOption) *DistributedGCRA {
	if prefix == "" {
		prefix = "rl"
	}
	d := &DistributedGCRA{
		emissionInterval: time.Duration(float64(time.Second) / rate),
		burst:            burst,
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

func (d *DistributedGCRA) serverTimeArg() int {
	if d.useServerTime {
		return 1
	}
	return 0
}

func (d *DistributedGCRA) redisKey(key string) string {
	return fmt.Sprintf("%s:gcra:%s", d.prefix, key)
}

// Allow checks if 1 request is allowed.
func (d *DistributedGCRA) Allow(ctx context.Context, key string) ratelimit.Result {
	return d.AllowN(ctx, key, 1)
}

// AllowN checks if n requests are allowed atomically.
func (d *DistributedGCRA) AllowN(ctx context.Context, key string, n int) ratelimit.Result {
	// Validate inputs the same way the in-memory GCRA does (H-7/GCRA-1):
	// reject empty/injection keys, n < 1, and n exceeding the burst ceiling
	// (which can never be satisfied) instead of forwarding them to Redis.
	if err := ratelimit.ValidateKey(key); err != nil {
		return ratelimit.Result{Allowed: false, Limit: d.burst, Algorithm: distributedAlgorithmName}
	}
	if err := ratelimit.ValidateN(n); err != nil {
		return ratelimit.Result{Allowed: false, Limit: d.burst, Algorithm: distributedAlgorithmName}
	}
	if n > d.burst {
		return ratelimit.Result{Allowed: false, Limit: d.burst, Remaining: 0, Algorithm: distributedAlgorithmName}
	}

	nowNs := time.Now().UnixNano()
	ttlMs := int64(d.emissionInterval*time.Duration(d.burst)) / int64(time.Millisecond)
	if ttlMs < 1000 {
		ttlMs = 1000
	}

	result, err := d.store.Eval(ctx, store.GCRAScriptID,
		[]string{d.redisKey(key)},
		int64(d.emissionInterval), d.burst, n, nowNs, ttlMs, d.serverTimeArg(),
	)
	if err != nil {
		return ratelimit.Result{
			Allowed:   false,
			Algorithm: distributedAlgorithmName,
		}
	}

	arr, ok := result.([]any)
	if !ok || len(arr) < 2 {
		return ratelimit.Result{
			Allowed:   false,
			Algorithm: distributedAlgorithmName,
		}
	}

	allowed, _ := arr[0].(int64)
	retryAfterNs, _ := arr[1].(int64)

	return ratelimit.Result{
		Allowed:    allowed == 1,
		Algorithm:  distributedAlgorithmName,
		RetryAfter: time.Duration(retryAfterNs),
	}
}

// Wait blocks until a request is allowed or context is cancelled.
func (d *DistributedGCRA) Wait(ctx context.Context, key string) error {
	return d.WaitN(ctx, key, 1)
}

// WaitN blocks until n requests are allowed or context is cancelled.
func (d *DistributedGCRA) WaitN(ctx context.Context, key string, n int) error {
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
			waitFor = d.emissionInterval
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
func (d *DistributedGCRA) Peek(ctx context.Context, key string) ratelimit.State {
	return ratelimit.State{
		Key:       key,
		Algorithm: distributedAlgorithmName,
	}
}

// Reset deletes the TAT for a key.
func (d *DistributedGCRA) Reset(ctx context.Context, key string) error {
	return d.store.Del(ctx, d.redisKey(key))
}

// Close is a no-op.
func (d *DistributedGCRA) Close() error { return nil }
