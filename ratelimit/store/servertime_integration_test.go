//go:build integration

// Integration tests for Redis clock-skew mitigation (ENHANCEMENTS §5.1).
// Run with: go test ./ratelimit/store/ -tags=integration -run ServerTime
// Requires a Redis instance at localhost:6379 or REDIS_ADDR env var.
package store_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/store"
)

// TestRedis_ServerTimeSkew_Small asserts that against a real, co-located Redis
// the measured skew is small (well under a second). CI machines and Redis share
// a clock source, so the only error is the RTT/2 measurement noise.
func TestRedis_ServerTimeSkew_Small(t *testing.T) {
	s := newTestRedis(t)
	ctx := context.Background()

	skew, err := s.ServerTimeSkew(ctx)
	if err != nil {
		t.Fatalf("ServerTimeSkew: %v", err)
	}
	abs := skew
	if abs < 0 {
		abs = -abs
	}
	t.Logf("measured server-time skew = %v", skew)
	if abs > time.Second {
		t.Fatalf("expected small skew against local Redis, got %v", skew)
	}

	// CheckServerTimeSkew with the default threshold must report not-exceeded.
	sk2, exceeded, err := s.CheckServerTimeSkew(ctx, 0)
	if err != nil {
		t.Fatalf("CheckServerTimeSkew: %v", err)
	}
	if exceeded {
		t.Fatalf("expected skew %v not to exceed default threshold %v", sk2, store.ServerTimeSkewThreshold)
	}

	// A deliberately tiny threshold should trip (skew, even if ~microseconds,
	// exceeds a 1ns threshold once RTT noise is present); we only assert the API
	// shape here, not a specific bool, to avoid flakiness on a zero-RTT loopback.
	_, _, err = s.CheckServerTimeSkew(ctx, time.Nanosecond)
	if err != nil {
		t.Fatalf("CheckServerTimeSkew(1ns): %v", err)
	}
}

// TestRedis_ServerTime_TokenBucket_IgnoresClientSkew is the core test: with
// server-time mode ON, a deliberately-skewed client `now` (now + 10s) must NOT
// corrupt the token-bucket decision, because the script uses the Redis clock.
// In client-time mode the same skew jump would over-refill the bucket.
func TestRedis_ServerTime_TokenBucket_IgnoresClientSkew(t *testing.T) {
	ctx := context.Background()

	// Two independent stores over the same key space: one server-time, one client.
	prefix := fmt.Sprintf("test:st:tb:%d:", time.Now().UnixNano())
	srvStore := store.NewRedis(store.RedisOptions{Addr: redisAddr(), KeyPrefix: prefix + "srv:", UseServerTime: true})
	cliStore := store.NewRedis(store.RedisOptions{Addr: redisAddr(), KeyPrefix: prefix + "cli:", UseServerTime: false})
	if err := srvStore.Ping(ctx); err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	t.Cleanup(func() { srvStore.Close(); cliStore.Close() })

	const capacity = 5.0
	const refillRate = 1.0 / float64(time.Second) // 1 token/sec
	const ttlMs = 60000

	realNow := time.Now().UnixNano()
	skewedNow := realNow + int64(10*time.Second) // +10s app clock skew

	// Drain the bucket to 0 at realNow (5 tokens, capacity 5), server-time mode.
	drain := func(s *store.Redis, nowNs int64) int {
		allowed := 0
		for i := 0; i < 5; i++ {
			res, err := s.Eval(ctx, store.TokenBucketScript,
				[]string{"k"}, capacity, refillRate, 1, nowNs, ttlMs, 1,
			)
			if err != nil {
				t.Fatalf("eval: %v", err)
			}
			if res.([]any)[0].(int64) == 1 {
				allowed++
			}
		}
		return allowed
	}
	if got := drain(srvStore, realNow); got != 5 {
		t.Fatalf("server-time: expected to drain 5 tokens, got %d", got)
	}

	// Now immediately request again with a +10s skewed client clock. In
	// server-time mode the script ignores skewedNow and uses the (barely-advanced)
	// Redis clock, so almost no refill has happened and the request is DENIED.
	res, err := srvStore.Eval(ctx, store.TokenBucketScript,
		[]string{"k"}, capacity, refillRate, 1, skewedNow, ttlMs, 1,
	)
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if res.([]any)[0].(int64) == 1 {
		t.Fatal("server-time mode: skewed +10s client clock must NOT refill the bucket (should deny)")
	}

	// Contrast: client-time mode. Drain, then the +10s skewed now over-refills
	// (10 tokens > capacity) so the request is wrongly ALLOWED — demonstrating the
	// skew corruption that server-time mode prevents.
	if got := drain(cliStore, realNow); got != 5 {
		t.Fatalf("client-time: expected to drain 5 tokens, got %d", got)
	}
	res2, err := cliStore.Eval(ctx, store.TokenBucketScript,
		[]string{"k"}, capacity, refillRate, 1, skewedNow, ttlMs, 0,
	)
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if res2.([]any)[0].(int64) != 1 {
		t.Fatal("client-time mode: +10s skew SHOULD over-refill and allow (baseline for contrast)")
	}
}

// TestRedis_ServerTime_ReplicateCommands_NoError ensures the replicate_commands
// preamble does not error on this Redis version for any of the three scripts.
func TestRedis_ServerTime_ReplicateCommands_NoError(t *testing.T) {
	s := newTestRedis(t)
	ctx := context.Background()
	now := time.Now().UnixNano()

	if _, err := s.Eval(ctx, store.TokenBucketScript,
		[]string{"rc:tb"}, 5.0, 1.0/float64(time.Second), 1, now, 60000, 1); err != nil {
		t.Fatalf("token bucket server-time eval: %v", err)
	}
	if _, err := s.Eval(ctx, store.GCRAScript,
		[]string{"rc:gcra"}, int64(time.Second), 5, 1, now, 60000, 1); err != nil {
		t.Fatalf("gcra server-time eval: %v", err)
	}
	if _, err := s.Eval(ctx, store.SlidingWindowLogScript,
		[]string{"rc:swl"}, 5, int64(time.Second), now, "id", 60000, 1, 1); err != nil {
		t.Fatalf("sliding window log server-time eval: %v", err)
	}
}
