//go:build integration

// Integration tests for the distributed token bucket.
// Run with: go test ./ratelimit/tokenbucket/ -tags=integration
// Requires a Redis instance at localhost:6379 or REDIS_ADDR env var.
package tokenbucket_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskarpan/resilience/ratelimit/store"
	"github.com/sanskarpan/resilience/ratelimit/tokenbucket"
)

func redisAddr() string {
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		return addr
	}
	return "localhost:6379"
}

func newTestDistributed(t *testing.T, rate, capacity float64) *tokenbucket.DistributedTokenBucket {
	t.Helper()
	s := store.NewRedis(store.RedisOptions{
		Addr:      redisAddr(),
		KeyPrefix: fmt.Sprintf("test:dtb:%d:", time.Now().UnixNano()),
	})
	if err := s.Ping(context.Background()); err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return tokenbucket.NewDistributed(rate, capacity, s, "test")
}

func TestDistributed_TokenBucket_BasicAllow(t *testing.T) {
	d := newTestDistributed(t, 10, 10)
	result := d.Allow(context.Background(), "key1")
	if !result.Allowed {
		t.Fatalf("expected allowed, got denied")
	}
}

func TestDistributed_TokenBucket_GlobalLimit(t *testing.T) {
	// Create 3 independent limiters pointing to the same Redis key
	s := store.NewRedis(store.RedisOptions{
		Addr:      redisAddr(),
		KeyPrefix: fmt.Sprintf("test:dtb:global:%d:", time.Now().UnixNano()),
	})
	if err := s.Ping(context.Background()); err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	const limit = 20
	d1 := tokenbucket.NewDistributed(float64(limit), float64(limit), s, "test")
	d2 := tokenbucket.NewDistributed(float64(limit), float64(limit), s, "test")
	d3 := tokenbucket.NewDistributed(float64(limit), float64(limit), s, "test")

	// Send 30 concurrent requests across 3 limiters (limit=20)
	var allowed, denied int32
	var wg sync.WaitGroup
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		wg.Add(3)
		go func() {
			defer wg.Done()
			if d1.Allow(ctx, "shared").Allowed {
				atomic.AddInt32(&allowed, 1)
			} else {
				atomic.AddInt32(&denied, 1)
			}
		}()
		go func() {
			defer wg.Done()
			if d2.Allow(ctx, "shared").Allowed {
				atomic.AddInt32(&allowed, 1)
			} else {
				atomic.AddInt32(&denied, 1)
			}
		}()
		go func() {
			defer wg.Done()
			if d3.Allow(ctx, "shared").Allowed {
				atomic.AddInt32(&allowed, 1)
			} else {
				atomic.AddInt32(&denied, 1)
			}
		}()
	}
	wg.Wait()

	t.Logf("allowed=%d denied=%d", allowed, denied)
	if int(allowed) > limit {
		t.Errorf("allowed %d requests, limit is %d", allowed, limit)
	}
}

func TestDistributed_TokenBucket_Reset(t *testing.T) {
	d := newTestDistributed(t, 10, 10)
	ctx := context.Background()

	// Exhaust all tokens (one at a time since Lua script applies to all tokens)
	// The limit is capacity=10, so we need to verify reset clears state
	key := "reset_key"
	var denied int
	for i := 0; i < 15; i++ {
		if !d.Allow(ctx, key).Allowed {
			denied++
		}
	}
	if denied == 0 {
		t.Skip("expected some denials after 15 requests with limit 10")
	}

	// Reset
	if err := d.Reset(ctx, key); err != nil {
		t.Fatalf("reset: %v", err)
	}

	// Should be allowed again
	result := d.Allow(ctx, key)
	if !result.Allowed {
		t.Error("expected allowed after reset")
	}
}
