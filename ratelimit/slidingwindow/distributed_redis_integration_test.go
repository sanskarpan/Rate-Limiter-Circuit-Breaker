//go:build integration

// Integration tests validating the ACTUAL Redis Lua scripts for the distributed
// sliding-window limiters (H-1, H-2, H-3) — not just the in-memory emulation.
// Run with: go test ./ratelimit/slidingwindow/ -tags=integration
// Requires Redis at localhost:6379 or REDIS_ADDR.
package slidingwindow_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskarpan/resilience/ratelimit/slidingwindow"
	"github.com/sanskarpan/resilience/ratelimit/store"
)

func redisAddr() string {
	if a := os.Getenv("REDIS_ADDR"); a != "" {
		return a
	}
	return "localhost:6379"
}

func newRedis(t *testing.T) *store.Redis {
	t.Helper()
	s := store.NewRedis(store.RedisOptions{Addr: redisAddr(), KeyPrefix: fmt.Sprintf("it:%d:", time.Now().UnixNano())})
	if err := s.Ping(context.Background()); err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestRedis_DistributedSlidingLog_H1_AllowNConsumesN(t *testing.T) {
	d := slidingwindow.NewDistributedLog(5, time.Minute, newRedis(t), "swl")
	ctx := context.Background()
	if !d.AllowN(ctx, "k", 5).Allowed {
		t.Fatal("AllowN(5) within limit 5 should be allowed")
	}
	if d.Allow(ctx, "k").Allowed {
		t.Fatal("window should be full after AllowN(5) — real Lua must consume n, not 1")
	}
}

func TestRedis_DistributedSlidingLog_H1_OverLimitBatchDenied(t *testing.T) {
	d := slidingwindow.NewDistributedLog(5, time.Minute, newRedis(t), "swl2")
	if d.AllowN(context.Background(), "k", 6).Allowed {
		t.Fatal("AllowN(6) with limit 5 must be denied by the real Lua (count+n>limit)")
	}
}

func TestRedis_DistributedSlidingLog_H2_NoMemberCollision(t *testing.T) {
	d := slidingwindow.NewDistributedLog(100, time.Minute, newRedis(t), "swl3")
	ctx := context.Background()
	allowed := 0
	for i := 0; i < 100; i++ {
		if d.Allow(ctx, "k").Allowed {
			allowed++
		}
	}
	if allowed != 100 {
		t.Fatalf("expected 100 allowed, got %d (ZSET member collision in real Redis?)", allowed)
	}
	if d.Allow(ctx, "k").Allowed {
		t.Fatal("101st must be denied")
	}
}

func TestRedis_DistributedCounter_H3_AtomicUnderConcurrency(t *testing.T) {
	const limit = 50
	d := slidingwindow.NewDistributedCounter(limit, time.Minute, newRedis(t), "swc")
	ctx := context.Background()
	var allowed int64
	var wg sync.WaitGroup
	for i := 0; i < 300; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if d.Allow(ctx, "k").Allowed {
				atomic.AddInt64(&allowed, 1)
			}
		}()
	}
	wg.Wait()
	if allowed > limit {
		t.Fatalf("real Redis counter over-admitted under concurrency: %d > %d (non-atomic script)", allowed, limit)
	}
}
