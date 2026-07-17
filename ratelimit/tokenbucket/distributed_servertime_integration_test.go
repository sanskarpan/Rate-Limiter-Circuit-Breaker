//go:build integration

// Integration test for server-time mode at the distributed-limiter level.
// Run with: go test ./ratelimit/tokenbucket/ -tags=integration -run ServerTime
package tokenbucket_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/store"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
)

// TestDistributedTokenBucket_ServerTime_Redis_Enforces verifies the full path:
// a server-time store yields a server-time limiter that enforces the capacity
// against real Redis. (Skew-immunity of the script is covered by the store-level
// integration test, which can inject a skewed `now`.)
func TestDistributedTokenBucket_ServerTime_Redis_Enforces(t *testing.T) {
	ctx := context.Background()
	s := store.NewRedis(store.RedisOptions{
		Addr:          redisAddr(),
		KeyPrefix:     fmt.Sprintf("test:st:dtb:%d:", time.Now().UnixNano()),
		UseServerTime: true,
	})
	if err := s.Ping(ctx); err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	if !s.ServerTimeMode() {
		t.Fatal("store should report ServerTimeMode()==true")
	}

	// rate 1/s, capacity 5: expect exactly 5 admits before the refill matters.
	d := tokenbucket.NewDistributed(1, 5, s, "test")
	allowed := 0
	for i := 0; i < 8; i++ {
		if d.Allow(ctx, "k").Allowed {
			allowed++
		}
	}
	if allowed < 5 || allowed > 6 {
		t.Fatalf("server-time distributed bucket: expected ~5 admits, got %d", allowed)
	}
}
