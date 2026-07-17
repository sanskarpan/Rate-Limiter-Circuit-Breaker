//go:build integration

// Integration test validating the ACTUAL Redis Lua GCRA script (not just the
// in-memory emulation). Run with: go test ./ratelimit/gcra/ -tags=integration
// Requires Redis at localhost:6379 or REDIS_ADDR.
package gcra_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/gcra"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/store"
)

func newRedis(t *testing.T) *store.Redis {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	s := store.NewRedis(store.RedisOptions{Addr: addr, KeyPrefix: fmt.Sprintf("it:%d:", time.Now().UnixNano())})
	if err := s.Ping(context.Background()); err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestRedis_DistributedGCRA_BurstThenDeny(t *testing.T) {
	// rate 10/s, burst 5: a fresh key admits a burst then denies.
	d := gcra.NewDistributed(10, 5, newRedis(t), "gcra")
	ctx := context.Background()
	allowed := 0
	for i := 0; i < 20; i++ {
		if d.Allow(ctx, "k").Allowed {
			allowed++
		}
	}
	if allowed < 1 || allowed > 6 {
		t.Fatalf("expected a bounded burst (~burst 5) then denials, got %d allowed", allowed)
	}
	// The very next immediate request must be denied (burst exhausted).
	if d.Allow(ctx, "k").Allowed {
		t.Fatal("expected deny after burst exhausted")
	}
}

func TestRedis_DistributedGCRA_H7_ValidatesInput(t *testing.T) {
	d := gcra.NewDistributed(10, 5, newRedis(t), "gcra2")
	ctx := context.Background()
	if d.AllowN(ctx, "k", 0).Allowed {
		t.Fatal("AllowN(n=0) must be denied")
	}
	if d.AllowN(ctx, "", 1).Allowed {
		t.Fatal("empty key must be denied")
	}
	if d.AllowN(ctx, "k", 6).Allowed {
		t.Fatal("AllowN(n > burst) must be denied")
	}
}
