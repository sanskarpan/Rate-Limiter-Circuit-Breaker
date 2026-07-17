//go:build integration

// Integration test validating the ACTUAL Redis behavior of the distributed
// fixed-window limiter (H-4: a rejected over-limit AllowN must not poison the
// window). Run with: go test ./ratelimit/fixedwindow/ -tags=integration
package fixedwindow_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/fixedwindow"
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

func TestRedis_DistributedFixedWindow_H4_RejectedBatchDoesNotPoison(t *testing.T) {
	d := fixedwindow.NewDistributed(5, time.Minute, newRedis(t), "fw")
	ctx := context.Background()
	if d.AllowN(ctx, "k", 10).Allowed {
		t.Fatal("AllowN(10) over limit 5 must be denied")
	}
	for i := 0; i < 5; i++ {
		if !d.Allow(ctx, "k").Allowed {
			t.Fatalf("request %d must succeed after a rejected over-limit batch (real Redis window poisoned)", i)
		}
	}
	if d.Allow(ctx, "k").Allowed {
		t.Fatal("6th request should be denied")
	}
}

func TestRedis_DistributedFixedWindow_H7_ValidatesInput(t *testing.T) {
	d := fixedwindow.NewDistributed(5, time.Minute, newRedis(t), "fw2")
	ctx := context.Background()
	if d.AllowN(ctx, "k", 0).Allowed {
		t.Fatal("AllowN(n=0) must be denied")
	}
	if d.AllowN(ctx, "", 1).Allowed {
		t.Fatal("empty key must be denied")
	}
}
