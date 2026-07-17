package slidingwindow_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/slidingwindow"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/store"
)

// Distributed sliding-window tests run against the in-memory script emulation
// (H-21/TQ-1) and verify H-1 (AllowN consumes n), H-2 (no ZSET member
// collision), and H-3 (counter is atomic under concurrency).

func TestDistributedSlidingLog_H1_AllowNConsumesN(t *testing.T) {
	s := store.NewMemoryWithScripts()
	defer s.Close()
	d := slidingwindow.NewDistributedLog(5, time.Minute, s, "swl")
	ctx := context.Background()

	if !d.AllowN(ctx, "k", 5).Allowed {
		t.Fatal("AllowN(5) within limit 5 should be allowed")
	}
	// If AllowN consumed 5 (not 1), the window is now full.
	if d.Allow(ctx, "k").Allowed {
		t.Fatal("window should be full after AllowN(5) — proves n tokens consumed, not 1")
	}
}

func TestDistributedSlidingLog_H1_DeniesOverLimitBatch(t *testing.T) {
	s := store.NewMemoryWithScripts()
	defer s.Close()
	d := slidingwindow.NewDistributedLog(5, time.Minute, s, "swl2")
	if d.AllowN(context.Background(), "k", 6).Allowed {
		t.Fatal("AllowN(6) with limit 5 must be denied (count+n > limit)")
	}
}

func TestDistributedSlidingLog_H2_NoMemberCollision(t *testing.T) {
	s := store.NewMemoryWithScripts()
	defer s.Close()
	d := slidingwindow.NewDistributedLog(100, time.Minute, s, "swl3")
	ctx := context.Background()

	allowed := 0
	for i := 0; i < 100; i++ {
		if d.Allow(ctx, "k").Allowed {
			allowed++
		}
	}
	// If same-instant requests produced colliding members, ZCARD would
	// under-count and either admit >100 or mis-count. Exactly 100 must pass.
	if allowed != 100 {
		t.Fatalf("expected exactly 100 allowed, got %d (member collision?)", allowed)
	}
	if d.Allow(ctx, "k").Allowed {
		t.Fatal("101st request must be denied")
	}
}

func TestDistributedCounter_H3_AtomicUnderConcurrency(t *testing.T) {
	s := store.NewMemoryWithScripts()
	defer s.Close()
	const limit = 50
	d := slidingwindow.NewDistributedCounter(limit, time.Minute, s, "swc")
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

	// Atomicity: concurrent callers must never over-admit past the limit within
	// a single window (previous window is empty for a fresh key).
	if allowed > limit {
		t.Fatalf("over-admitted under concurrency: %d > limit %d (non-atomic)", allowed, limit)
	}
	if allowed != limit {
		t.Fatalf("expected exactly %d admitted in one window, got %d", limit, allowed)
	}
}
