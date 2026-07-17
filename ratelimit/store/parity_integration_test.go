//go:build integration

// Parity tests: drive identical script args through BOTH the real Redis Lua and
// the in-memory emulation, asserting they return the same allow/deny sequence.
// This is the regression for F-1 (GCRA) and F-2 (sliding-window log): the
// emulation must match Redis's float64-precision behaviour, not diverge from it.
// Run with: go test ./ratelimit/store/ -tags=integration
package store_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/store"
)

func parityStores(t *testing.T) (*store.Redis, *store.Memory) {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	r := store.NewRedis(store.RedisOptions{Addr: addr, KeyPrefix: fmt.Sprintf("par:%d:", time.Now().UnixNano())})
	if err := r.Ping(context.Background()); err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	m := store.NewMemoryWithScripts()
	t.Cleanup(func() { r.Close(); m.Close() })
	return r, m
}

// allowedOf normalizes a script result to just its allow/deny flag (or a
// sentinel), so an error, wrong shape, or differing decision all surface as a
// mismatch between the two backends.
func allowedOf(res any, err error) int64 {
	if err != nil {
		return -1
	}
	arr, ok := res.([]any)
	if !ok || len(arr) == 0 {
		return -2
	}
	a, _ := arr[0].(int64)
	return a
}

// TestParity_GCRA_RedisVsEmulation drives the GCRA script through both stores at
// a sub-256ns-sensitive boundary and asserts identical allow/deny decisions.
func TestParity_GCRA_RedisVsEmulation(t *testing.T) {
	r, m := parityStores(t)
	ctx := context.Background()
	emission := int64(time.Second / 7) // ~142857142ns, not a multiple of 256
	burst := int64(4)
	base := time.Now().UnixNano()
	for i := 0; i < 40; i++ {
		now := base + int64(i)*(emission/3)
		args := []any{emission, burst, int64(1), now, int64(10000)}
		rres, rerr := r.Eval(ctx, store.GCRAScript, []string{"k"}, args...)
		mres, merr := m.Eval(ctx, store.GCRAScript, []string{"k"}, args...)
		if ra, ma := allowedOf(rres, rerr), allowedOf(mres, merr); ra != ma {
			t.Fatalf("GCRA parity mismatch at i=%d now=%d: redis allowed=%d, emulation allowed=%d", i, now, ra, ma)
		}
	}
}

// TestParity_SlidingLog_RedisVsEmulation drives the sliding-window-log script
// through both stores across a window boundary and asserts identical decisions.
func TestParity_SlidingLog_RedisVsEmulation(t *testing.T) {
	r, m := parityStores(t)
	ctx := context.Background()
	limit := int64(5)
	window := int64(time.Second)
	base := time.Now().UnixNano()
	for i := 0; i < 40; i++ {
		now := base + int64(i)*(window/9)
		entryID := fmt.Sprintf("e-%d", i)
		args := []any{limit, window, now, entryID, int64(2000), int64(1)}
		rres, rerr := r.Eval(ctx, store.SlidingWindowLogScript, []string{"k"}, args...)
		mres, merr := m.Eval(ctx, store.SlidingWindowLogScript, []string{"k"}, args...)
		if ra, ma := allowedOf(rres, rerr), allowedOf(mres, merr); ra != ma {
			t.Fatalf("SWL parity mismatch at i=%d now=%d: redis allowed=%d, emulation allowed=%d", i, now, ra, ma)
		}
	}
}
