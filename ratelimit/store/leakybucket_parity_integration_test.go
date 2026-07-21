//go:build integration

// Parity test for the distributed leaky bucket (ENHANCEMENTS §1.8): drive
// identical LeakyBucketScript args through BOTH real Redis Lua and the in-memory
// emulation, asserting they return the same allow/deny sequence. This is the
// regression that the emulation matches Redis's float64-precision behaviour (the
// same discipline as the GCRA / sliding-window-log parity tests).
// Run with: go test ./ratelimit/store/ -tags=integration
package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/store"
)

// TestParity_LeakyBucket_RedisVsEmulation drives the leaky bucket script through
// both stores across a range of times spanning the queue-full boundary and
// asserts identical allow/deny decisions.
func TestParity_LeakyBucket_RedisVsEmulation(t *testing.T) {
	r, m := parityStores(t)
	ctx := context.Background()
	emission := int64(time.Second / 7) // ~142857142ns, not a multiple of 256
	capacity := int64(4)
	base := time.Now().UnixNano()
	for i := 0; i < 40; i++ {
		now := base + int64(i)*(emission/3)
		args := []any{emission, capacity, int64(1), now, int64(10000)}
		rres, rerr := r.Eval(ctx, store.LeakyBucketScriptID, []string{"lbk"}, args...)
		mres, merr := m.Eval(ctx, store.LeakyBucketScriptID, []string{"lbk"}, args...)
		if ra, ma := allowedOf(rres, rerr), allowedOf(mres, merr); ra != ma {
			t.Fatalf("leaky-bucket parity mismatch at i=%d now=%d: redis allowed=%d, emulation allowed=%d", i, now, ra, ma)
		}
	}
}
