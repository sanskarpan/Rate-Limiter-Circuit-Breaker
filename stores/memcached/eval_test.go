package memcached

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/store"
)

// asIntSlice coerces an Eval result (which the store returns as []any of int64)
// to a []int64 for easy assertions.
func asIntSlice(t *testing.T, v any) []int64 {
	t.Helper()
	s, ok := v.([]any)
	if !ok {
		t.Fatalf("result not []any: %T", v)
	}
	out := make([]int64, len(s))
	for i, e := range s {
		n, ok := e.(int64)
		if !ok {
			t.Fatalf("element %d not int64: %T", i, e)
		}
		out[i] = n
	}
	return out
}

// TestEvalTokenBucket checks that the client-side token bucket admits up to
// capacity and then denies, matching the semantics of the Redis Lua script.
func TestEvalTokenBucket(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(newFakeMemcache())

	capacity := 3.0
	refillRate := 0.0 // no refill during the test window (tokens/ns)
	now := int64(1_700_000_000_000_000_000)
	ttlMs := int64(60_000)

	// refillRate must be > 0 per the fail-closed contract; use a tiny positive
	// rate that refills negligibly over the (identical) now values.
	refillRate = 1e-18

	allowed := 0
	for i := 0; i < 5; i++ {
		res, err := s.Eval(ctx, store.TokenBucketScriptID, []string{"tb"},
			capacity, refillRate, int64(1), now, ttlMs, "0")
		if err != nil {
			t.Fatalf("Eval token bucket: %v", err)
		}
		out := asIntSlice(t, res)
		if out[0] == 1 {
			allowed++
		}
	}
	if allowed != 3 {
		t.Fatalf("token bucket admitted %d, want 3 (capacity)", allowed)
	}
}

// TestEvalTokenBucketRefill verifies tokens refill over elapsed time.
func TestEvalTokenBucketRefill(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(newFakeMemcache())

	capacity := 2.0
	refillRate := 1e-9 // 1 token per second (per ns)
	now := int64(1_700_000_000_000_000_000)
	ttlMs := int64(60_000)

	// Drain the bucket.
	for i := 0; i < 2; i++ {
		res, _ := s.Eval(ctx, store.TokenBucketScriptID, []string{"tb"}, capacity, refillRate, int64(1), now, ttlMs, "0")
		if asIntSlice(t, res)[0] != 1 {
			t.Fatalf("expected admit %d", i)
		}
	}
	// Now empty: deny.
	res, _ := s.Eval(ctx, store.TokenBucketScriptID, []string{"tb"}, capacity, refillRate, int64(1), now, ttlMs, "0")
	if asIntSlice(t, res)[0] != 0 {
		t.Fatal("expected deny on empty bucket")
	}
	// Advance 1 second → 1 token refilled → admit.
	later := now + int64(time.Second)
	res, _ = s.Eval(ctx, store.TokenBucketScriptID, []string{"tb"}, capacity, refillRate, int64(1), later, ttlMs, "0")
	if asIntSlice(t, res)[0] != 1 {
		t.Fatal("expected admit after refill")
	}
}

func TestEvalTokenBucketRefillRateZeroFailsClosed(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(newFakeMemcache())
	_, err := s.Eval(ctx, store.TokenBucketScriptID, []string{"tb"}, 3.0, 0.0, int64(1), int64(1), int64(1000), "0")
	if err == nil {
		t.Fatal("expected error for refillRate <= 0 (fail closed)")
	}
}

// TestEvalFixedWindow verifies the fixed-window script admits up to limit and
// then denies without poisoning the counter.
func TestEvalFixedWindow(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(newFakeMemcache())

	limit := int64(3)
	ttlMs := int64(1000)

	for i := 1; i <= 3; i++ {
		res, err := s.Eval(ctx, store.FixedWindowScriptID, []string{"fw"}, limit, int64(1), ttlMs)
		if err != nil {
			t.Fatalf("Eval fixed window: %v", err)
		}
		out := asIntSlice(t, res)
		if out[0] != 1 || out[1] != int64(i) {
			t.Fatalf("iter %d: got %v, want allow count=%d", i, out, i)
		}
	}
	// Over limit: deny, count stays 3.
	res, _ := s.Eval(ctx, store.FixedWindowScriptID, []string{"fw"}, limit, int64(1), ttlMs)
	out := asIntSlice(t, res)
	if out[0] != 0 || out[1] != 3 {
		t.Fatalf("over limit: got %v, want deny count=3", out)
	}
	// AllowN larger than limit must not poison the window.
	res, _ = s.Eval(ctx, store.FixedWindowScriptID, []string{"fw"}, limit, int64(10), ttlMs)
	if asIntSlice(t, res)[0] != 0 {
		t.Fatal("AllowN over limit should deny")
	}
}

// TestEvalGCRA verifies the GCRA script admits a burst then denies.
func TestEvalGCRA(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(newFakeMemcache())

	emission := int64(time.Second) // 1 req/sec
	burst := int64(2)
	now := int64(1_700_000_000_000_000_000)
	ttlMs := int64(60_000)

	allowed := 0
	for i := 0; i < 5; i++ {
		res, err := s.Eval(ctx, store.GCRAScriptID, []string{"g"}, emission, burst, int64(1), now, ttlMs, "0")
		if err != nil {
			t.Fatalf("Eval GCRA: %v", err)
		}
		if asIntSlice(t, res)[0] == 1 {
			allowed++
		}
	}
	// GCRA at now with burst=2 admits burst then denies (all at the same instant).
	if allowed != 2 {
		t.Fatalf("GCRA admitted %d, want 2 (burst)", allowed)
	}
}

// TestEvalLeakyBucket verifies leaky bucket admits up to capacity.
func TestEvalLeakyBucket(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(newFakeMemcache())

	emission := int64(time.Second)
	capacity := int64(3)
	now := int64(1_700_000_000_000_000_000)
	ttlMs := int64(60_000)

	allowed := 0
	for i := 0; i < 6; i++ {
		res, err := s.Eval(ctx, store.LeakyBucketScriptID, []string{"lb"}, emission, capacity, int64(1), now, ttlMs, "0")
		if err != nil {
			t.Fatalf("Eval leaky bucket: %v", err)
		}
		if asIntSlice(t, res)[0] == 1 {
			allowed++
		}
	}
	if allowed != 3 {
		t.Fatalf("leaky bucket admitted %d, want 3 (capacity)", allowed)
	}
}

// TestEvalSlidingWindowLog verifies the log admits up to limit within a window.
func TestEvalSlidingWindowLog(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(newFakeMemcache())

	limit := int64(3)
	windowNs := int64(time.Second)
	now := int64(1_700_000_000_000_000_000)
	ttlMs := int64(60_000)

	allowed := 0
	for i := 0; i < 5; i++ {
		// Distinct entry IDs per call, exactly as the real distributed limiter
		// supplies (otherwise identical member names would collapse in the ZSET).
		entryID := "entry-" + string(rune('a'+i))
		res, err := s.Eval(ctx, store.SlidingWindowLogScriptID, []string{"swl"},
			limit, windowNs, now, entryID, ttlMs, int64(1), "0")
		if err != nil {
			t.Fatalf("Eval sliding window log: %v", err)
		}
		if asIntSlice(t, res)[0] == 1 {
			allowed++
		}
	}
	if allowed != 3 {
		t.Fatalf("sliding window log admitted %d, want 3 (limit)", allowed)
	}

	// Advance past the window: entries expire, admit again.
	later := now + windowNs + 1
	res, _ := s.Eval(ctx, store.SlidingWindowLogScriptID, []string{"swl"},
		limit, windowNs, later, "entry2", ttlMs, int64(1), "0")
	if asIntSlice(t, res)[0] != 1 {
		t.Fatal("expected admit after window slid past old entries")
	}
}

// TestEvalUnsupportedScripts verifies multi-key/CB scripts fail with a clear error.
func TestEvalUnsupportedScripts(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(newFakeMemcache())

	for _, sc := range []store.ScriptID{
		store.SlidingWindowCounterScriptID,
		store.CircuitBreakerAcquireScriptID,
		store.CircuitBreakerRecordScriptID,
		store.CircuitBreakerReadScriptID,
	} {
		_, err := s.Eval(ctx, sc, []string{"a", "b"}, int64(1))
		if !errors.Is(err, ErrScriptUnsupported) {
			t.Fatalf("expected ErrScriptUnsupported, got %v", err)
		}
	}

	_, err := s.Eval(ctx, store.NewScriptID("unknown body"), []string{"a"})
	if !errors.Is(err, ErrScriptUnsupported) {
		t.Fatalf("unknown script: expected ErrScriptUnsupported, got %v", err)
	}
}
