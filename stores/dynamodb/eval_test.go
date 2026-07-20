package dynamodb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/store"
)

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

func TestEvalTokenBucket(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(newFakeDDB("pk"))

	capacity := 3.0
	refillRate := 1e-18
	now := int64(1_700_000_000_000_000_000)
	ttlMs := int64(60_000)

	allowed := 0
	for i := 0; i < 5; i++ {
		res, err := s.Eval(ctx, store.TokenBucketScript, []string{"tb"},
			capacity, refillRate, int64(1), now, ttlMs, "0")
		if err != nil {
			t.Fatalf("Eval: %v", err)
		}
		if asIntSlice(t, res)[0] == 1 {
			allowed++
		}
	}
	if allowed != 3 {
		t.Fatalf("token bucket admitted %d, want 3", allowed)
	}
}

func TestEvalTokenBucketRefill(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(newFakeDDB("pk"))

	capacity := 2.0
	refillRate := 1e-9
	now := int64(1_700_000_000_000_000_000)
	ttlMs := int64(60_000)

	for i := 0; i < 2; i++ {
		res, _ := s.Eval(ctx, store.TokenBucketScript, []string{"tb"}, capacity, refillRate, int64(1), now, ttlMs, "0")
		if asIntSlice(t, res)[0] != 1 {
			t.Fatalf("expected admit %d", i)
		}
	}
	res, _ := s.Eval(ctx, store.TokenBucketScript, []string{"tb"}, capacity, refillRate, int64(1), now, ttlMs, "0")
	if asIntSlice(t, res)[0] != 0 {
		t.Fatal("expected deny on empty bucket")
	}
	later := now + int64(time.Second)
	res, _ = s.Eval(ctx, store.TokenBucketScript, []string{"tb"}, capacity, refillRate, int64(1), later, ttlMs, "0")
	if asIntSlice(t, res)[0] != 1 {
		t.Fatal("expected admit after refill")
	}
}

func TestEvalTokenBucketRefillRateZeroFailsClosed(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(newFakeDDB("pk"))
	if _, err := s.Eval(ctx, store.TokenBucketScript, []string{"tb"}, 3.0, 0.0, int64(1), int64(1), int64(1000), "0"); err == nil {
		t.Fatal("expected error for refillRate <= 0")
	}
}

func TestEvalFixedWindow(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(newFakeDDB("pk"))

	limit := int64(3)
	for i := 1; i <= 3; i++ {
		res, err := s.Eval(ctx, store.FixedWindowScript, []string{"fw"}, limit, int64(1), int64(1000))
		if err != nil {
			t.Fatalf("Eval: %v", err)
		}
		out := asIntSlice(t, res)
		if out[0] != 1 || out[1] != int64(i) {
			t.Fatalf("iter %d: got %v", i, out)
		}
	}
	res, _ := s.Eval(ctx, store.FixedWindowScript, []string{"fw"}, limit, int64(1), int64(1000))
	out := asIntSlice(t, res)
	if out[0] != 0 || out[1] != 3 {
		t.Fatalf("over limit: got %v", out)
	}
}

func TestEvalGCRA(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(newFakeDDB("pk"))

	emission := int64(time.Second)
	burst := int64(2)
	now := int64(1_700_000_000_000_000_000)

	allowed := 0
	for i := 0; i < 5; i++ {
		res, err := s.Eval(ctx, store.GCRAScript, []string{"g"}, emission, burst, int64(1), now, int64(60_000), "0")
		if err != nil {
			t.Fatalf("Eval: %v", err)
		}
		if asIntSlice(t, res)[0] == 1 {
			allowed++
		}
	}
	if allowed != 2 {
		t.Fatalf("GCRA admitted %d, want 2", allowed)
	}
}

func TestEvalLeakyBucket(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(newFakeDDB("pk"))

	emission := int64(time.Second)
	capacity := int64(3)
	now := int64(1_700_000_000_000_000_000)

	allowed := 0
	for i := 0; i < 6; i++ {
		res, err := s.Eval(ctx, store.LeakyBucketScript, []string{"lb"}, emission, capacity, int64(1), now, int64(60_000), "0")
		if err != nil {
			t.Fatalf("Eval: %v", err)
		}
		if asIntSlice(t, res)[0] == 1 {
			allowed++
		}
	}
	if allowed != 3 {
		t.Fatalf("leaky bucket admitted %d, want 3", allowed)
	}
}

func TestEvalSlidingWindowLog(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(newFakeDDB("pk"))

	limit := int64(3)
	windowNs := int64(time.Second)
	now := int64(1_700_000_000_000_000_000)

	allowed := 0
	for i := 0; i < 5; i++ {
		entryID := "entry-" + string(rune('a'+i))
		res, err := s.Eval(ctx, store.SlidingWindowLogScript, []string{"swl"},
			limit, windowNs, now, entryID, int64(60_000), int64(1), "0")
		if err != nil {
			t.Fatalf("Eval: %v", err)
		}
		if asIntSlice(t, res)[0] == 1 {
			allowed++
		}
	}
	if allowed != 3 {
		t.Fatalf("sliding window log admitted %d, want 3", allowed)
	}
	later := now + windowNs + 1
	res, _ := s.Eval(ctx, store.SlidingWindowLogScript, []string{"swl"},
		limit, windowNs, later, "entry2", int64(60_000), int64(1), "0")
	if asIntSlice(t, res)[0] != 1 {
		t.Fatal("expected admit after window slid")
	}
}

func TestEvalUnsupportedScripts(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(newFakeDDB("pk"))

	for _, sc := range []string{
		store.SlidingWindowCounterScript,
		store.CircuitBreakerAcquireScript,
		store.CircuitBreakerRecordScript,
		store.CircuitBreakerReadScript,
	} {
		if _, err := s.Eval(ctx, sc, []string{"a", "b"}, int64(1)); !errors.Is(err, ErrScriptUnsupported) {
			t.Fatalf("expected ErrScriptUnsupported, got %v", err)
		}
	}
	if _, err := s.Eval(ctx, "unknown", []string{"a"}); !errors.Is(err, ErrScriptUnsupported) {
		t.Fatalf("unknown: expected ErrScriptUnsupported, got %v", err)
	}
}
