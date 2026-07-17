package tokenbucket_test

import (
	"context"
	"testing"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
)

// TestTokenBucket_OnDecisionHook_AllowAndDeny verifies the WithOnDecision hook
// fires with the correct key and Result on both an allow and a deny.
func TestTokenBucket_OnDecisionHook_AllowAndDeny(t *testing.T) {
	clk := newTestClock()

	type call struct {
		key string
		res ratelimit.Result
	}
	var calls []call

	// capacity 1, refill 1/s so the second immediate request is denied.
	tb := tokenbucket.New(1, 1,
		tokenbucket.WithClock(clk),
		tokenbucket.WithOnDecision(func(key string, r ratelimit.Result) {
			calls = append(calls, call{key: key, res: r})
		}),
	)
	defer tb.Close()
	ctx := context.Background()

	first := tb.Allow(ctx, "user:1")
	if !first.Allowed {
		t.Fatalf("first request should be allowed")
	}
	second := tb.Allow(ctx, "user:1")
	if second.Allowed {
		t.Fatalf("second request should be denied")
	}

	if len(calls) != 2 {
		t.Fatalf("expected hook to fire twice, got %d", len(calls))
	}
	if calls[0].key != "user:1" || !calls[0].res.Allowed {
		t.Fatalf("first hook call wrong: %+v", calls[0])
	}
	if calls[1].key != "user:1" || calls[1].res.Allowed {
		t.Fatalf("second hook call should be a deny: %+v", calls[1])
	}
}

// TestTokenBucket_NilHook_Safe verifies a limiter with no hook (default nil)
// makes decisions without panicking.
func TestTokenBucket_NilHook_Safe(t *testing.T) {
	clk := newTestClock()
	tb := tokenbucket.New(2, 1, tokenbucket.WithClock(clk))
	defer tb.Close()
	ctx := context.Background()

	if !tb.Allow(ctx, "k").Allowed {
		t.Fatalf("expected allow with nil hook")
	}
}

// TestTokenBucket_AllowN_CostMetadata verifies AllowN with n>1 surfaces the
// consumed cost in Result.Metadata["cost"] and drains the bucket n times faster.
func TestTokenBucket_AllowN_CostMetadata(t *testing.T) {
	clk := newTestClock()
	tb := tokenbucket.New(10, 1, tokenbucket.WithClock(clk))
	defer tb.Close()
	ctx := context.Background()

	res := tb.AllowN(ctx, "k", 5)
	if !res.Allowed {
		t.Fatalf("AllowN(5) should be allowed with capacity 10")
	}
	if res.Metadata == nil || res.Metadata["cost"] != 5 {
		t.Fatalf("expected Metadata[cost]==5, got %v", res.Metadata)
	}
	if res.Remaining != 5 {
		t.Fatalf("expected 5 remaining after consuming 5 of 10, got %d", res.Remaining)
	}

	// Consuming 5 more exhausts the bucket; a further 5 is denied.
	if !tb.AllowN(ctx, "k", 5).Allowed {
		t.Fatalf("second AllowN(5) should be allowed (10 total)")
	}
	if tb.AllowN(ctx, "k", 5).Allowed {
		t.Fatalf("third AllowN(5) should be denied (bucket exhausted)")
	}
}

// TestTokenBucket_AllowN_SingleTokenNoCostMetadata verifies the n==1 hot path
// stays free of cost metadata (default behavior unchanged).
func TestTokenBucket_AllowN_SingleTokenNoCostMetadata(t *testing.T) {
	clk := newTestClock()
	tb := tokenbucket.New(5, 1, tokenbucket.WithClock(clk))
	defer tb.Close()

	res := tb.Allow(context.Background(), "k")
	if res.Metadata != nil {
		if _, ok := res.Metadata["cost"]; ok {
			t.Fatalf("n==1 path must not set cost metadata, got %v", res.Metadata)
		}
	}
}

// TestTokenBucket_AllowCost_Fractional verifies the float64 cost path consumes
// fractional tokens and records the cost.
func TestTokenBucket_AllowCost_Fractional(t *testing.T) {
	clk := newTestClock()
	tb := tokenbucket.New(10, 1, tokenbucket.WithClock(clk))
	defer tb.Close()
	ctx := context.Background()

	res := tb.AllowCost(ctx, "k", 2.5)
	if !res.Allowed {
		t.Fatalf("AllowCost(2.5) should be allowed with capacity 10")
	}
	if res.Metadata == nil || res.Metadata["cost"] != 2.5 {
		t.Fatalf("expected Metadata[cost]==2.5, got %v", res.Metadata)
	}

	// 7.5 remains; a cost of 8.0 must be denied all-or-nothing.
	if tb.AllowCost(ctx, "k", 8.0).Allowed {
		t.Fatalf("AllowCost(8.0) should be denied with 7.5 remaining")
	}
	// A cost of 7.0 fits.
	if !tb.AllowCost(ctx, "k", 7.0).Allowed {
		t.Fatalf("AllowCost(7.0) should be allowed with 7.5 remaining")
	}

	// Non-positive cost is rejected.
	if tb.AllowCost(ctx, "k", 0).Allowed {
		t.Fatalf("AllowCost(0) should be denied")
	}
}
