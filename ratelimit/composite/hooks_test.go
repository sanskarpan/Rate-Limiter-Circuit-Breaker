package composite_test

import (
	"context"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/composite"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/fixedwindow"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
)

// TestComposite_OnDecisionHook verifies WithOnDecision fires with the composite's
// key and final Result on both allow and deny.
func TestComposite_OnDecisionHook(t *testing.T) {
	clk := newClock()
	limA := tokenbucket.New(10, 10, tokenbucket.WithClock(clk))
	// limit 1 so the second request denies the AND composite.
	limB := fixedwindow.New(1, time.Second, fixedwindow.WithClock(clk))
	defer limA.Close()
	defer limB.Close()

	var keys []string
	var results []ratelimit.Result
	comp := composite.New(composite.AND, limA, limB).
		WithClock(clk).
		WithOnDecision(func(key string, r ratelimit.Result) {
			keys = append(keys, key)
			results = append(results, r)
		})
	defer comp.Close()

	ctx := context.Background()
	if !comp.Allow(ctx, "key").Allowed {
		t.Fatal("first composite request should be allowed")
	}
	if comp.Allow(ctx, "key").Allowed {
		t.Fatal("second composite request should be denied (limB exhausted)")
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 hook calls, got %d", len(results))
	}
	if keys[0] != "key" || keys[1] != "key" {
		t.Fatalf("unexpected keys: %v", keys)
	}
	if !results[0].Allowed || results[1].Allowed {
		t.Fatalf("expected first allow, second deny; got %v %v", results[0].Allowed, results[1].Allowed)
	}
}
