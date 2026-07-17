package adaptive_test

import (
	"context"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/adaptive"
)

// TestAdaptive_OnDecisionHook verifies the hook fires with the adaptive
// algorithm's Result on both allow and deny.
func TestAdaptive_OnDecisionHook(t *testing.T) {
	signals := adaptive.NewStaticSignals(20, 0.05, 10*time.Millisecond)

	var keys []string
	var results []ratelimit.Result
	// initialLimit 1 so the second immediate request is denied.
	al := adaptive.New(1, 1, 10, signals,
		adaptive.WithClock(clock.RealClock{}),
		adaptive.WithOnDecision(func(key string, r ratelimit.Result) {
			keys = append(keys, key)
			results = append(results, r)
		}),
	)
	defer al.Close()
	ctx := context.Background()

	_ = al.Allow(ctx, "u")
	_ = al.Allow(ctx, "u")

	if len(results) != 2 {
		t.Fatalf("expected 2 hook calls, got %d", len(results))
	}
	if keys[0] != "u" || keys[1] != "u" {
		t.Fatalf("unexpected keys: %v", keys)
	}
	if results[0].Algorithm != "adaptive" {
		t.Fatalf("expected adaptive algorithm in hook Result, got %q", results[0].Algorithm)
	}
	if !results[0].Allowed || results[1].Allowed {
		t.Fatalf("expected first allow, second deny; got %v %v", results[0].Allowed, results[1].Allowed)
	}
}
