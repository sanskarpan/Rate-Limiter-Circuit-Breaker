package fixedwindow_test

import (
	"context"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/fixedwindow"
)

// TestFixedWindow_OnDecisionHook_AllowAndDeny verifies WithOnDecision fires with
// the correct key and Result on both an allow and a deny.
func TestFixedWindow_OnDecisionHook_AllowAndDeny(t *testing.T) {
	clk := clock.NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))

	type call struct {
		key string
		res ratelimit.Result
	}
	var calls []call

	// limit 1 per window so the second request in the same window is denied.
	fw := fixedwindow.New(1, time.Second,
		fixedwindow.WithClock(clk),
		fixedwindow.WithOnDecision(func(key string, r ratelimit.Result) {
			calls = append(calls, call{key: key, res: r})
		}),
	)
	defer fw.Close()
	ctx := context.Background()

	if !fw.Allow(ctx, "u").Allowed {
		t.Fatalf("first request should be allowed")
	}
	if fw.Allow(ctx, "u").Allowed {
		t.Fatalf("second request should be denied")
	}

	if len(calls) != 2 {
		t.Fatalf("expected hook to fire twice, got %d", len(calls))
	}
	if calls[0].key != "u" || !calls[0].res.Allowed {
		t.Fatalf("first hook call wrong: %+v", calls[0])
	}
	if calls[1].key != "u" || calls[1].res.Allowed {
		t.Fatalf("second hook call should be a deny: %+v", calls[1])
	}
}

// TestFixedWindow_NilHook_Safe verifies a nil hook (default) is safe.
func TestFixedWindow_NilHook_Safe(t *testing.T) {
	clk := clock.NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	fw := fixedwindow.New(2, time.Second, fixedwindow.WithClock(clk))
	defer fw.Close()

	if !fw.Allow(context.Background(), "k").Allowed {
		t.Fatalf("expected allow with nil hook")
	}
}

// TestFixedWindow_AllowN_CostMetadata verifies AllowN(n>1) records the cost and
// drains the window n times faster.
func TestFixedWindow_AllowN_CostMetadata(t *testing.T) {
	clk := clock.NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	fw := fixedwindow.New(10, time.Second, fixedwindow.WithClock(clk))
	defer fw.Close()
	ctx := context.Background()

	res := fw.AllowN(ctx, "k", 5)
	if !res.Allowed {
		t.Fatalf("AllowN(5) should be allowed with limit 10")
	}
	if res.Metadata == nil || res.Metadata["cost"] != 5 {
		t.Fatalf("expected Metadata[cost]==5, got %v", res.Metadata)
	}
	if !fw.AllowN(ctx, "k", 5).Allowed {
		t.Fatalf("second AllowN(5) should be allowed (10 total)")
	}
	if fw.AllowN(ctx, "k", 5).Allowed {
		t.Fatalf("third AllowN(5) should be denied (window exhausted)")
	}
}
