package ratelimitctx_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/ratelimitctx"
)

func TestCostRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		set      int
		wantCost int // effective cost after clamping
	}{
		{name: "typical", set: 5, wantCost: 5},
		{name: "one", set: 1, wantCost: 1},
		{name: "zero_clamped_to_one", set: 0, wantCost: 1},
		{name: "negative_clamped_to_one", set: -3, wantCost: 1},
		{name: "large", set: 1000, wantCost: 1000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := ratelimitctx.WithCost(context.Background(), tt.set)

			got, ok := ratelimitctx.CostFromContext(ctx)
			if !ok {
				t.Fatal("CostFromContext: ok=false after WithCost")
			}
			if got != tt.wantCost {
				t.Fatalf("CostFromContext = %d, want %d", got, tt.wantCost)
			}
			if d := ratelimitctx.CostOrDefault(ctx); d != tt.wantCost {
				t.Fatalf("CostOrDefault = %d, want %d", d, tt.wantCost)
			}
		})
	}
}

func TestCostMissing(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	if c, ok := ratelimitctx.CostFromContext(ctx); ok || c != 0 {
		t.Fatalf("CostFromContext on empty ctx = (%d, %v), want (0, false)", c, ok)
	}
	if d := ratelimitctx.CostOrDefault(ctx); d != ratelimitctx.DefaultCost {
		t.Fatalf("CostOrDefault on empty ctx = %d, want %d", d, ratelimitctx.DefaultCost)
	}
}

func TestPriorityRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		set  int
	}{
		{name: "positive", set: 7},
		{name: "zero", set: 0},
		{name: "negative", set: -2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := ratelimitctx.WithPriority(context.Background(), tt.set)

			got, ok := ratelimitctx.PriorityFromContext(ctx)
			if !ok {
				t.Fatal("PriorityFromContext: ok=false after WithPriority")
			}
			if got != tt.set {
				t.Fatalf("PriorityFromContext = %d, want %d", got, tt.set)
			}
			if p := ratelimitctx.PriorityOrDefault(ctx, 99); p != tt.set {
				t.Fatalf("PriorityOrDefault = %d, want %d", p, tt.set)
			}
		})
	}
}

func TestPriorityMissing(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	if p, ok := ratelimitctx.PriorityFromContext(ctx); ok || p != 0 {
		t.Fatalf("PriorityFromContext on empty ctx = (%d, %v), want (0, false)", p, ok)
	}
	if p := ratelimitctx.PriorityOrDefault(ctx, 42); p != 42 {
		t.Fatalf("PriorityOrDefault on empty ctx = %d, want 42", p)
	}
}

func TestCostAndPriorityIndependent(t *testing.T) {
	t.Parallel()

	ctx := ratelimitctx.WithPriority(ratelimitctx.WithCost(context.Background(), 4), 9)

	if c, ok := ratelimitctx.CostFromContext(ctx); !ok || c != 4 {
		t.Fatalf("cost = (%d, %v), want (4, true)", c, ok)
	}
	if p, ok := ratelimitctx.PriorityFromContext(ctx); !ok || p != 9 {
		t.Fatalf("priority = (%d, %v), want (9, true)", p, ok)
	}
}

func TestCostFuncFromContext(t *testing.T) {
	t.Parallel()

	fn := ratelimitctx.CostFuncFromContext()

	tests := []struct {
		name string
		ctx  context.Context
		want int
	}{
		{name: "default_when_absent", ctx: context.Background(), want: ratelimitctx.DefaultCost},
		{name: "reads_context_value", ctx: ratelimitctx.WithCost(context.Background(), 8), want: 8},
		{name: "clamped_value", ctx: ratelimitctx.WithCost(context.Background(), 0), want: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest("GET", "/", nil).WithContext(tt.ctx)
			if got := fn(req); got != tt.want {
				t.Fatalf("CostFuncFromContext = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestGRPCCostFuncFromContext(t *testing.T) {
	t.Parallel()

	fn := ratelimitctx.GRPCCostFuncFromContext()

	if got := fn(context.Background(), "/svc/Method"); got != ratelimitctx.DefaultCost {
		t.Fatalf("default = %d, want %d", got, ratelimitctx.DefaultCost)
	}
	ctx := ratelimitctx.WithCost(context.Background(), 6)
	if got := fn(ctx, "/svc/Method"); got != 6 {
		t.Fatalf("from context = %d, want 6", got)
	}
	if got := fn(ratelimitctx.WithCost(context.Background(), -1), "/svc/Method"); got != 1 {
		t.Fatalf("clamped = %d, want 1", got)
	}
}
