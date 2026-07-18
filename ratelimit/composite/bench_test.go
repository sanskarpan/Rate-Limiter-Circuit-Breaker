package composite_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/composite"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
)

// BenchmarkComposite_AND_Allow_SingleKey measures the AND two-phase
// (Peek-all-then-consume-all) hot path over two limiters, single key.
func BenchmarkComposite_AND_Allow_SingleKey(b *testing.B) {
	tb1 := tokenbucket.New(1e9, 1e9, tokenbucket.WithClock(clock.RealClock{}))
	tb2 := tokenbucket.New(1e9, 1e9, tokenbucket.WithClock(clock.RealClock{}))
	c := composite.New(composite.AND, tb1, tb2).WithClock(clock.RealClock{})
	defer c.Close()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		c.Allow(ctx, "bench-key")
	}
}

// BenchmarkComposite_AND_Allow_100Keys measures the AND path under key
// contention across 100 distinct keys.
func BenchmarkComposite_AND_Allow_100Keys(b *testing.B) {
	tb1 := tokenbucket.New(1e9, 1e9, tokenbucket.WithClock(clock.RealClock{}))
	tb2 := tokenbucket.New(1e9, 1e9, tokenbucket.WithClock(clock.RealClock{}))
	c := composite.New(composite.AND, tb1, tb2).WithClock(clock.RealClock{})
	defer c.Close()
	ctx := context.Background()
	keys := make([]string, 100)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		c.Allow(ctx, keys[i%100])
	}
}

// BenchmarkComposite_OR_Allow_SingleKey measures the OR short-circuit path
// (first limiter allows and wins) over two limiters, single key.
func BenchmarkComposite_OR_Allow_SingleKey(b *testing.B) {
	tb1 := tokenbucket.New(1e9, 1e9, tokenbucket.WithClock(clock.RealClock{}))
	tb2 := tokenbucket.New(1e9, 1e9, tokenbucket.WithClock(clock.RealClock{}))
	c := composite.New(composite.OR, tb1, tb2).WithClock(clock.RealClock{})
	defer c.Close()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		c.Allow(ctx, "bench-key")
	}
}
