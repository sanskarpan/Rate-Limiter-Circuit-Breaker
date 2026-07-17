package circuitbreaker_test

import (
	"context"
	"testing"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
)

func BenchmarkCB_Execute_Closed(b *testing.B) {
	cfg := circuitbreaker.Config{
		Name:             "bench",
		WindowType:       circuitbreaker.CountBased,
		WindowSize:       10,
		FailureThreshold: 10,
		Clock:            clock.RealClock{},
	}
	cb := circuitbreaker.New(cfg)
	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cb.Execute(ctx, succeed) //nolint:errcheck
	}
}

func BenchmarkCB_Execute_Open(b *testing.B) {
	cfg := circuitbreaker.Config{
		Name:             "bench",
		WindowType:       circuitbreaker.CountBased,
		WindowSize:       5,
		FailureThreshold: 1,
		Clock:            clock.RealClock{},
	}
	cb := circuitbreaker.New(cfg)
	ctx := context.Background()
	// Open the circuit
	cb.Execute(ctx, fail) //nolint:errcheck
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cb.Execute(ctx, succeed) //nolint:errcheck
	}
}

func BenchmarkCB_Execute_Closed_Parallel(b *testing.B) {
	cfg := circuitbreaker.Config{
		Name:             "bench",
		WindowType:       circuitbreaker.CountBased,
		WindowSize:       100000,
		FailureThreshold: 100000,
		Clock:            clock.RealClock{},
	}
	cb := circuitbreaker.New(cfg)
	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cb.Execute(ctx, succeed) //nolint:errcheck
		}
	})
}
