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

// timeBasedBenchCfg builds a TimeBased config with a high failure-rate threshold
// so the circuit stays CLOSED for the entire benchmark (exercising the common
// hot path: record + slide-check on every call).
func timeBasedBenchCfg() circuitbreaker.Config {
	return circuitbreaker.Config{
		Name:                 "bench",
		WindowType:           circuitbreaker.TimeBased,
		WindowDuration:       60 * 1000 * 1000 * 1000, // 60s in ns
		BucketDuration:       1000 * 1000 * 1000,      // 1s in ns
		FailureThreshold:     1 << 30,                 // effectively never trips
		FailureRateThreshold: 1.0,
		MinimumRequests:      1 << 30,
		Clock:                clock.RealClock{},
	}
}

func BenchmarkCB_Execute_Closed_TimeBased(b *testing.B) {
	cb := circuitbreaker.New(timeBasedBenchCfg())
	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cb.Execute(ctx, succeed) //nolint:errcheck
	}
}

func BenchmarkCB_Execute_Closed_Parallel_TimeBased(b *testing.B) {
	cb := circuitbreaker.New(timeBasedBenchCfg())
	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cb.Execute(ctx, succeed) //nolint:errcheck
		}
	})
}

// BenchmarkCB_Execute_Closed_Failures exercises the CountBased CLOSED failure
// path (record + threshold check), which previously took the window mutex twice
// per call and now takes it once (§3.4). Threshold is huge so it stays closed.
func BenchmarkCB_Execute_Closed_Failures(b *testing.B) {
	cfg := circuitbreaker.Config{
		Name:             "bench",
		WindowType:       circuitbreaker.CountBased,
		WindowSize:       100000,
		FailureThreshold: 1 << 30,
		Clock:            clock.RealClock{},
	}
	cb := circuitbreaker.New(cfg)
	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cb.Execute(ctx, fail) //nolint:errcheck
	}
}
