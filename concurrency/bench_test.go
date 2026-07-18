package concurrency_test

import (
	"context"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/concurrency"
)

// benchConfig sizes the limit high enough that Acquire always succeeds, so the
// benchmark measures the admission gate + strategy Update overhead rather than
// shedding.
var benchConfig = concurrency.Config{
	InitialLimit: 1_000_000,
	MaxLimit:     1_000_000,
	MinLimit:     1,
	RTTTolerance: 1.5,
}

// benchOutcome is a fixed, healthy outcome so strategy Update runs its normal
// (non-drop) path each release.
var benchOutcome = concurrency.Outcome{RTT: time.Millisecond, Dropped: false}

// BenchmarkGradient2_AcquireRelease measures the full acquire → release cycle
// for the recommended Gradient2 strategy (admission CAS + strategy Update).
func BenchmarkGradient2_AcquireRelease(b *testing.B) {
	lim := concurrency.NewGradient2(benchConfig)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rel, ok := lim.Acquire(ctx)
		if ok {
			rel(benchOutcome)
		}
	}
}

// BenchmarkAIMD_AcquireRelease measures the acquire → release cycle for AIMD.
func BenchmarkAIMD_AcquireRelease(b *testing.B) {
	lim := concurrency.NewAIMD(benchConfig)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rel, ok := lim.Acquire(ctx)
		if ok {
			rel(benchOutcome)
		}
	}
}

// BenchmarkVegas_AcquireRelease measures the acquire → release cycle for Vegas.
func BenchmarkVegas_AcquireRelease(b *testing.B) {
	lim := concurrency.NewVegas(benchConfig)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rel, ok := lim.Acquire(ctx)
		if ok {
			rel(benchOutcome)
		}
	}
}

// BenchmarkGradient2_AcquireRelease_Parallel measures the acquire → release
// cycle under concurrency (CAS contention on inflight plus strategy lock).
func BenchmarkGradient2_AcquireRelease_Parallel(b *testing.B) {
	lim := concurrency.NewGradient2(benchConfig)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rel, ok := lim.Acquire(ctx)
			if ok {
				rel(benchOutcome)
			}
		}
	})
}
