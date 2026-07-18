package adaptive_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/adaptive"
)

// healthySignals reports a comfortably-healthy system so the adjust loop is
// stable and the benchmark measures the Allow hot path, not oscillation.
func healthySignals() adaptive.SignalSource {
	return adaptive.NewStaticSignals(10, 0.0, 10*time.Millisecond)
}

// BenchmarkAdaptive_Allow_SingleKey measures the adaptive limiter's Allow hot
// path (a token-bucket AllowN plus the adaptive bookkeeping) for a single key.
func BenchmarkAdaptive_Allow_SingleKey(b *testing.B) {
	al := adaptive.New(1_000_000, 1, 1_000_000, healthySignals(),
		adaptive.WithClock(clock.RealClock{}))
	defer al.Close()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		al.Allow(ctx, "bench-key")
	}
}

// BenchmarkAdaptive_Allow_100Keys measures the Allow path under key contention
// across 100 distinct keys.
func BenchmarkAdaptive_Allow_100Keys(b *testing.B) {
	al := adaptive.New(1_000_000, 1, 1_000_000, healthySignals(),
		adaptive.WithClock(clock.RealClock{}))
	defer al.Close()
	ctx := context.Background()
	keys := make([]string, 100)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		al.Allow(ctx, keys[i%100])
	}
}

// BenchmarkAdaptive_ForceAdjust measures the adjustment control loop in
// isolation (the periodic limit-recalculation the background goroutine runs).
func BenchmarkAdaptive_ForceAdjust(b *testing.B) {
	// Alternate stress/healthy so adjust actually moves the limit each cycle
	// rather than short-circuiting on "no change".
	al := adaptive.New(1000, 1, 1_000_000,
		adaptive.NewStaticSignals(90, 0.1, time.Second),
		adaptive.WithClock(clock.RealClock{}))
	defer al.Close()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		al.ForceAdjust()
	}
}
