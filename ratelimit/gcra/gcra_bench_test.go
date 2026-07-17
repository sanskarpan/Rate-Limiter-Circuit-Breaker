package gcra_test

import (
	"context"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/gcra"
)

func BenchmarkGCRA_Allow_SingleKey(b *testing.B) {
	g := gcra.New(1000000, 1000000, time.Second, gcra.WithClock(clock.RealClock{}))
	defer g.Close()
	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		g.Allow(ctx, "key")
	}
}

func BenchmarkGCRA_Allow_100Keys(b *testing.B) {
	g := gcra.New(1000000, 1000000, time.Second, gcra.WithClock(clock.RealClock{}))
	defer g.Close()
	ctx := context.Background()
	keys := make([]string, 100)
	for i := range keys {
		keys[i] = "key-" + string(rune('0'+i%10))
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		g.Allow(ctx, keys[i%100])
	}
}

func BenchmarkGCRA_Allow_Parallel(b *testing.B) {
	g := gcra.New(1000000, 1000000, time.Second, gcra.WithClock(clock.RealClock{}))
	defer g.Close()
	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			g.Allow(ctx, "key")
		}
	})
}
