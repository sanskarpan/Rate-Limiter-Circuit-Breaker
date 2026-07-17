package leakybucket_test

import (
	"context"
	"testing"

	"github.com/sanskarpan/resilience/internal/clock"
	"github.com/sanskarpan/resilience/ratelimit/leakybucket"
)

func BenchmarkLeakyBucket_Allow_SingleKey(b *testing.B) {
	lb := leakybucket.New(1000000, 1000000, leakybucket.WithClock(clock.RealClock{}))
	defer lb.Close()
	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		lb.Allow(ctx, "key")
	}
}

func BenchmarkLeakyBucket_Allow_100Keys(b *testing.B) {
	lb := leakybucket.New(1000000, 1000000, leakybucket.WithClock(clock.RealClock{}))
	defer lb.Close()
	ctx := context.Background()
	keys := make([]string, 100)
	for i := range keys {
		keys[i] = "key-" + string(rune('0'+i%10))
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		lb.Allow(ctx, keys[i%100])
	}
}

func BenchmarkLeakyBucket_Allow_Parallel(b *testing.B) {
	lb := leakybucket.New(1000000, 1000000, leakybucket.WithClock(clock.RealClock{}))
	defer lb.Close()
	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			lb.Allow(ctx, "key")
		}
	})
}
