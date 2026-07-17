package tokenbucket_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/sanskarpan/resilience/internal/clock"
	"github.com/sanskarpan/resilience/ratelimit/tokenbucket"
)

func BenchmarkTokenBucket_Allow_SingleKey(b *testing.B) {
	tb := tokenbucket.New(1e9, 1e9, tokenbucket.WithClock(clock.RealClock{}))
	defer tb.Close()
	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		tb.Allow(ctx, "bench-key")
	}
}

func BenchmarkTokenBucket_Allow_100Keys(b *testing.B) {
	tb := tokenbucket.New(1e9, 1e9, tokenbucket.WithClock(clock.RealClock{}))
	defer tb.Close()
	ctx := context.Background()
	keys := make([]string, 100)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		tb.Allow(ctx, keys[i%100])
	}
}

func BenchmarkTokenBucket_AllowN_5(b *testing.B) {
	tb := tokenbucket.New(1e9, 1e9, tokenbucket.WithClock(clock.RealClock{}))
	defer tb.Close()
	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		tb.AllowN(ctx, "bench-key", 5)
	}
}

func BenchmarkTokenBucket_Allow_Parallel(b *testing.B) {
	tb := tokenbucket.New(1e9, 1e9, tokenbucket.WithClock(clock.RealClock{}))
	defer tb.Close()
	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tb.Allow(ctx, "bench-key")
		}
	})
}

func BenchmarkTokenBucket_Peek(b *testing.B) {
	tb := tokenbucket.New(1e9, 1e9, tokenbucket.WithClock(clock.RealClock{}))
	defer tb.Close()
	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		tb.Peek(ctx, "bench-key")
	}
}
