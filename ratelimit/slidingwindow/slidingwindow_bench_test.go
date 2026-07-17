package slidingwindow_test

import (
	"context"
	"testing"
	"time"

	"github.com/sanskarpan/resilience/ratelimit/slidingwindow"
)

// BenchmarkSlidingWindowLog_Allow_100Keys benchmarks with 100 distinct keys.
func BenchmarkSlidingWindowLog_Allow_100Keys(b *testing.B) {
	sw := slidingwindow.NewLog(1000, time.Second)
	defer sw.Close() //nolint:errcheck
	ctx := context.Background()
	keys := make([]string, 100)
	for i := range keys {
		keys[i] = "key-" + string(rune('a'+i%26)) + string(rune('0'+i/26))
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sw.Allow(ctx, keys[i%100])
	}
}

// BenchmarkSlidingWindowLog_Allow_Parallel benchmarks concurrent access.
func BenchmarkSlidingWindowLog_Allow_Parallel(b *testing.B) {
	sw := slidingwindow.NewLog(100000, time.Second)
	defer sw.Close() //nolint:errcheck
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sw.Allow(ctx, "parallel-key")
		}
	})
}

// BenchmarkSlidingWindowCounter_Allow_100Keys benchmarks with 100 distinct keys.
func BenchmarkSlidingWindowCounter_Allow_100Keys(b *testing.B) {
	sw := slidingwindow.NewCounter(1000, time.Second)
	defer sw.Close() //nolint:errcheck
	ctx := context.Background()
	keys := make([]string, 100)
	for i := range keys {
		keys[i] = "key-" + string(rune('a'+i%26)) + string(rune('0'+i/26))
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sw.Allow(ctx, keys[i%100])
	}
}

// BenchmarkSlidingWindowCounter_Peek benchmarks Peek (read-only, no consumption).
func BenchmarkSlidingWindowCounter_Peek(b *testing.B) {
	sw := slidingwindow.NewCounter(100, time.Second)
	defer sw.Close() //nolint:errcheck
	ctx := context.Background()
	sw.Allow(ctx, "bench-key") //nolint:errcheck — prime the key

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sw.Peek(ctx, "bench-key")
	}
}
