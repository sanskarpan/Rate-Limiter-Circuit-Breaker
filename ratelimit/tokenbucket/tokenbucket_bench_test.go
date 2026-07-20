package tokenbucket_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
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

// BenchmarkTokenBucket_Allow_ParallelManyKeys hits many distinct keys
// concurrently. This is the workload the sharded key map (§3.1) targets:
// without sharding, every key lookup/creation serialized on one global RWMutex.
// Each goroutine walks its own disjoint key range so contention is on the map,
// not the per-bucket mutex.
func BenchmarkTokenBucket_Allow_ParallelManyKeys(b *testing.B) {
	tb := tokenbucket.New(1e9, 1e9, tokenbucket.WithClock(clock.RealClock{}))
	defer tb.Close()
	const keysPerG = 512
	ctx := context.Background()
	var gid atomic.Int64
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		base := gid.Add(1) * keysPerG
		keys := make([]string, keysPerG)
		for i := range keys {
			keys[i] = fmt.Sprintf("k-%d", base+int64(i))
		}
		i := 0
		for pb.Next() {
			tb.Allow(ctx, keys[i%keysPerG])
			i++
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
