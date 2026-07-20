package gcra_test

import (
	"context"
	"fmt"
	"sync/atomic"
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

// BenchmarkGCRA_Allow_ParallelManyKeys hits many distinct keys concurrently.
// This is the workload the sharded key map (§3.1) targets: without sharding,
// every key creation/lookup serialized on one global RWMutex. Each goroutine
// walks its own disjoint key range so contention is on the map, not per-entry.
func BenchmarkGCRA_Allow_ParallelManyKeys(b *testing.B) {
	g := gcra.New(1000000, 1000000, time.Second, gcra.WithClock(clock.RealClock{}))
	defer g.Close()
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
			g.Allow(ctx, keys[i%keysPerG])
			i++
		}
	})
}
