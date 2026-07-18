package bulkhead_test

import (
	"context"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/bulkhead"
)

func noop(_ context.Context) error { return nil }

// BenchmarkBulkhead_Execute_NonBlocking measures the non-blocking acquire path
// (maxWait == 0): a slot is always free so every call runs the fast semaphore
// send/receive around fn.
func BenchmarkBulkhead_Execute_NonBlocking(b *testing.B) {
	bh := bulkhead.New(1<<20, 0)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = bh.Execute(ctx, noop)
	}
}

// BenchmarkBulkhead_Execute_NonBlocking_Parallel measures the non-blocking path
// under concurrency (semaphore contention with plenty of slots).
func BenchmarkBulkhead_Execute_NonBlocking_Parallel(b *testing.B) {
	bh := bulkhead.New(1<<20, 0)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = bh.Execute(ctx, noop)
		}
	})
}

// BenchmarkBulkhead_Execute_Blocking measures the blocking acquire path
// (maxWait > 0): a slot is always immediately available, so this exercises the
// timer setup and wait-stat accounting on the blocking branch without ever
// actually blocking.
func BenchmarkBulkhead_Execute_Blocking(b *testing.B) {
	bh := bulkhead.New(1<<20, time.Second)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = bh.Execute(ctx, noop)
	}
}
