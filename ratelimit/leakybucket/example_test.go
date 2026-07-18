package leakybucket_test

import (
	"context"
	"fmt"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/leakybucket"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/store"
)

func Example() {
	// Leaky bucket: queue of 10, processes at 5 requests/second.
	// Unlike token bucket, this enforces a constant output rate.
	limiter := leakybucket.New(10, 5)
	defer limiter.Close()

	ctx := context.Background()
	result := limiter.Allow(ctx, "user:123")
	if result.Allowed {
		fmt.Println("Request processed")
	} else {
		fmt.Printf("Queue full. Retry after: %s\n", result.RetryAfter)
	}
	// Output: Request processed
}

// ExampleLeakyBucket_AllowP shows priority-aware waiting: under contention, a
// higher-priority caller acquires the next leaked slot before a lower-priority
// one on the same key (ENHANCEMENTS §1.7).
func ExampleLeakyBucket_AllowP() {
	limiter := leakybucket.New(10, 5)
	defer limiter.Close()

	ctx := context.Background()
	// priority 10 is served ahead of any lower-priority waiter on "user:123".
	result := limiter.AllowP(ctx, "user:123", 10)
	if result.Allowed {
		fmt.Println("High-priority request processed")
	}
	// Output: High-priority request processed
}

// ExampleNewDistributed shows a Redis-backed leaky bucket sharing one queue
// across a fleet (ENHANCEMENTS §1.8). Here it runs against an in-memory store
// with the script emulations so the example is self-contained.
func ExampleNewDistributed() {
	s := store.NewMemoryWithScripts()
	defer s.Close()

	// Queue depth 10, drains at 5 requests/second, shared under the "svc" prefix.
	limiter := leakybucket.NewDistributed(10, 5, s, "svc")
	defer limiter.Close()

	ctx := context.Background()
	if limiter.Allow(ctx, "user:123").Allowed {
		fmt.Println("Distributed request admitted")
	}
	// Output: Distributed request admitted
}
