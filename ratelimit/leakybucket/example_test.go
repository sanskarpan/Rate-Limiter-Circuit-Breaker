package leakybucket_test

import (
	"context"
	"fmt"

	"github.com/sanskarpan/resilience/ratelimit/leakybucket"
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
