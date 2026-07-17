package gcra_test

import (
	"context"
	"fmt"
	"time"

	"github.com/sanskarpan/resilience/ratelimit/gcra"
)

func Example() {
	// GCRA: 10 requests per second, burst of 3.
	// emissionInterval = 100ms, burstOffset = 200ms
	limiter := gcra.New(10, 3, time.Second)
	defer limiter.Close()

	ctx := context.Background()
	result := limiter.Allow(ctx, "user:123")
	if result.Allowed {
		fmt.Printf("Request allowed. Remaining: %d\n", result.Remaining)
	} else {
		fmt.Printf("Rate limited. Retry after: %s\n", result.RetryAfter)
	}
	// Output: Request allowed. Remaining: 2
}

func ExampleGCRA_AllowN() {
	// GCRA with burst=5: consume 3 at once
	limiter := gcra.New(10, 5, time.Second)
	defer limiter.Close()

	ctx := context.Background()
	result := limiter.AllowN(ctx, "batch", 3)
	fmt.Printf("Consumed 3: allowed=%v, remaining=%d\n", result.Allowed, result.Remaining)
	// Output: Consumed 3: allowed=true, remaining=2
}
