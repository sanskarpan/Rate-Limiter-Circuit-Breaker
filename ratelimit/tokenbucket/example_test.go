package tokenbucket_test

import (
	"context"
	"fmt"

	"github.com/sanskarpan/resilience/ratelimit/tokenbucket"
)

func Example() {
	// Create a token bucket: 10 tokens capacity, refills at 5 tokens/second.
	// This allows a burst of 10 requests, then sustains 5 requests/second.
	limiter := tokenbucket.New(10, 5)
	defer limiter.Close()

	ctx := context.Background()
	result := limiter.Allow(ctx, "user:123")
	if result.Allowed {
		fmt.Printf("Request allowed. Remaining: %d\n", result.Remaining)
	} else {
		fmt.Printf("Rate limited. Retry after: %s\n", result.RetryAfter)
	}
	// Output: Request allowed. Remaining: 9
}

func ExampleTokenBucket_Wait() {
	// Wait blocks until a token is available or context is cancelled.
	limiter := tokenbucket.New(1, 10) // 1 token, refills at 10/sec
	defer limiter.Close()

	ctx := context.Background()

	// This succeeds immediately (1 token available)
	if err := limiter.Wait(ctx, "user:456"); err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	fmt.Println("Got token!")
	// Output: Got token!
}

func ExampleTokenBucket_AllowN() {
	// AllowN checks if n tokens are available atomically.
	// Either all n are consumed or none (never partial).
	limiter := tokenbucket.New(5, 1)
	defer limiter.Close()

	ctx := context.Background()
	result := limiter.AllowN(ctx, "batch-job", 3)
	fmt.Printf("Consumed 3 tokens: allowed=%v, remaining=%d\n", result.Allowed, result.Remaining)
	// Output: Consumed 3 tokens: allowed=true, remaining=2
}
