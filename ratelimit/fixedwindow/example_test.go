package fixedwindow_test

import (
	"context"
	"fmt"
	"time"

	"github.com/sanskarpan/resilience/ratelimit/fixedwindow"
)

// ExampleNew demonstrates basic fixed window rate limiting.
func ExampleNew() {
	// 5 requests per second
	fw := fixedwindow.New(5, time.Second)
	defer fw.Close() //nolint:errcheck
	ctx := context.Background()

	for i := 0; i < 6; i++ {
		result := fw.Allow(ctx, "user:123")
		if result.Allowed {
			fmt.Printf("request %d: allowed (remaining=%d)\n", i+1, result.Remaining)
		} else {
			fmt.Println("request denied: rate limit exceeded")
		}
	}
	// Output:
	// request 1: allowed (remaining=4)
	// request 2: allowed (remaining=3)
	// request 3: allowed (remaining=2)
	// request 4: allowed (remaining=1)
	// request 5: allowed (remaining=0)
	// request denied: rate limit exceeded
}

// ExampleNew_multipleKeys shows that different keys are rate limited independently.
func ExampleNew_multipleKeys() {
	fw := fixedwindow.New(2, time.Second)
	defer fw.Close() //nolint:errcheck
	ctx := context.Background()

	// Exhaust user:alice
	fw.Allow(ctx, "user:alice") //nolint:errcheck
	fw.Allow(ctx, "user:alice") //nolint:errcheck

	// user:bob still has full capacity
	result := fw.Allow(ctx, "user:bob")
	fmt.Println("bob allowed:", result.Allowed)

	result = fw.Allow(ctx, "user:alice")
	fmt.Println("alice allowed:", result.Allowed)

	// Output:
	// bob allowed: true
	// alice allowed: false
}
