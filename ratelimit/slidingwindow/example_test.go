package slidingwindow_test

import (
	"context"
	"fmt"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/slidingwindow"
)

// ExampleNewLog shows the exact (per-timestamp) sliding window log.
func ExampleNewLog() {
	lim := slidingwindow.NewLog(2, time.Minute)
	defer lim.Close()

	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		fmt.Printf("request %d allowed=%v\n", i, lim.Allow(ctx, "ip").Allowed)
	}
	// Output:
	// request 1 allowed=true
	// request 2 allowed=true
	// request 3 allowed=false
}

// ExampleNewCounter shows the memory-efficient sliding window counter.
func ExampleNewCounter() {
	lim := slidingwindow.NewCounter(2, time.Minute)
	defer lim.Close()

	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		fmt.Printf("request %d allowed=%v\n", i, lim.Allow(ctx, "ip").Allowed)
	}
	// Output:
	// request 1 allowed=true
	// request 2 allowed=true
	// request 3 allowed=false
}
