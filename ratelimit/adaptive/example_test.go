package adaptive_test

import (
	"context"
	"fmt"
	"time"

	"github.com/sanskarpan/resilience/ratelimit/adaptive"
)

func Example() {
	// Create an adaptive limiter that adjusts between 10 and 1000 req/s.
	// Starts at 100 req/s and adjusts based on system signals.
	signals := adaptive.NewRuntimeSignals()
	limiter := adaptive.New(100, 10, 1000, signals)
	defer limiter.Close()

	// Record request outcomes to feed the signal source
	signals.RecordSuccess(5 * time.Millisecond)
	signals.RecordSuccess(8 * time.Millisecond)

	ctx := context.Background()
	result := limiter.Allow(ctx, "user:123")
	if result.Allowed {
		fmt.Println("Request allowed")
	}
	// Output: Request allowed
}
