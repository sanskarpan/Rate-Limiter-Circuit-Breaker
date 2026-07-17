package backoff_test

import (
	"fmt"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/retry/backoff"
)

// ExampleExponential shows deterministic exponential backoff capped at a max.
func ExampleExponential() {
	b := backoff.Exponential(100*time.Millisecond, 2*time.Second)
	for attempt := 0; attempt < 4; attempt++ {
		fmt.Println(b.Next(attempt))
	}
	// Output:
	// 100ms
	// 200ms
	// 400ms
	// 800ms
}
