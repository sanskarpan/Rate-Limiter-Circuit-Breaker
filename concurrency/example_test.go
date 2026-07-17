package concurrency_test

import (
	"context"
	"fmt"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/concurrency"
)

// Example demonstrates the basic non-blocking admission pattern: Acquire a slot,
// do the work, and report the outcome (RTT and whether it was dropped) back to
// the limiter so it can adapt the concurrency limit.
func Example() {
	lim := concurrency.NewGradient2(concurrency.Config{
		InitialLimit: 20,
		MaxLimit:     1000,
		MinLimit:     4,
		RTTTolerance: 1.5,
	})

	release, ok := lim.Acquire(context.Background())
	if !ok {
		// At the current concurrency limit — shed this request.
		fmt.Println("shed")
		return
	}
	// ... perform the guarded work, measuring its round-trip time ...
	measured := 5 * time.Millisecond
	release(concurrency.Outcome{RTT: measured, Dropped: false})

	// After release the slot is returned; the limit adapts from the reported RTT.
	fmt.Printf("admitted and released, inflight=%d\n", lim.Inflight())
	// Output: admitted and released, inflight=0
}
