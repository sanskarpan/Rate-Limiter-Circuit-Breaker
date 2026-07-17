package ratelimit_test

import (
	"context"
	"fmt"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
)

// Every algorithm implements the ratelimit.Limiter interface, so you can swap
// implementations without changing call sites.
func Example() {
	// Capacity 2, refilling 1 token/second.
	lim := tokenbucket.New(2, 1)
	defer lim.Close()

	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		r := lim.Allow(ctx, "user:42")
		fmt.Printf("request %d allowed=%v\n", i, r.Allowed)
	}
	// Output:
	// request 1 allowed=true
	// request 2 allowed=true
	// request 3 allowed=false
}
