package circuitbreaker_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
)

func Example() {
	cb := circuitbreaker.New(circuitbreaker.Config{
		Name:             "my-service",
		WindowType:       circuitbreaker.CountBased,
		WindowSize:       10,
		FailureThreshold: 5,
	})

	ctx := context.Background()
	err := cb.Execute(ctx, func(ctx context.Context) error {
		// Call your downstream service here
		return nil
	})
	if err != nil {
		if errors.Is(err, circuitbreaker.ErrCircuitOpen) {
			fmt.Println("Circuit is open — fast fail")
		} else {
			fmt.Printf("Error: %v\n", err)
		}
		return
	}
	fmt.Println("Success")
	// Output: Success
}

func ExampleCircuitBreaker_ExecuteWithFallback() {
	cb := circuitbreaker.New(circuitbreaker.Config{
		Name:             "payment-service",
		WindowType:       circuitbreaker.CountBased,
		WindowSize:       10,
		FailureThreshold: 5,
	})

	ctx := context.Background()
	err := cb.ExecuteWithFallback(
		ctx,
		func(ctx context.Context) error {
			return nil // primary call succeeds
		},
		func(ctx context.Context, origErr error) error {
			fmt.Printf("Fallback: %v\n", origErr)
			return nil
		},
	)
	fmt.Printf("err=%v\n", err)
	// Output: err=<nil>
}
