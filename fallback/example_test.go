package fallback_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/fallback"
)

// ExampleDo runs a primary function and, if it fails, a fallback.
func ExampleDo() {
	err := fallback.Do(context.Background(),
		func(ctx context.Context) error { return errors.New("primary unavailable") },
		func(ctx context.Context, cause error) error {
			fmt.Println("falling back, cause:", cause)
			return nil
		},
	)
	fmt.Println("final err:", err)
	// Output:
	// falling back, cause: primary unavailable
	// final err: <nil>
}
