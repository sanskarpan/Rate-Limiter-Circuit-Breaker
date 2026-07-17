package timeout_test

import (
	"context"
	"fmt"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/timeout"
)

// ExampleDo bounds the execution of fn by a deadline.
func ExampleDo() {
	err := timeout.Do(context.Background(), 50*time.Millisecond, func(ctx context.Context) error {
		return nil // completes well within the deadline
	})
	fmt.Println("err:", err)
	// Output: err: <nil>
}
