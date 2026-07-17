package pipeline_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/pipeline"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/retry"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/retry/backoff"
)

func Example() {
	limiter := tokenbucket.New(100, 10)
	defer limiter.Close()

	cb := circuitbreaker.New(circuitbreaker.Config{
		Name:             "payment-service",
		WindowType:       circuitbreaker.CountBased,
		WindowSize:       10,
		FailureThreshold: 5,
		OpenTimeout:      30 * time.Second,
	})

	p := pipeline.New().
		RateLimit(limiter, pipeline.KeyByValue("global")).
		Bulkhead(50, 10*time.Millisecond).
		Timeout(5 * time.Second).
		CircuitBreaker(cb).
		Retry(&retry.Policy{
			MaxAttempts: 3,
			Backoff:     backoff.Exponential(100*time.Millisecond, 2*time.Second),
		}).
		Build()

	ctx := context.Background()
	err := p.Execute(ctx, func(ctx context.Context) error {
		// Call downstream service
		return nil
	})
	if err != nil {
		if errors.Is(err, pipeline.ErrRateLimited) {
			fmt.Println("rate limited")
		} else if errors.Is(err, circuitbreaker.ErrCircuitOpen) {
			fmt.Println("circuit open")
		} else {
			fmt.Printf("error: %v\n", err)
		}
	} else {
		fmt.Println("success")
	}
	// Output: success
}

func ExampleNew_customKeyFunction() {
	type userKeyCtx struct{}

	limiter := tokenbucket.New(10, 5)
	defer limiter.Close()

	// Extract per-user rate limit key from context
	p := pipeline.New().
		RateLimit(limiter, pipeline.KeyFromContext(
			func(ctx context.Context) string {
				if uid, ok := ctx.Value(userKeyCtx{}).(string); ok {
					return uid
				}
				return ""
			},
			"anonymous",
		)).
		Build()

	ctx := context.WithValue(context.Background(), userKeyCtx{}, "user:42")
	err := p.Execute(ctx, func(ctx context.Context) error {
		return nil
	})
	fmt.Printf("err=%v\n", err)
	// Output: err=<nil>
}
