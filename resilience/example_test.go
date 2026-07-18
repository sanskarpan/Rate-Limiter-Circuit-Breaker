package resilience_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/bulkhead"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/resilience"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/retry"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/retry/backoff"
)

// Example builds a full resilience stack — rate limiter, circuit breaker,
// retry, bulkhead, timeout, and fallback — and executes an operation through it.
// The layers are configured in an arbitrary order; Build sorts them into the
// fixed canonical order (fallback → rate limit → bulkhead → timeout → breaker →
// retry → operation).
func Example() {
	// Each primitive is built with its own constructor, then composed uniformly.
	limiter := tokenbucket.New(100, 100) // capacity 100 (burst), refill 100/s
	cb := circuitbreaker.New(circuitbreaker.Config{
		Name:             "downstream",
		FailureThreshold: 5,
	})
	bh := bulkhead.New(8, 50*time.Millisecond, bulkhead.WithName("downstream"))
	policy := retry.New(
		retry.WithMaxAttempts(3),
		retry.WithBackoff(backoff.Constant(time.Millisecond)),
	)

	stack := resilience.New().
		WithRetry(policy).
		WithTimeout(2*time.Second).
		WithCircuitBreaker(cb).
		WithBulkhead(bh).
		WithRateLimit(limiter, resilience.KeyByValue("global")).
		WithFallback(func(ctx context.Context, err error) error {
			// Any failure the stack produces lands here; degrade gracefully.
			fmt.Println("fallback engaged:", err)
			return nil
		}).
		Build()

	err := stack.Execute(context.Background(), func(ctx context.Context) error {
		return nil // the protected operation
	})
	fmt.Println("execute err:", err)

	// Output:
	// execute err: <nil>
}

// ExampleExecute shows the generic, value-returning entry point together with a
// typed fallback via ExecuteWithFallback.
func ExampleExecute() {
	cb := circuitbreaker.New(circuitbreaker.Config{Name: "svc"})
	stack := resilience.New().
		WithCircuitBreaker(cb).
		WithTimeout(time.Second).
		Build()

	// Happy path: the typed value flows straight through.
	n, err := resilience.Execute(context.Background(), stack,
		func(ctx context.Context) (int, error) { return 42, nil })
	fmt.Println(n, err)

	// Failure path with a typed fallback that supplies a default value.
	sentinel := errors.New("boom")
	v, err := resilience.ExecuteWithFallback(context.Background(), stack,
		func(ctx context.Context) (string, error) { return "", sentinel },
		func(ctx context.Context, cause error) (string, error) {
			return "default", nil
		})
	fmt.Println(v, err)

	// Output:
	// 42 <nil>
	// default <nil>
}
