// Package main demonstrates the resilience.Builder facade — a single fluent
// builder that composes a rate limiter, bulkhead, timeout, circuit breaker,
// retry (with an optional retry budget) and an outer fallback into one stack.
//
// The builder methods may be called in any order; Build() sorts the layers into
// the fixed canonical order (fallback → rate limit → bulkhead → timeout →
// circuit breaker → retry → operation), reusing the same stable ordering the
// pipeline package guarantees. Execute runs an error-only operation; the generic
// resilience.Execute[T] runs a value-returning one.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/bulkhead"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/resilience"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/retry"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/retry/backoff"
)

func main() {
	// Build each primitive with its own constructor, then compose them uniformly.
	limiter := tokenbucket.New(50, 50) // burst 50, 50 tokens/s sustained
	defer limiter.Close()

	cb := circuitbreaker.New(circuitbreaker.Config{
		Name:             "downstream",
		FailureThreshold: 5,
		OpenTimeout:      5 * time.Second,
	})

	bh := bulkhead.New(8, 50*time.Millisecond, bulkhead.WithName("downstream"))

	policy := retry.New(
		retry.WithMaxAttempts(3),
		retry.WithBackoff(backoff.Exponential(10*time.Millisecond, 500*time.Millisecond)),
	)

	// A retry budget caps retries as a fraction of throughput so a failing
	// dependency can't be amplified into an outage by unbounded retries.
	budget := retry.NewBudget(retry.BudgetConfig{Ratio: 0.2, MinPerSecond: 3})

	// Layers are added in an arbitrary order on purpose — Build sorts them into
	// the fixed canonical order.
	builder := resilience.New().
		WithRetryBudget(policy, budget).
		WithTimeout(2*time.Second).
		WithCircuitBreaker(cb).
		WithBulkhead(bh).
		WithRateLimit(limiter, resilience.KeyByValue("global")).
		WithFallback(func(_ context.Context, err error) error {
			// Any failure the inner stack produces lands here; degrade gracefully.
			log.Printf("fallback engaged: %v", err)
			return nil
		})
	stack := builder.Build()

	fmt.Println("stack layers (outermost-to-innermost):", builder.Layers())

	// 1. Error-only Execute: a flaky operation that fails once then succeeds.
	//    The retry layer absorbs the transient failure; no fallback needed.
	var calls int
	err := stack.Execute(context.Background(), func(_ context.Context) error {
		calls++
		if calls == 1 {
			return errors.New("transient blip")
		}
		return nil
	})
	fmt.Printf("Execute: err=%v after %d call(s)\n", err, calls)

	// 2. Generic Execute[T]: a value-returning operation flows straight through.
	n, err := resilience.Execute(context.Background(), stack,
		func(_ context.Context) (int, error) { return 42, nil })
	fmt.Printf("Execute[int]: value=%d err=%v\n", n, err)

	// 3. Generic ExecuteWithFallback[T]: the primary fails, the typed fallback
	//    supplies a default value so the caller still gets a usable result.
	v, err := resilience.ExecuteWithFallback(context.Background(), stack,
		func(_ context.Context) (string, error) { return "", errors.New("boom") },
		func(_ context.Context, cause error) (string, error) {
			return "last-known-good", nil // serve a default on failure
		})
	fmt.Printf("ExecuteWithFallback[string]: value=%q err=%v\n", v, err)
}
