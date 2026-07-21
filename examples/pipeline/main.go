// Package main demonstrates the pipeline builder pattern for composing resilience policies.
// The pipeline applies rate limiting, bulkhead, timeout, circuit breaking, and retry
// in a fixed, well-defined order.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/bulkhead"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/pipeline"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/retry"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/retry/backoff"
)

func main() {
	// 1. Create individual components.
	limiter := tokenbucket.New(10, 20)
	defer limiter.Close()

	bh := bulkhead.New(5, 100*time.Millisecond)

	cb := circuitbreaker.New(circuitbreaker.Config{
		Name:             "example",
		WindowType:       circuitbreaker.CountBased,
		WindowSize:       10,
		FailureThreshold: 3,
		OpenTimeout:      5 * time.Second,
	})

	retryPolicy := &retry.Policy{
		MaxAttempts: 3,
		Backoff:     backoff.Exponential(100*time.Millisecond, 2*time.Second),
		RetryIf: func(err error) bool {
			// Only retry transient errors, not permanent failures
			return !errors.Is(err, pipeline.ErrRateLimited)
		},
	}

	// 2. Build pipeline: RateLimit → Bulkhead → Timeout(2s) → CircuitBreaker → Retry
	p := pipeline.New().
		RateLimit(limiter, pipeline.KeyByValue("global")).
		BulkheadWith(bh).
		Timeout(2 * time.Second).
		CircuitBreaker(cb).
		Retry(retryPolicy).
		Build()

	// 3. Execute operations through the pipeline.
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		attempt := i
		err := p.Execute(ctx, func(ctx context.Context) error {
			fmt.Printf("  Executing operation %d\n", attempt)
			if attempt == 2 {
				return fmt.Errorf("transient error on attempt %d", attempt)
			}
			return nil
		})
		if err != nil {
			log.Printf("Pipeline error on iteration %d: %v", i, err)
		} else {
			log.Printf("Pipeline success on iteration %d", i)
		}
	}
}
