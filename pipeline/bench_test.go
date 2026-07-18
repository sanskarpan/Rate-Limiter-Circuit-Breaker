package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/pipeline"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/retry"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/retry/backoff"
)

// noop is the innermost operation: always succeeds so every stage runs its
// happy path and we measure per-request pipeline overhead, not failure handling.
func noop(_ context.Context) error { return nil }

// BenchmarkPipeline_Execute builds a representative, production-shaped pipeline
// (rate limit → bulkhead → timeout → circuit breaker → retry) with everything
// sized so a request flows straight through and measures the steady-state
// Execute overhead across all stages.
func BenchmarkPipeline_Execute(b *testing.B) {
	tb := tokenbucket.New(1e9, 1e9, tokenbucket.WithClock(clock.RealClock{}))
	defer tb.Close()

	cb := circuitbreaker.New(circuitbreaker.Config{
		Name:             "bench",
		WindowType:       circuitbreaker.CountBased,
		WindowSize:       100,
		FailureThreshold: 100,
		Clock:            clock.RealClock{},
	})

	p := pipeline.New().
		RateLimit(tb, pipeline.KeyByValue("bench-key")).
		Bulkhead(1<<20, 0).
		Timeout(time.Minute).
		CircuitBreaker(cb).
		Retry(&retry.Policy{
			MaxAttempts: 3,
			Backoff:     backoff.Constant(time.Millisecond),
			Clock:       clock.RealClock{},
		}).
		Build()

	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = p.Execute(ctx, noop)
	}
}

// BenchmarkPipeline_Execute_RateLimitBulkhead measures a leaner two-stage
// pipeline (rate limit + non-blocking bulkhead) — the common "admission +
// concurrency cap" combination.
func BenchmarkPipeline_Execute_RateLimitBulkhead(b *testing.B) {
	tb := tokenbucket.New(1e9, 1e9, tokenbucket.WithClock(clock.RealClock{}))
	defer tb.Close()

	p := pipeline.New().
		RateLimit(tb, pipeline.KeyByValue("bench-key")).
		Bulkhead(1<<20, 0).
		Build()

	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = p.Execute(ctx, noop)
	}
}
