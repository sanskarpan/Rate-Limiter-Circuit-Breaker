package pipeline_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/bulkhead"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/pipeline"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/retry"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/retry/backoff"
)

var errDownstream = errors.New("downstream error")

func succeed(ctx context.Context) error { return nil }
func fail(ctx context.Context) error    { return errDownstream }

// ---------------------------------------------------------------------------
// Basic pipeline passthrough
// ---------------------------------------------------------------------------

func TestPipeline_EmptyPipeline_PassesThrough(t *testing.T) {
	p := pipeline.New().Build()
	err := p.Execute(context.Background(), succeed)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPipeline_EmptyPipeline_PropagatesError(t *testing.T) {
	p := pipeline.New().Build()
	err := p.Execute(context.Background(), fail)
	if !errors.Is(err, errDownstream) {
		t.Fatalf("expected errDownstream, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Rate limiter stage
// ---------------------------------------------------------------------------

func TestPipeline_RateLimit_AllowsUnderLimit(t *testing.T) {
	limiter := tokenbucket.New(10, 10, tokenbucket.WithClock(clock.RealClock{}))
	defer limiter.Close()

	p := pipeline.New().
		RateLimit(limiter, pipeline.KeyByValue("test")).
		Build()

	for i := 0; i < 5; i++ {
		err := p.Execute(context.Background(), succeed)
		if err != nil {
			t.Fatalf("iteration %d: unexpected error: %v", i, err)
		}
	}
}

func TestPipeline_RateLimit_RejectsWhenExhausted(t *testing.T) {
	// 1 token, refill rate = 1/s means very slow refill
	limiter := tokenbucket.New(1, 1, tokenbucket.WithClock(clock.RealClock{}))
	defer limiter.Close()

	p := pipeline.New().
		RateLimit(limiter, pipeline.KeyByValue("test")).
		Build()

	// First request consumes the token
	err := p.Execute(context.Background(), succeed)
	if err != nil {
		t.Fatalf("first request: unexpected error: %v", err)
	}

	// Second request should be rate limited
	err = p.Execute(context.Background(), succeed)
	if !errors.Is(err, pipeline.ErrRateLimited) {
		t.Fatalf("expected ErrRateLimited, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Bulkhead stage
// ---------------------------------------------------------------------------

func TestPipeline_Bulkhead_LimitsConcurrency(t *testing.T) {
	const maxConcurrency = 2
	p := pipeline.New().
		Bulkhead(maxConcurrency, 0). // non-blocking
		Build()

	inFlight := make(chan struct{}, maxConcurrency+2)
	block := make(chan struct{})
	var rejected int32

	// Start maxConcurrency+1 concurrent calls
	for i := 0; i < maxConcurrency+1; i++ {
		go func() {
			err := p.Execute(context.Background(), func(ctx context.Context) error {
				inFlight <- struct{}{}
				<-block
				return nil
			})
			if errors.Is(err, bulkhead.ErrBulkheadFull) {
				atomic.AddInt32(&rejected, 1)
				inFlight <- struct{}{} // signal completion even if rejected
			}
		}()
	}

	// Wait for all goroutines to either start or reject
	for i := 0; i < maxConcurrency+1; i++ {
		select {
		case <-inFlight:
		case <-time.After(2 * time.Second):
			t.Fatal("goroutine did not complete")
		}
	}

	close(block) // unblock all

	if atomic.LoadInt32(&rejected) < 1 {
		t.Error("expected at least one rejection from bulkhead")
	}
}

// ---------------------------------------------------------------------------
// Timeout stage
// ---------------------------------------------------------------------------

func TestPipeline_Timeout_EnforcesDeadline(t *testing.T) {
	p := pipeline.New().
		Timeout(20 * time.Millisecond).
		Build()

	err := p.Execute(context.Background(), func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
			return nil
		}
	})

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
}

func TestPipeline_Timeout_ZeroDuration_NoTimeout(t *testing.T) {
	p := pipeline.New().
		Timeout(0). // no timeout
		Build()

	err := p.Execute(context.Background(), succeed)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Circuit breaker stage
// ---------------------------------------------------------------------------

func TestPipeline_CircuitBreaker_OpenOnFailures(t *testing.T) {
	cb := circuitbreaker.New(circuitbreaker.Config{
		Name:             "test-cb",
		WindowType:       circuitbreaker.CountBased,
		WindowSize:       5,
		FailureThreshold: 3,
		OpenTimeout:      10 * time.Second,
		Clock:            clock.RealClock{},
	})

	p := pipeline.New().
		CircuitBreaker(cb).
		Build()

	// Trigger 3 failures to open the circuit
	for i := 0; i < 3; i++ {
		p.Execute(context.Background(), fail) //nolint:errcheck
	}

	if cb.State() != circuitbreaker.StateOpen {
		t.Fatalf("expected StateOpen, got %s", cb.State())
	}

	// Next call should be rejected
	err := p.Execute(context.Background(), succeed)
	if !errors.Is(err, circuitbreaker.ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Retry stage
// ---------------------------------------------------------------------------

func TestPipeline_Retry_RetriesOnError(t *testing.T) {
	var calls int32
	p := pipeline.New().
		Retry(&retry.Policy{
			MaxAttempts: 3,
			Backoff:     backoff.Constant(0),
		}).
		Build()

	err := p.Execute(context.Background(), func(ctx context.Context) error {
		n := atomic.AddInt32(&calls, 1)
		if n < 3 {
			return errDownstream
		}
		return nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if atomic.LoadInt32(&calls) != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

// ---------------------------------------------------------------------------
// Full pipeline: RateLimit → Bulkhead → Timeout → CircuitBreaker → Retry
// ---------------------------------------------------------------------------

func TestPipeline_Full_SuccessPath(t *testing.T) {
	limiter := tokenbucket.New(100, 100, tokenbucket.WithClock(clock.RealClock{}))
	defer limiter.Close()

	cb := circuitbreaker.New(circuitbreaker.Config{
		Name:             "full-cb",
		WindowType:       circuitbreaker.CountBased,
		WindowSize:       10,
		FailureThreshold: 5,
		Clock:            clock.RealClock{},
	})

	p := pipeline.New().
		RateLimit(limiter, pipeline.KeyByValue("user")).
		Bulkhead(10, 100*time.Millisecond).
		Timeout(1 * time.Second).
		CircuitBreaker(cb).
		Retry(&retry.Policy{MaxAttempts: 1}).
		Build()

	for i := 0; i < 10; i++ {
		err := p.Execute(context.Background(), succeed)
		if err != nil {
			t.Fatalf("iteration %d: unexpected error: %v", i, err)
		}
	}
}

func TestPipeline_Full_RateLimitShortCircuits(t *testing.T) {
	// Rate limiter with 1 token — second request denied without touching CB
	limiter := tokenbucket.New(1, 1, tokenbucket.WithClock(clock.RealClock{}))
	defer limiter.Close()

	var fnCalls int32
	cb := circuitbreaker.New(circuitbreaker.Config{
		Name:             "ratelimit-cb",
		WindowType:       circuitbreaker.CountBased,
		WindowSize:       10,
		FailureThreshold: 2,
		Clock:            clock.RealClock{},
	})

	p := pipeline.New().
		RateLimit(limiter, pipeline.KeyByValue("user")).
		CircuitBreaker(cb).
		Build()

	// First call succeeds
	p.Execute(context.Background(), func(ctx context.Context) error { //nolint:errcheck
		atomic.AddInt32(&fnCalls, 1)
		return nil
	})

	// Second call is rate limited — fn should NOT be called, CB should NOT record failure
	err := p.Execute(context.Background(), func(ctx context.Context) error {
		atomic.AddInt32(&fnCalls, 1)
		return nil
	})
	if !errors.Is(err, pipeline.ErrRateLimited) {
		t.Fatalf("expected ErrRateLimited, got %v", err)
	}

	// CB should still be closed — rate limit doesn't count as CB failure
	if cb.State() != circuitbreaker.StateClosed {
		t.Errorf("expected StateClosed, got %s", cb.State())
	}
	if atomic.LoadInt32(&fnCalls) != 1 {
		t.Errorf("expected 1 fn call, got %d", fnCalls)
	}
}

func TestPipeline_StageOrder_TimeoutBeforeCB(t *testing.T) {
	// Verify that timeout context propagates into CB.Execute, not the other way around.
	// CB receives a context that already has the timeout applied.
	timeoutSeen := make(chan time.Duration, 1)
	cb := circuitbreaker.New(circuitbreaker.Config{
		Name:             "order-cb",
		WindowType:       circuitbreaker.CountBased,
		WindowSize:       10,
		FailureThreshold: 10,
		Clock:            clock.RealClock{},
	})

	p := pipeline.New().
		Timeout(50 * time.Millisecond).
		CircuitBreaker(cb).
		Build()

	p.Execute(context.Background(), func(ctx context.Context) error { //nolint:errcheck
		deadline, ok := ctx.Deadline()
		if ok {
			timeoutSeen <- time.Until(deadline)
		}
		return nil
	})

	select {
	case d := <-timeoutSeen:
		if d <= 0 || d > 50*time.Millisecond {
			t.Errorf("unexpected deadline duration: %v", d)
		}
	case <-time.After(500 * time.Millisecond):
		t.Error("fn was not called or deadline not set")
	}
}

func TestPipeline_CustomStage(t *testing.T) {
	var stageCalled bool
	p := pipeline.New().
		Use(func(ctx context.Context, fn func(context.Context) error) error {
			stageCalled = true
			return fn(ctx)
		}).
		Build()

	err := p.Execute(context.Background(), succeed)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !stageCalled {
		t.Error("custom stage was not called")
	}
}

func TestPipeline_KeyFunctions(t *testing.T) {
	// Test KeyByValue
	kf := pipeline.KeyByValue("global")
	if kf(context.Background()) != "global" {
		t.Error("KeyByValue should return fixed key")
	}

	// Test KeyFromContext
	type ctxKey struct{}
	kf2 := pipeline.KeyFromContext(func(ctx context.Context) string {
		if v, ok := ctx.Value(ctxKey{}).(string); ok {
			return v
		}
		return ""
	}, "default")

	ctx := context.WithValue(context.Background(), ctxKey{}, "user:123")
	if kf2(ctx) != "user:123" {
		t.Error("KeyFromContext should extract value from context")
	}
	if kf2(context.Background()) != "default" {
		t.Error("KeyFromContext should fall back to default")
	}
}

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------

func TestPipeline_Concurrent_NoRace(t *testing.T) {
	limiter := tokenbucket.New(10000, 10000, tokenbucket.WithClock(clock.RealClock{}))
	defer limiter.Close()

	p := pipeline.New().
		RateLimit(limiter, pipeline.KeyByValue("concurrent")).
		Bulkhead(100, 10*time.Millisecond).
		Timeout(100 * time.Millisecond).
		Build()

	done := make(chan struct{}, 100)
	for i := 0; i < 100; i++ {
		go func() {
			p.Execute(context.Background(), succeed) //nolint:errcheck
			done <- struct{}{}
		}()
	}
	for i := 0; i < 100; i++ {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("goroutine did not complete")
		}
	}
}
