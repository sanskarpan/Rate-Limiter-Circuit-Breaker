package resilience_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/bulkhead"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/pipeline"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/resilience"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/retry"
)

// ---------------------------------------------------------------------------
// Deterministic fakes
// ---------------------------------------------------------------------------

// fakeLimiter is a deterministic ratelimit.Limiter whose Allow decision is
// driven by an atomic toggle, so tests can force allow/deny with no timing.
type fakeLimiter struct {
	allow atomic.Bool
	calls atomic.Int64
}

func newFakeLimiter(allow bool) *fakeLimiter {
	l := &fakeLimiter{}
	l.allow.Store(allow)
	return l
}

func (l *fakeLimiter) Allow(ctx context.Context, key string) ratelimit.Result {
	l.calls.Add(1)
	return ratelimit.Result{Allowed: l.allow.Load(), Algorithm: "fake"}
}
func (l *fakeLimiter) AllowN(ctx context.Context, key string, n int) ratelimit.Result {
	return l.Allow(ctx, key)
}
func (l *fakeLimiter) Wait(ctx context.Context, key string) error         { return nil }
func (l *fakeLimiter) WaitN(ctx context.Context, key string, n int) error { return nil }
func (l *fakeLimiter) Peek(ctx context.Context, key string) ratelimit.State {
	return ratelimit.State{Algorithm: "fake"}
}
func (l *fakeLimiter) Reset(ctx context.Context, key string) error { return nil }
func (l *fakeLimiter) Close() error                                { return nil }

var errBoom = errors.New("boom")

// ---------------------------------------------------------------------------
// Per-layer behaviour
// ---------------------------------------------------------------------------

func TestStack_EmptyPassesThrough(t *testing.T) {
	t.Parallel()
	stack := resilience.New().Build()
	var ran bool
	err := stack.Execute(context.Background(), func(ctx context.Context) error {
		ran = true
		return nil
	})
	if err != nil || !ran {
		t.Fatalf("empty stack: ran=%v err=%v", ran, err)
	}
}

func TestStack_Layers(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		build func() *resilience.Stack
		fn    func(ctx context.Context) error
		// wantIs, if non-nil, must match Execute's error via errors.Is.
		wantIs error
		// wantNil asserts a nil error (e.g. fallback recovered).
		wantNil bool
	}{
		{
			name: "rate limiter denies -> ErrRateLimited, operation never runs",
			build: func() *resilience.Stack {
				return resilience.New().
					WithRateLimit(newFakeLimiter(false), resilience.KeyByValue("k")).
					Build()
			},
			fn:     func(ctx context.Context) error { t.Fatal("operation must not run when limiter denies"); return nil },
			wantIs: pipeline.ErrRateLimited,
		},
		{
			name: "rate limiter allows -> operation runs",
			build: func() *resilience.Stack {
				return resilience.New().
					WithRateLimit(newFakeLimiter(true), resilience.KeyByValue("k")).
					Build()
			},
			fn:      func(ctx context.Context) error { return nil },
			wantNil: true,
		},
		{
			name: "circuit breaker open -> ErrCircuitOpen",
			build: func() *resilience.Stack {
				cb := circuitbreaker.New(circuitbreaker.Config{
					Name:             "cb",
					FailureThreshold: 1, // opens after a single failure
				})
				// Trip it before use.
				_ = cb.Execute(context.Background(), func(ctx context.Context) error { return errBoom })
				return resilience.New().WithCircuitBreaker(cb).Build()
			},
			fn:     func(ctx context.Context) error { t.Fatal("operation must not run when circuit open"); return nil },
			wantIs: circuitbreaker.ErrCircuitOpen,
		},
		{
			name: "timeout fires -> DeadlineExceeded",
			build: func() *resilience.Stack {
				return resilience.New().WithTimeout(10 * time.Millisecond).Build()
			},
			fn: func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			},
			wantIs: context.DeadlineExceeded,
		},
		{
			name: "fallback recovers any inner error -> nil",
			build: func() *resilience.Stack {
				return resilience.New().
					WithRateLimit(newFakeLimiter(false), resilience.KeyByValue("k")).
					WithFallback(func(ctx context.Context, err error) error {
						if !errors.Is(err, pipeline.ErrRateLimited) {
							t.Errorf("fallback got %v, want ErrRateLimited", err)
						}
						return nil
					}).
					Build()
			},
			fn:      func(ctx context.Context) error { return nil },
			wantNil: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			s := tt.build()
			err := s.Execute(context.Background(), tt.fn)
			switch {
			case tt.wantNil && err != nil:
				t.Fatalf("want nil err, got %v", err)
			case tt.wantIs != nil && !errors.Is(err, tt.wantIs):
				t.Fatalf("want errors.Is(err, %v), got %v", tt.wantIs, err)
			}
		})
	}
}

func TestStack_RetryRetriesThenSucceeds(t *testing.T) {
	t.Parallel()
	policy := retry.New(retry.WithMaxAttempts(3)) // no backoff -> no sleeping
	stack := resilience.New().WithRetry(policy).Build()

	var calls atomic.Int64
	err := stack.Execute(context.Background(), func(ctx context.Context) error {
		if calls.Add(1) < 3 {
			return errBoom
		}
		return nil
	})
	if err != nil {
		t.Fatalf("want success after retries, got %v", err)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("want 3 attempts, got %d", got)
	}
}

func TestStack_BreakerOpensAfterFailures(t *testing.T) {
	t.Parallel()
	cb := circuitbreaker.New(circuitbreaker.Config{Name: "cb", FailureThreshold: 3})
	stack := resilience.New().WithCircuitBreaker(cb).Build()

	// Drive 3 failures to open the breaker.
	for i := 0; i < 3; i++ {
		_ = stack.Execute(context.Background(), func(ctx context.Context) error { return errBoom })
	}
	if cb.State() != circuitbreaker.StateOpen {
		t.Fatalf("breaker should be open, got %s", cb.State())
	}
	// Now the operation must be short-circuited.
	var ran bool
	err := stack.Execute(context.Background(), func(ctx context.Context) error { ran = true; return nil })
	if ran {
		t.Fatal("operation ran despite open circuit")
	}
	if !errors.Is(err, circuitbreaker.ErrCircuitOpen) {
		t.Fatalf("want ErrCircuitOpen, got %v", err)
	}
}

func TestStack_BulkheadRejectsWhenFull(t *testing.T) {
	t.Parallel()
	bh := bulkhead.New(1, 0) // capacity 1, non-blocking
	stack := resilience.New().WithBulkhead(bh).Build()

	// Occupy the single slot with a long-running call, then attempt another.
	release := make(chan struct{})
	entered := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = stack.Execute(context.Background(), func(ctx context.Context) error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered // ensure the slot is held

	err := stack.Execute(context.Background(), func(ctx context.Context) error {
		t.Error("operation ran despite full bulkhead")
		return nil
	})
	if !errors.Is(err, bulkhead.ErrBulkheadFull) {
		t.Fatalf("want ErrBulkheadFull, got %v", err)
	}
	close(release)
	wg.Wait()
}

// ---------------------------------------------------------------------------
// Ordering
// ---------------------------------------------------------------------------

// TestOrdering_LimiterBeforeBulkheadBeforeBreakerBeforeRetry proves the
// documented wrapping order by observing side effects: a denying limiter must
// short-circuit before the bulkhead slot is ever acquired or the operation runs.
func TestOrdering_RateLimitOutermostOfCore(t *testing.T) {
	t.Parallel()
	lim := newFakeLimiter(false)
	bh := bulkhead.New(1, 0)
	stack := resilience.New().
		// Configure out of canonical order on purpose.
		WithBulkhead(bh).
		WithRateLimit(lim, resilience.KeyByValue("k")).
		Build()

	err := stack.Execute(context.Background(), func(ctx context.Context) error {
		t.Fatal("operation must not run")
		return nil
	})
	if !errors.Is(err, pipeline.ErrRateLimited) {
		t.Fatalf("want ErrRateLimited, got %v", err)
	}
	if lim.calls.Load() != 1 {
		t.Fatalf("limiter should be consulted once, got %d", lim.calls.Load())
	}
	if bh.Inflight() != 0 {
		t.Fatalf("bulkhead slot acquired despite limiter denial: inflight=%d", bh.Inflight())
	}
}

// TestOrdering_RetryInnermostOfBreaker proves retry runs INSIDE the circuit
// breaker (retry innermost, breaker outermost per the documented order): the
// breaker wraps the whole retry loop, so a single Execute that internally
// retries 3 times is observed by the breaker as exactly ONE request whose final
// outcome is success. If retry were outermost the breaker would instead observe
// 3 separate requests. The operation itself is still called 3 times.
func TestOrdering_RetryInnermostOfBreaker(t *testing.T) {
	t.Parallel()
	// Breaker with a high threshold so it never opens during the test.
	cb := circuitbreaker.New(circuitbreaker.Config{Name: "cb", FailureThreshold: 100})
	policy := retry.New(retry.WithMaxAttempts(3))
	stack := resilience.New().
		WithRetry(policy).
		WithCircuitBreaker(cb).
		Build()

	var calls atomic.Int64
	err := stack.Execute(context.Background(), func(ctx context.Context) error {
		if calls.Add(1) < 3 {
			return errBoom
		}
		return nil
	})
	if err != nil {
		t.Fatalf("want eventual success, got %v", err)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("operation should be retried 3 times, got %d", got)
	}
	// The breaker wraps the retry loop, so it records ONE request (the final
	// success), not one per attempt — proof that retry is innermost of the two.
	snap := cb.Snapshot()
	if snap.Requests != 1 {
		t.Fatalf("breaker should observe 1 request (retry innermost, inside CB), got %d", snap.Requests)
	}
}

// TestOrdering_CustomStageIsInnermost proves a WithCustom stage runs closest to
// the operation (inside retry), by recording the call order.
func TestOrdering_CustomInnermost(t *testing.T) {
	t.Parallel()
	var order []string
	var mu sync.Mutex
	record := func(s string) { mu.Lock(); order = append(order, s); mu.Unlock() }

	stack := resilience.New().
		WithRateLimit(newFakeLimiter(true), resilience.KeyByValue("k")).
		WithCustom(func(ctx context.Context, fn func(context.Context) error) error {
			record("custom-before")
			err := fn(ctx)
			record("custom-after")
			return err
		}).
		Build()

	err := stack.Execute(context.Background(), func(ctx context.Context) error {
		record("op")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"custom-before", "op", "custom-after"}
	if len(order) != len(want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order = %v, want %v", order, want)
		}
	}
}

func TestBuilder_Layers(t *testing.T) {
	t.Parallel()
	b := resilience.New().
		WithRetry(retry.New(retry.WithMaxAttempts(2))).
		WithTimeout(time.Second).
		WithCircuitBreaker(circuitbreaker.New(circuitbreaker.Config{Name: "x"})).
		WithBulkheadLimit(2, 0).
		WithRateLimit(newFakeLimiter(true), resilience.KeyByValue("k")).
		WithFallback(func(ctx context.Context, err error) error { return nil })
	got := b.Layers()
	want := []string{"fallback", "ratelimit", "bulkhead", "timeout", "circuitbreaker", "retry"}
	if len(got) != len(want) {
		t.Fatalf("Layers() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Layers() = %v, want %v", got, want)
		}
	}
}

// ---------------------------------------------------------------------------
// Generic Execute[T]
// ---------------------------------------------------------------------------

func TestExecuteGeneric(t *testing.T) {
	t.Parallel()
	cb := circuitbreaker.New(circuitbreaker.Config{Name: "cb", FailureThreshold: 1})
	stack := resilience.New().WithCircuitBreaker(cb).Build()

	// success returns the typed value.
	got, err := resilience.Execute(context.Background(), stack,
		func(ctx context.Context) (int, error) { return 7, nil })
	if err != nil || got != 7 {
		t.Fatalf("Execute happy path: got=%d err=%v", got, err)
	}

	// failure returns zero value + verbatim error.
	got, err = resilience.Execute(context.Background(), stack,
		func(ctx context.Context) (int, error) { return 99, errBoom })
	if !errors.Is(err, errBoom) || got != 0 {
		t.Fatalf("Execute failure: got=%d err=%v", got, err)
	}
}

func TestExecuteWithFallbackGeneric(t *testing.T) {
	t.Parallel()
	stack := resilience.New().
		WithRateLimit(newFakeLimiter(false), resilience.KeyByValue("k")).
		Build()

	got, err := resilience.ExecuteWithFallback(context.Background(), stack,
		func(ctx context.Context) (string, error) { return "primary", nil },
		func(ctx context.Context, cause error) (string, error) {
			if !errors.Is(cause, pipeline.ErrRateLimited) {
				t.Errorf("fallback cause = %v, want ErrRateLimited", cause)
			}
			return "fallback", nil
		})
	if err != nil || got != "fallback" {
		t.Fatalf("ExecuteWithFallback: got=%q err=%v", got, err)
	}

	// fallback error is propagated, value is zero.
	got, err = resilience.ExecuteWithFallback(context.Background(), stack,
		func(ctx context.Context) (string, error) { return "primary", nil },
		func(ctx context.Context, cause error) (string, error) { return "ignored", errBoom })
	if !errors.Is(err, errBoom) || got != "" {
		t.Fatalf("ExecuteWithFallback err path: got=%q err=%v", got, err)
	}
}

// ---------------------------------------------------------------------------
// Full stack + concurrency (race)
// ---------------------------------------------------------------------------

func TestStack_FullComposition(t *testing.T) {
	t.Parallel()
	cb := circuitbreaker.New(circuitbreaker.Config{Name: "full", FailureThreshold: 100})
	bh := bulkhead.New(4, 20*time.Millisecond)
	policy := retry.New(retry.WithMaxAttempts(2))
	stack := resilience.New().
		WithRateLimit(newFakeLimiter(true), resilience.KeyByValue("k")).
		WithBulkhead(bh).
		WithTimeout(time.Second).
		WithCircuitBreaker(cb).
		WithRetry(policy).
		WithFallback(func(ctx context.Context, err error) error { return nil }).
		Build()

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = stack.Execute(context.Background(), func(ctx context.Context) error {
				if i%2 == 0 {
					return errBoom // fallback recovers -> nil
				}
				return nil
			})
		}(i)
	}
	wg.Wait()
}

func TestStack_ConcurrentGeneric(t *testing.T) {
	t.Parallel()
	stack := resilience.New().
		WithBulkheadLimit(8, 10*time.Millisecond).
		Build()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v, err := resilience.Execute(context.Background(), stack,
				func(ctx context.Context) (int, error) { return i, nil })
			if err == nil && v != i {
				t.Errorf("got %d want %d", v, i)
			}
		}(i)
	}
	wg.Wait()
}
