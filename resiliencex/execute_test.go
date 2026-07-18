package resiliencex_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/bulkhead"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/resiliencex"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/retry"
)

// errBoom is a distinct sentinel used to prove that fn's own error is returned
// verbatim (checkable with errors.Is) through the generic wrappers.
var errBoom = errors.New("boom")

// newTrippedBreaker returns a count-based breaker that is already Open. The
// breaker trips after a single failure (FailureThreshold: 1) and stays open for
// a long OpenTimeout, so subsequent Execute calls deterministically reject with
// ErrCircuitOpen without any timing races.
func newTrippedBreaker(t *testing.T) *circuitbreaker.CircuitBreaker {
	t.Helper()
	cb := circuitbreaker.New(circuitbreaker.Config{
		Name:             "test",
		WindowType:       circuitbreaker.CountBased,
		WindowSize:       10,
		FailureThreshold: 1,
		OpenTimeout:      time.Hour,
	})
	// Trip the breaker: one failing call opens it.
	err := cb.Execute(context.Background(), func(context.Context) error { return errBoom })
	if err == nil {
		t.Fatalf("expected trip call to return an error")
	}
	if got := cb.State(); got != circuitbreaker.StateOpen {
		t.Fatalf("expected breaker Open after trip, got %v", got)
	}
	return cb
}

func TestExecuteCB(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		breaker   func(t *testing.T) *circuitbreaker.CircuitBreaker
		fn        func(context.Context) (int, error)
		wantValue int
		wantErr   error // matched with errors.Is; nil means expect no error
		fnCalled  bool  // whether fn is expected to run
	}{
		{
			name:      "success returns value",
			breaker:   func(*testing.T) *circuitbreaker.CircuitBreaker { return circuitbreaker.New(circuitbreaker.Config{Name: "ok"}) },
			fn:        func(context.Context) (int, error) { return 42, nil },
			wantValue: 42,
			wantErr:   nil,
			fnCalled:  true,
		},
		{
			name:      "fn error returned verbatim with zero value",
			breaker:   func(*testing.T) *circuitbreaker.CircuitBreaker { return circuitbreaker.New(circuitbreaker.Config{Name: "ok"}) },
			fn:        func(context.Context) (int, error) { return 7, errBoom },
			wantValue: 0,
			wantErr:   errBoom,
			fnCalled:  true,
		},
		{
			name:      "open circuit returns ErrCircuitOpen and zero value",
			breaker:   newTrippedBreaker,
			fn:        func(context.Context) (int, error) { return 99, nil },
			wantValue: 0,
			wantErr:   circuitbreaker.ErrCircuitOpen,
			fnCalled:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cb := tc.breaker(t)
			var called bool
			got, err := resiliencex.ExecuteCB(context.Background(), cb, func(ctx context.Context) (int, error) {
				called = true
				return tc.fn(ctx)
			})
			if called != tc.fnCalled {
				t.Errorf("fn called = %v, want %v", called, tc.fnCalled)
			}
			if got != tc.wantValue {
				t.Errorf("value = %d, want %d", got, tc.wantValue)
			}
			if tc.wantErr == nil {
				if err != nil {
					t.Errorf("err = %v, want nil", err)
				}
			} else if !errors.Is(err, tc.wantErr) {
				t.Errorf("err = %v, want errors.Is %v", err, tc.wantErr)
			}
		})
	}
}

func TestExecuteCBWithFallback(t *testing.T) {
	t.Parallel()

	t.Run("primary success bypasses fallback", func(t *testing.T) {
		t.Parallel()
		cb := circuitbreaker.New(circuitbreaker.Config{Name: "ok"})
		var fallbackCalled bool
		got, err := resiliencex.ExecuteCBWithFallback(context.Background(), cb,
			func(context.Context) (string, error) { return "primary", nil },
			func(context.Context, error) (string, error) { fallbackCalled = true; return "fallback", nil },
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "primary" {
			t.Errorf("value = %q, want %q", got, "primary")
		}
		if fallbackCalled {
			t.Errorf("fallback should not have been called")
		}
	})

	t.Run("open circuit routes to fallback with sentinel", func(t *testing.T) {
		t.Parallel()
		cb := newTrippedBreaker(t)
		var seen error
		got, err := resiliencex.ExecuteCBWithFallback(context.Background(), cb,
			func(context.Context) (string, error) { return "primary", nil },
			func(_ context.Context, e error) (string, error) { seen = e; return "fallback", nil },
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "fallback" {
			t.Errorf("value = %q, want %q", got, "fallback")
		}
		if !errors.Is(seen, circuitbreaker.ErrCircuitOpen) {
			t.Errorf("fallback received %v, want errors.Is ErrCircuitOpen", seen)
		}
	})

	t.Run("fallback error yields zero value", func(t *testing.T) {
		t.Parallel()
		cb := circuitbreaker.New(circuitbreaker.Config{Name: "ok"})
		got, err := resiliencex.ExecuteCBWithFallback(context.Background(), cb,
			func(context.Context) (int, error) { return 5, errBoom },
			func(_ context.Context, e error) (int, error) { return 123, e },
		)
		if !errors.Is(err, errBoom) {
			t.Errorf("err = %v, want errors.Is errBoom", err)
		}
		if got != 0 {
			t.Errorf("value = %d, want 0 (zero value on error)", got)
		}
	})
}

func TestExecuteRetry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		policy     *retry.Policy
		fn         func(*int) func(context.Context) (int, error)
		wantValue  int
		wantErr    error
		wantCalls  int
	}{
		{
			name:   "success on first attempt",
			policy: &retry.Policy{MaxAttempts: 3},
			fn: func(calls *int) func(context.Context) (int, error) {
				return func(context.Context) (int, error) { *calls++; return 10, nil }
			},
			wantValue: 10,
			wantErr:   nil,
			wantCalls: 1,
		},
		{
			name:   "success after transient failures",
			policy: &retry.Policy{MaxAttempts: 3},
			fn: func(calls *int) func(context.Context) (int, error) {
				return func(context.Context) (int, error) {
					*calls++
					if *calls < 3 {
						return 0, errBoom
					}
					return 55, nil
				}
			},
			wantValue: 55,
			wantErr:   nil,
			wantCalls: 3,
		},
		{
			name:   "exhausted attempts return last error verbatim, zero value",
			policy: &retry.Policy{MaxAttempts: 2},
			fn: func(calls *int) func(context.Context) (int, error) {
				return func(context.Context) (int, error) { *calls++; return 3, errBoom }
			},
			wantValue: 0,
			wantErr:   errBoom,
			wantCalls: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var calls int
			got, err := resiliencex.ExecuteRetry(context.Background(), tc.policy, tc.fn(&calls))
			if calls != tc.wantCalls {
				t.Errorf("calls = %d, want %d", calls, tc.wantCalls)
			}
			if got != tc.wantValue {
				t.Errorf("value = %d, want %d", got, tc.wantValue)
			}
			if tc.wantErr == nil {
				if err != nil {
					t.Errorf("err = %v, want nil", err)
				}
			} else if !errors.Is(err, tc.wantErr) {
				t.Errorf("err = %v, want errors.Is %v", err, tc.wantErr)
			}
		})
	}
}

func TestExecuteBulkhead(t *testing.T) {
	t.Parallel()

	t.Run("success returns value", func(t *testing.T) {
		t.Parallel()
		b := bulkhead.New(2, 0)
		got, err := resiliencex.ExecuteBulkhead(context.Background(), b, func(context.Context) (string, error) {
			return "ok", nil
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "ok" {
			t.Errorf("value = %q, want %q", got, "ok")
		}
	})

	t.Run("fn error returned verbatim with zero value", func(t *testing.T) {
		t.Parallel()
		b := bulkhead.New(1, 0)
		got, err := resiliencex.ExecuteBulkhead(context.Background(), b, func(context.Context) (int, error) {
			return 8, errBoom
		})
		if !errors.Is(err, errBoom) {
			t.Errorf("err = %v, want errors.Is errBoom", err)
		}
		if got != 0 {
			t.Errorf("value = %d, want 0 (zero value on error)", got)
		}
	})

	t.Run("full bulkhead rejects with ErrBulkheadFull and zero value", func(t *testing.T) {
		t.Parallel()
		// Non-blocking bulkhead of size 1. Hold the single slot from another
		// goroutine, then a second call must reject immediately.
		b := bulkhead.New(1, 0)
		release := make(chan struct{})
		holding := make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			_, _ = resiliencex.ExecuteBulkhead(context.Background(), b, func(context.Context) (int, error) {
				close(holding)
				<-release
				return 1, nil
			})
		}()
		<-holding // slot is now held

		got, err := resiliencex.ExecuteBulkhead(context.Background(), b, func(context.Context) (int, error) {
			return 2, nil
		})
		if !errors.Is(err, bulkhead.ErrBulkheadFull) {
			t.Errorf("err = %v, want errors.Is ErrBulkheadFull", err)
		}
		if got != 0 {
			t.Errorf("value = %d, want 0 (zero value on error)", got)
		}
		close(release)
		<-done
	})
}

// TestConcurrent exercises every wrapper from many goroutines at once so the
// race detector can flag any data race the adapters might introduce. The
// wrappers hold no shared state of their own, but result-capturing closures are
// a classic race hazard, so this guards against a regression there.
func TestConcurrent(t *testing.T) {
	t.Parallel()

	cb := circuitbreaker.New(circuitbreaker.Config{Name: "concurrent"})
	p := &retry.Policy{MaxAttempts: 2}
	b := bulkhead.New(8, 50*time.Millisecond)

	const goroutines = 64
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		i := i
		go func() {
			defer wg.Done()
			ctx := context.Background()

			if v, err := resiliencex.ExecuteCB(ctx, cb, func(context.Context) (int, error) { return i, nil }); err != nil || v != i {
				t.Errorf("ExecuteCB: got (%d, %v), want (%d, nil)", v, err, i)
			}
			if v, err := resiliencex.ExecuteRetry(ctx, p, func(context.Context) (int, error) { return i, nil }); err != nil || v != i {
				t.Errorf("ExecuteRetry: got (%d, %v), want (%d, nil)", v, err, i)
			}
			if v, err := resiliencex.ExecuteBulkhead(ctx, b, func(context.Context) (int, error) { return i, nil }); err != nil || v != i {
				t.Errorf("ExecuteBulkhead: got (%d, %v), want (%d, nil)", v, err, i)
			}
			if v, err := resiliencex.ExecuteCBWithFallback(ctx, cb,
				func(context.Context) (int, error) { return i, nil },
				func(context.Context, error) (int, error) { return -1, nil },
			); err != nil || v != i {
				t.Errorf("ExecuteCBWithFallback: got (%d, %v), want (%d, nil)", v, err, i)
			}
		}()
	}
	wg.Wait()
}
