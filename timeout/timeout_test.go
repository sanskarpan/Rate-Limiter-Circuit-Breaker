package timeout_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sanskarpan/resilience/timeout"
)

func TestTimeout_CompletesWithinDeadline(t *testing.T) {
	err := timeout.Do(context.Background(), 100*time.Millisecond, func(ctx context.Context) error {
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestTimeout_ExceedsDeadline(t *testing.T) {
	err := timeout.Do(context.Background(), 10*time.Millisecond, func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
			return nil
		}
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
}

func TestTimeout_PropagatesError(t *testing.T) {
	sentinel := errors.New("sentinel")
	err := timeout.Do(context.Background(), 100*time.Millisecond, func(ctx context.Context) error {
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error, got %v", err)
	}
}

func TestTimeout_ZeroDuration_NoTimeout(t *testing.T) {
	// With d=0, no timeout is applied — fn runs with original context.
	err := timeout.Do(context.Background(), 0, func(ctx context.Context) error {
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestTimeout_RespectsParentCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	err := timeout.Do(ctx, 1*time.Second, func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
			return nil
		}
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected Canceled, got %v", err)
	}
}

func TestTimeout_Struct_Do(t *testing.T) {
	to := timeout.New(50 * time.Millisecond)
	if to.Duration() != 50*time.Millisecond {
		t.Fatalf("unexpected duration: %v", to.Duration())
	}

	err := to.Do(context.Background(), func(ctx context.Context) error {
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should timeout
	err = to.Do(context.Background(), func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
			return nil
		}
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected timeout, got %v", err)
	}
}

func TestTimeout_DoWithResult(t *testing.T) {
	result, err := timeout.DoWithResult(context.Background(), 100*time.Millisecond,
		func(ctx context.Context) (int, error) {
			return 42, nil
		})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != 42 {
		t.Fatalf("expected 42, got %d", result)
	}
}

func TestTimeout_DoWithResult_Timeout(t *testing.T) {
	_, err := timeout.DoWithResult(context.Background(), 10*time.Millisecond,
		func(ctx context.Context) (string, error) {
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(1 * time.Second):
				return "ok", nil
			}
		})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
}

func TestTimeout_ConcurrentSafe(t *testing.T) {
	to := timeout.New(100 * time.Millisecond)
	done := make(chan struct{})

	for i := 0; i < 100; i++ {
		go func() {
			to.Do(context.Background(), func(ctx context.Context) error { //nolint:errcheck
				return nil
			})
			done <- struct{}{}
		}()
	}
	for i := 0; i < 100; i++ {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("goroutine did not complete")
		}
	}
}

// ---------------------------------------------------------------------------
// Regression: M-10 — Do must enforce the deadline against an uncooperative fn
// that ignores ctx, returning ~at the deadline with a *timeout.TimeoutError.
// ---------------------------------------------------------------------------

func TestTimeout_EnforcesDeadlineAgainstUncooperativeFn(t *testing.T) {
	start := time.Now()
	err := timeout.Do(context.Background(), 10*time.Millisecond, func(ctx context.Context) error {
		time.Sleep(1 * time.Second) // ignores ctx entirely
		return nil
	})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected a timeout error, got nil")
	}
	// errors.As must find the typed *TimeoutError.
	var te *timeout.TimeoutError
	if !errors.As(err, &te) {
		t.Fatalf("expected *timeout.TimeoutError, got %T: %v", err, err)
	}
	// errors.Is must still see the wrapped sentinel.
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected error to wrap context.DeadlineExceeded, got %v", err)
	}
	// Must return roughly at the deadline, well before fn's 1s sleep.
	if elapsed > 500*time.Millisecond {
		t.Fatalf("Do did not enforce deadline: returned after %v", elapsed)
	}
}

func TestTimeout_DoWithResult_EnforcesDeadlineAgainstUncooperativeFn(t *testing.T) {
	start := time.Now()
	val, err := timeout.DoWithResult(context.Background(), 10*time.Millisecond,
		func(ctx context.Context) (int, error) {
			time.Sleep(1 * time.Second)
			return 99, nil
		})
	elapsed := time.Since(start)

	var te *timeout.TimeoutError
	if !errors.As(err, &te) {
		t.Fatalf("expected *timeout.TimeoutError, got %T: %v", err, err)
	}
	if val != 0 {
		t.Fatalf("expected zero value on timeout, got %d", val)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("DoWithResult did not enforce deadline: returned after %v", elapsed)
	}
}
