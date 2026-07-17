package fallback_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/fallback"
)

var errPrimary = errors.New("primary error")
var errFallback = errors.New("fallback error")

// ---------------------------------------------------------------------------
// Fallback (Do)
// ---------------------------------------------------------------------------

func TestFallback_Do_SuccessSkipsFallback(t *testing.T) {
	fbCalled := false
	err := fallback.Do(context.Background(),
		func(ctx context.Context) error { return nil },
		func(ctx context.Context, origErr error) error {
			fbCalled = true
			return nil
		},
	)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if fbCalled {
		t.Fatal("fallback should not have been called on success")
	}
}

func TestFallback_Do_CallsFallbackOnError(t *testing.T) {
	var capturedErr error
	err := fallback.Do(context.Background(),
		func(ctx context.Context) error { return errPrimary },
		func(ctx context.Context, origErr error) error {
			capturedErr = origErr
			return nil
		},
	)
	if err != nil {
		t.Fatalf("expected fallback success, got %v", err)
	}
	if !errors.Is(capturedErr, errPrimary) {
		t.Fatalf("expected errPrimary, got %v", capturedErr)
	}
}

func TestFallback_Do_FallbackErrorPropagates(t *testing.T) {
	err := fallback.Do(context.Background(),
		func(ctx context.Context) error { return errPrimary },
		func(ctx context.Context, origErr error) error { return errFallback },
	)
	if !errors.Is(err, errFallback) {
		t.Fatalf("expected errFallback, got %v", err)
	}
}

func TestFallback_DoWithResult_Success(t *testing.T) {
	result, err := fallback.DoWithResult(
		context.Background(),
		func(ctx context.Context) (int, error) { return 42, nil },
		func(ctx context.Context, origErr error) (int, error) { return 0, origErr },
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != 42 {
		t.Fatalf("expected 42, got %d", result)
	}
}

func TestFallback_DoWithResult_Fallback(t *testing.T) {
	result, err := fallback.DoWithResult(
		context.Background(),
		func(ctx context.Context) (int, error) { return 0, errPrimary },
		func(ctx context.Context, origErr error) (int, error) { return 99, nil },
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != 99 {
		t.Fatalf("expected 99 from fallback, got %d", result)
	}
}

func TestFallback_Struct_Execute(t *testing.T) {
	fb := fallback.New(func(ctx context.Context, origErr error) error {
		return nil // swallow
	})

	err := fb.Execute(context.Background(), func(ctx context.Context) error {
		return errPrimary
	})
	if err != nil {
		t.Fatalf("expected fallback success, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Hedge
// ---------------------------------------------------------------------------

func TestHedge_PrimaryCompletesBeforeDelay(t *testing.T) {
	var calls int32
	result := fallback.Hedge(
		context.Background(),
		100*time.Millisecond,
		func(ctx context.Context) error {
			atomic.AddInt32(&calls, 1)
			return nil
		},
	)
	if result.Err != nil {
		t.Fatalf("expected success, got %v", result.Err)
	}
	if !result.Primary {
		t.Fatal("expected primary to win")
	}
	// Only primary should have been called
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}
}

func TestHedge_BackupWinsWhenPrimaryIsSlow(t *testing.T) {
	var calls int32

	// Primary is slow; backup completes immediately
	result := fallback.Hedge(
		context.Background(),
		10*time.Millisecond,
		func(ctx context.Context) error {
			n := atomic.AddInt32(&calls, 1)
			if n == 1 {
				// Primary: block until cancelled
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(5 * time.Second):
					return nil
				}
			}
			// Backup: complete immediately
			return nil
		},
	)

	if result.Primary {
		t.Error("expected backup to win")
	}
	if result.Err != nil {
		t.Fatalf("expected success, got %v", result.Err)
	}

	// Allow time for goroutines to settle
	time.Sleep(20 * time.Millisecond)
	if atomic.LoadInt32(&calls) < 2 {
		t.Error("expected at least 2 calls (primary + backup)")
	}
}

func TestHedge_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result := fallback.Hedge(ctx, 100*time.Millisecond, func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
			return nil
		}
	})

	if !errors.Is(result.Err, context.Canceled) {
		t.Fatalf("expected Canceled, got %v", result.Err)
	}
}

func TestHedge_BothFail_ReturnsLastError(t *testing.T) {
	var calls int32
	result := fallback.Hedge(
		context.Background(),
		5*time.Millisecond,
		func(ctx context.Context) error {
			atomic.AddInt32(&calls, 1)
			return errPrimary
		},
	)

	// Both primary and backup should fail
	if result.Err == nil {
		t.Fatal("expected error")
	}
}

func TestHedge_PrimaryWins_BackupCancelled(t *testing.T) {
	// Primary returns quickly with success; verify backup is cancelled
	backupStarted := make(chan struct{}, 1)
	backupCancelled := make(chan struct{}, 1)
	var calls int32

	result := fallback.Hedge(
		context.Background(),
		5*time.Millisecond,
		func(ctx context.Context) error {
			n := atomic.AddInt32(&calls, 1)
			if n == 1 {
				// Primary: delay slightly but succeed
				time.Sleep(20 * time.Millisecond)
				return nil
			}
			// Backup
			backupStarted <- struct{}{}
			<-ctx.Done()
			backupCancelled <- struct{}{}
			return ctx.Err()
		},
	)

	if result.Err != nil {
		t.Fatalf("expected success, got %v", result.Err)
	}

	// If backup started, it should have been cancelled
	select {
	case <-backupStarted:
		select {
		case <-backupCancelled:
			// good: backup was cancelled
		case <-time.After(500 * time.Millisecond):
			t.Error("backup was not cancelled")
		}
	default:
		// backup never started — also fine if primary was fast enough
	}
}

func TestHedge_NoConcurrentRequests_WhenPrimaryFast(t *testing.T) {
	var calls int32

	start := time.Now()
	result := fallback.Hedge(
		context.Background(),
		500*time.Millisecond, // long hedge delay
		func(ctx context.Context) error {
			atomic.AddInt32(&calls, 1)
			return nil // immediate success
		},
	)

	elapsed := time.Since(start)
	if result.Err != nil {
		t.Fatalf("unexpected error: %v", result.Err)
	}
	if elapsed > 100*time.Millisecond {
		t.Errorf("hedge should not slow down fast requests: took %v", elapsed)
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Errorf("expected exactly 1 call, got %d", calls)
	}
}

func TestHedge_Concurrent_NoRace(t *testing.T) {
	var total int32
	const goroutines = 50

	done := make(chan struct{}, goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			fallback.Hedge( //nolint:errcheck
				context.Background(),
				5*time.Millisecond,
				func(ctx context.Context) error {
					atomic.AddInt32(&total, 1)
					time.Sleep(2 * time.Millisecond)
					return nil
				},
			)
			done <- struct{}{}
		}()
	}
	for i := 0; i < goroutines; i++ {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("goroutine did not complete")
		}
	}
}

func TestHedgeCond_SkipsHedgeWhenShouldHedgeFalse(t *testing.T) {
	var calls int32
	result := fallback.HedgeCond(
		context.Background(),
		100*time.Millisecond,
		func(ctx context.Context) error {
			atomic.AddInt32(&calls, 1)
			return nil
		},
		func(ctx context.Context) bool { return false },
	)

	if result.Err != nil {
		t.Fatalf("expected success, got %v", result.Err)
	}
	if !result.Primary {
		t.Fatal("should be marked as primary")
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("expected 1 call (no hedging), got %d", calls)
	}
}

func TestHedgeCond_HedgesWhenShouldHedgeTrue(t *testing.T) {
	var calls int32
	result := fallback.HedgeCond(
		context.Background(),
		5*time.Millisecond,
		func(ctx context.Context) error {
			atomic.AddInt32(&calls, 1)
			return nil
		},
		func(ctx context.Context) bool { return true },
	)

	if result.Err != nil {
		t.Fatalf("expected success, got %v", result.Err)
	}
}

func TestHedgeN_SingleAttempt(t *testing.T) {
	var calls int32
	err := fallback.HedgeN(context.Background(), 10*time.Millisecond, 1, func(ctx context.Context) error {
		atomic.AddInt32(&calls, 1)
		return nil
	})

	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}
}

func TestHedgeN_MultipleAttempts(t *testing.T) {
	var calls int32
	err := fallback.HedgeN(context.Background(), 5*time.Millisecond, 3, func(ctx context.Context) error {
		atomic.AddInt32(&calls, 1)
		// All attempts fail
		return errPrimary
	})

	// All attempts fail, so we get the last error
	if !errors.Is(err, errPrimary) {
		t.Fatalf("expected errPrimary, got %v", err)
	}
}

func TestHedgeN_AllFail(t *testing.T) {
	err := fallback.HedgeN(context.Background(), 5*time.Millisecond, 3, func(ctx context.Context) error {
		return errPrimary
	})

	if !errors.Is(err, errPrimary) {
		t.Fatalf("expected errPrimary, got %v", err)
	}
}

func TestHedgeN_ZeroN_CoercedToOne(t *testing.T) {
	var calls int32
	err := fallback.HedgeN(context.Background(), 10*time.Millisecond, 0, func(ctx context.Context) error {
		atomic.AddInt32(&calls, 1)
		return nil
	})

	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("expected 1 call (n=0 coerced to 1), got %d", calls)
	}
}

func TestHedgeN_NegativeN_CoercedToOne(t *testing.T) {
	var calls int32
	err := fallback.HedgeN(context.Background(), 10*time.Millisecond, -5, func(ctx context.Context) error {
		atomic.AddInt32(&calls, 1)
		return nil
	})

	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("expected 1 call (n=-5 coerced to 1), got %d", calls)
	}
}

// ---------------------------------------------------------------------------
// Regression: H-18 — HedgeN must fire its full budget even when attempts fail
// faster than hedgeDelay, and must return a later-succeeding attempt.
// ---------------------------------------------------------------------------

func TestHedgeN_FiresFullBudgetOnFastFailures(t *testing.T) {
	var calls int32
	const n = 4

	fn := func(ctx context.Context) error {
		idx := atomic.AddInt32(&calls, 1)
		if idx < n {
			// First few attempts fail fast (faster than hedgeDelay).
			time.Sleep(1 * time.Millisecond)
			return errPrimary
		}
		// The nth attempt succeeds after a short delay.
		time.Sleep(5 * time.Millisecond)
		return nil
	}

	err := fallback.HedgeN(context.Background(), 5*time.Millisecond, n, fn)
	if err != nil {
		t.Fatalf("expected success once all attempts fire, got %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != n {
		t.Fatalf("expected all %d attempts to fire, got %d", n, got)
	}
}

// TestHedgeN_AllFailReturnsLastError verifies that when every attempt fails,
// HedgeN still fires the whole budget of n before returning the final error.
func TestHedgeN_AllFailReturnsLastError(t *testing.T) {
	var calls int32
	const n = 3
	fn := func(ctx context.Context) error {
		atomic.AddInt32(&calls, 1)
		time.Sleep(1 * time.Millisecond)
		return errPrimary
	}
	err := fallback.HedgeN(context.Background(), 5*time.Millisecond, n, fn)
	if !errors.Is(err, errPrimary) {
		t.Fatalf("expected errPrimary, got %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != n {
		t.Fatalf("expected all %d attempts to fire before giving up, got %d", n, got)
	}
}

// ---------------------------------------------------------------------------
// Regression: M-11 — hedgeDelay<=0 must not double-fire.
// ---------------------------------------------------------------------------

func TestHedge_NonPositiveDelayFiresOnce(t *testing.T) {
	var calls int32
	fn := func(ctx context.Context) error {
		atomic.AddInt32(&calls, 1)
		time.Sleep(30 * time.Millisecond)
		return nil
	}
	res := fallback.Hedge(context.Background(), 0, fn)
	if res.Err != nil {
		t.Fatalf("unexpected error: %v", res.Err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("expected exactly 1 invocation with hedgeDelay=0, got %d", got)
	}
}

func TestHedgeN_NonPositiveDelayFiresOnce(t *testing.T) {
	var calls int32
	fn := func(ctx context.Context) error {
		atomic.AddInt32(&calls, 1)
		time.Sleep(30 * time.Millisecond)
		return nil
	}
	if err := fallback.HedgeN(context.Background(), 0, 4, fn); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("expected exactly 1 invocation with hedgeDelay=0, got %d", got)
	}
}
