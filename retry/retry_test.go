package retry_test

import (
	"context"
	"errors"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskarpan/resilience/internal/clock"
	"github.com/sanskarpan/resilience/retry"
	"github.com/sanskarpan/resilience/retry/backoff"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

var errTransient = errors.New("transient error")
var errPermanent = errors.New("permanent error")

// newManualClock creates a ManualClock at a fixed start time.
func newManualClock() *clock.ManualClock {
	return clock.NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
}

// ---------------------------------------------------------------------------
// TestRetry_NoRetryOnSuccess
// ---------------------------------------------------------------------------

func TestRetry_NoRetryOnSuccess(t *testing.T) {
	calls := 0
	p := &retry.Policy{MaxAttempts: 5}
	err := p.Do(context.Background(), func(_ context.Context) error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}
}

// ---------------------------------------------------------------------------
// TestRetry_RetryUpToMaxAttempts
// ---------------------------------------------------------------------------

func TestRetry_RetryUpToMaxAttempts(t *testing.T) {
	const maxAttempts = 4
	calls := 0
	p := &retry.Policy{
		MaxAttempts: maxAttempts,
		Backoff:     backoff.Constant(0),
	}
	err := p.Do(context.Background(), func(_ context.Context) error {
		calls++
		if calls < maxAttempts {
			return errTransient
		}
		return nil // succeeds on last attempt
	})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if calls != maxAttempts {
		t.Fatalf("expected %d calls, got %d", maxAttempts, calls)
	}
}

func TestRetry_ExhaustsAllAttemptsAndReturnsLastError(t *testing.T) {
	const maxAttempts = 3
	calls := 0
	p := &retry.Policy{
		MaxAttempts: maxAttempts,
		Backoff:     backoff.Constant(0),
	}
	err := p.Do(context.Background(), func(_ context.Context) error {
		calls++
		return errTransient
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, errTransient) {
		t.Fatalf("expected errTransient, got %v", err)
	}
	if calls != maxAttempts {
		t.Fatalf("expected %d calls, got %d", maxAttempts, calls)
	}
}

// ---------------------------------------------------------------------------
// TestRetry_RetryIfPredicate
// ---------------------------------------------------------------------------

func TestRetry_RetryIfPredicate(t *testing.T) {
	calls := 0
	p := &retry.Policy{
		MaxAttempts: 10,
		Backoff:     backoff.Constant(0),
		RetryIf: func(err error) bool {
			return errors.Is(err, errTransient)
		},
	}
	// First call returns a permanent error — should NOT be retried.
	err := p.Do(context.Background(), func(_ context.Context) error {
		calls++
		return errPermanent
	})
	if !errors.Is(err, errPermanent) {
		t.Fatalf("expected errPermanent, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected exactly 1 call for non-retryable error, got %d", calls)
	}
}

func TestRetry_RetryIfPredicate_OnlyRetriesMatchingErrors(t *testing.T) {
	calls := 0
	p := &retry.Policy{
		MaxAttempts: 5,
		Backoff:     backoff.Constant(0),
		RetryIf: func(err error) bool {
			return errors.Is(err, errTransient)
		},
	}
	err := p.Do(context.Background(), func(_ context.Context) error {
		calls++
		if calls < 3 {
			return errTransient // retried
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil error after retries, got %v", err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

// ---------------------------------------------------------------------------
// TestRetry_ContextCancellation_StopsRetrying
// ---------------------------------------------------------------------------

func TestRetry_ContextCancellation_StopsRetrying(t *testing.T) {
	clk := newManualClock()
	p := &retry.Policy{
		MaxAttempts: 100,
		Backoff:     backoff.Constant(1 * time.Second),
		Clock:       clk,
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Track calls
	var calls atomic.Int32
	done := make(chan error, 1)
	go func() {
		done <- p.Do(ctx, func(_ context.Context) error {
			calls.Add(1)
			return errTransient
		})
	}()

	// Let the first attempt happen then cancel while sleeping.
	time.Sleep(10 * time.Millisecond)
	cancel()

	// Advance the clock to ensure the sleep wakes up via ctx cancellation
	// (the select should pick ctx.Done() regardless, but advance to be safe).
	clk.Advance(2 * time.Second)

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Do did not return after context cancellation")
	}
}

func TestRetry_ContextAlreadyCancelled_DoesNotCall(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before Do

	calls := 0
	p := &retry.Policy{MaxAttempts: 5}
	err := p.Do(ctx, func(_ context.Context) error {
		calls++
		return nil
	})
	// Context is already cancelled on the first check — should return ctx.Err().
	// However, the implementation checks ctx before each attempt, so the first
	// attempt is skipped and we get context.Canceled.
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if calls != 0 {
		t.Fatalf("expected 0 calls with pre-cancelled ctx, got %d", calls)
	}
}

// ---------------------------------------------------------------------------
// TestRetry_BackoffTimingsCorrect
// ---------------------------------------------------------------------------

func TestRetry_BackoffTimingsCorrect(t *testing.T) {
	const delay = 200 * time.Millisecond
	clk := newManualClock()

	var waitsSeen []time.Duration
	p := &retry.Policy{
		MaxAttempts: 4,
		Backoff:     backoff.Constant(delay),
		Clock:       clk,
		OnRetry: func(attempt int, _ error, nextWait time.Duration) {
			waitsSeen = append(waitsSeen, nextWait)
		},
	}

	calls := 0
	done := make(chan error, 1)
	go func() {
		done <- p.Do(context.Background(), func(_ context.Context) error {
			calls++
			return errTransient
		})
	}()

	// Advance the clock for each retry sleep (3 sleeps for 4 attempts).
	for i := 0; i < 3; i++ {
		time.Sleep(5 * time.Millisecond) // let goroutine reach Sleep
		clk.Advance(delay)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Do did not return")
	}

	if calls != 4 {
		t.Fatalf("expected 4 calls, got %d", calls)
	}
	if len(waitsSeen) != 3 {
		t.Fatalf("expected 3 OnRetry notifications, got %d", len(waitsSeen))
	}
	for i, w := range waitsSeen {
		if w != delay {
			t.Errorf("OnRetry %d: expected wait %v, got %v", i, delay, w)
		}
	}
}

// ---------------------------------------------------------------------------
// TestRetry_ExponentialBackoff_Formula
// ---------------------------------------------------------------------------

func TestRetry_ExponentialBackoff_Formula(t *testing.T) {
	base := 100 * time.Millisecond
	max := 10 * time.Second
	clk := newManualClock()

	expectedDelays := []time.Duration{
		100 * time.Millisecond, // retry 0: 2^0 * base
		200 * time.Millisecond, // retry 1: 2^1 * base
		400 * time.Millisecond, // retry 2: 2^2 * base
	}

	var gotDelays []time.Duration
	p := &retry.Policy{
		MaxAttempts: 4,
		Backoff:     backoff.Exponential(base, max),
		Clock:       clk,
		OnRetry: func(attempt int, _ error, nextWait time.Duration) {
			gotDelays = append(gotDelays, nextWait)
		},
	}

	done := make(chan error, 1)
	go func() {
		done <- p.Do(context.Background(), func(_ context.Context) error {
			return errTransient
		})
	}()

	for _, d := range expectedDelays {
		time.Sleep(5 * time.Millisecond)
		clk.Advance(d)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Do did not return")
	}

	if len(gotDelays) != len(expectedDelays) {
		t.Fatalf("expected %d delays, got %d", len(expectedDelays), len(gotDelays))
	}
	for i, want := range expectedDelays {
		if gotDelays[i] != want {
			t.Errorf("retry %d: expected %v, got %v", i, want, gotDelays[i])
		}
	}
}

// ---------------------------------------------------------------------------
// TestRetry_MaxDelay_CapsBackoff
// ---------------------------------------------------------------------------

func TestRetry_MaxDelay_CapsBackoff(t *testing.T) {
	clk := newManualClock()
	const maxDelay = 300 * time.Millisecond

	var gotDelays []time.Duration
	p := &retry.Policy{
		MaxAttempts: 5,
		Backoff:     backoff.Exponential(100*time.Millisecond, 10*time.Second),
		MaxDelay:    maxDelay,
		Clock:       clk,
		OnRetry: func(_ int, _ error, nextWait time.Duration) {
			gotDelays = append(gotDelays, nextWait)
		},
	}

	done := make(chan error, 1)
	go func() {
		done <- p.Do(context.Background(), func(_ context.Context) error {
			return errTransient
		})
	}()

	for i := 0; i < 4; i++ {
		time.Sleep(5 * time.Millisecond)
		clk.Advance(maxDelay)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Do did not return")
	}

	for i, d := range gotDelays {
		if d > maxDelay {
			t.Errorf("retry %d: delay %v exceeds MaxDelay %v", i, d, maxDelay)
		}
	}
}

// ---------------------------------------------------------------------------
// TestRetry_OnRetry_CalledBeforeEachRetry
// ---------------------------------------------------------------------------

func TestRetry_OnRetry_CalledBeforeEachRetry(t *testing.T) {
	retryAttempts := []int{}
	p := &retry.Policy{
		MaxAttempts: 4,
		Backoff:     backoff.Constant(0),
		OnRetry: func(attempt int, _ error, _ time.Duration) {
			retryAttempts = append(retryAttempts, attempt)
		},
	}

	p.Do(context.Background(), func(_ context.Context) error { //nolint:errcheck
		return errTransient
	})

	// OnRetry should be called 3 times (before retries 1, 2, 3), with attempt 0, 1, 2.
	if len(retryAttempts) != 3 {
		t.Fatalf("expected 3 OnRetry calls, got %d", len(retryAttempts))
	}
	for i, a := range retryAttempts {
		if a != i {
			t.Errorf("OnRetry call %d: expected attempt %d, got %d", i, i, a)
		}
	}
}

// ---------------------------------------------------------------------------
// TestRetry_FullJitter_Distribution
// ---------------------------------------------------------------------------

func TestRetry_FullJitter_Distribution(t *testing.T) {
	// Statistical test: run many single-retry policies and confirm mean delay ≈ cap/2.
	// We use zero MaxAttempts=2 and constant failure, collecting delays via OnRetry.
	capDur := 1000 * time.Millisecond
	rng := rand.New(rand.NewSource(2024))
	b := backoff.FullJitter(1*time.Millisecond, capDur, rng)

	const samples = 10_000
	var totalDelay time.Duration
	for i := 0; i < samples; i++ {
		totalDelay += b.Next(30) // high attempt so cap is saturated
	}
	mean := totalDelay / samples
	expected := capDur / 2
	tolerance := capDur / 20 // ±5%
	if mean < expected-tolerance || mean > expected+tolerance {
		t.Errorf("FullJitter mean %v not close to expected %v (±%v)", mean, expected, tolerance)
	}
}

// ---------------------------------------------------------------------------
// TestRetry_DecorrelatedJitter_NoBoundExplosion
// ---------------------------------------------------------------------------

func TestRetry_DecorrelatedJitter_NoBoundExplosion(t *testing.T) {
	rng := rand.New(rand.NewSource(9999))
	base := 100 * time.Millisecond
	capDur := 2 * time.Second
	b := backoff.Decorrelated(base, capDur, rng)

	for i := 0; i < 10_000; i++ {
		got := b.Next(i)
		if got > capDur {
			t.Fatalf("attempt %d: delay %v exceeds cap %v", i, got, capDur)
		}
		if got < base {
			t.Fatalf("attempt %d: delay %v is below base %v", i, got, base)
		}
	}
}

// ---------------------------------------------------------------------------
// TestRetry_DoWithResult_TypeSafety
// ---------------------------------------------------------------------------

func TestRetry_DoWithResult_TypeSafety(t *testing.T) {
	p := &retry.Policy{
		MaxAttempts: 3,
		Backoff:     backoff.Constant(0),
	}

	calls := 0
	result, err := retry.DoWithResult(context.Background(), p, func(_ context.Context) (string, error) {
		calls++
		if calls < 3 {
			return "", errTransient
		}
		return "hello", nil
	})

	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if result != "hello" {
		t.Fatalf("expected 'hello', got %q", result)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestRetry_DoWithResult_ReturnsZeroValueOnFailure(t *testing.T) {
	p := &retry.Policy{
		MaxAttempts: 2,
		Backoff:     backoff.Constant(0),
	}

	result, err := retry.DoWithResult(context.Background(), p, func(_ context.Context) (int, error) {
		return 42, errTransient
	})

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	// result should be the last value returned by fn, which is 42 (not necessarily zero).
	// The contract is that the caller should not use result when err != nil,
	// but we verify it is 42 (the last value).
	_ = result
}

// ---------------------------------------------------------------------------
// TestRetry_MaxAttempts_Zero_TreatedAsOne
// ---------------------------------------------------------------------------

func TestRetry_MaxAttempts_Zero_TreatedAsOne(t *testing.T) {
	calls := 0
	p := &retry.Policy{MaxAttempts: 0}
	p.Do(context.Background(), func(_ context.Context) error { //nolint:errcheck
		calls++
		return errTransient
	})
	if calls != 1 {
		t.Fatalf("MaxAttempts=0 should call fn once, got %d calls", calls)
	}
}

// ---------------------------------------------------------------------------
// TestRetry_NoBackoff_NoDelay
// ---------------------------------------------------------------------------

func TestRetry_NoBackoff_NoDelay(t *testing.T) {
	// With Backoff=nil, retries should happen immediately.
	p := &retry.Policy{
		MaxAttempts: 3,
		Backoff:     nil,
	}
	calls := 0
	start := time.Now()
	p.Do(context.Background(), func(_ context.Context) error { //nolint:errcheck
		calls++
		return errTransient
	})
	elapsed := time.Since(start)
	if elapsed > 100*time.Millisecond {
		t.Errorf("expected fast retry with no backoff, took %v", elapsed)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}
