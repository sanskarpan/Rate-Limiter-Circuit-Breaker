package circuitbreaker_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/testutil"
)

var errTest = errors.New("test error")

func newClock() *clock.ManualClock {
	return clock.NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
}

func countBasedCB(t *testing.T, windowSize, failureThreshold int, opts ...func(*circuitbreaker.Config)) *circuitbreaker.CircuitBreaker {
	t.Helper()
	clk := newClock()
	cfg := circuitbreaker.Config{
		Name:             t.Name(),
		WindowType:       circuitbreaker.CountBased,
		WindowSize:       windowSize,
		FailureThreshold: failureThreshold,
		OpenTimeout:      time.Second,
		Clock:            clk,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return circuitbreaker.New(cfg)
}

func succeed(ctx context.Context) error { return nil }
func fail(ctx context.Context) error    { return errTest }

// TestCB_InitialState_Closed verifies new circuit breakers start closed.
func TestCB_InitialState_Closed(t *testing.T) {
	cb := countBasedCB(t, 10, 5)
	if cb.State() != circuitbreaker.StateClosed {
		t.Fatalf("expected Closed, got %s", cb.State())
	}
}

// TestCB_OpenAfterFailureThreshold_CountBased verifies count-based opening.
func TestCB_OpenAfterFailureThreshold_CountBased(t *testing.T) {
	cb := countBasedCB(t, 10, 5)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		cb.Execute(ctx, fail) //nolint:errcheck
	}
	if cb.State() != circuitbreaker.StateOpen {
		t.Fatalf("expected Open after 5 failures, got %s", cb.State())
	}
}

// TestCB_OpenAfterFailureRateThreshold_TimeBased verifies time-based opening.
func TestCB_OpenAfterFailureRateThreshold_TimeBased(t *testing.T) {
	clk := newClock()
	cfg := circuitbreaker.Config{
		Name:                 t.Name(),
		WindowType:           circuitbreaker.TimeBased,
		WindowDuration:       10 * time.Second,
		BucketDuration:       time.Second,
		FailureThreshold:     3,
		FailureRateThreshold: 0.5,
		MinimumRequests:      5,
		OpenTimeout:          time.Second,
		Clock:                clk,
	}
	cb := circuitbreaker.New(cfg)
	ctx := context.Background()

	// 3 failures, 2 successes — rate=0.6 ≥ 0.5, count ≥ 3, requests=5 ≥ 5
	for i := 0; i < 3; i++ {
		cb.Execute(ctx, fail) //nolint:errcheck
	}
	for i := 0; i < 2; i++ {
		cb.Execute(ctx, succeed) //nolint:errcheck
	}

	if cb.State() != circuitbreaker.StateOpen {
		t.Fatalf("expected Open (failure rate 60%% with 5 requests), got %s", cb.State())
	}
}

// TestCB_MinimumRequestsNotMet_StaysClosed verifies minimum request gate.
func TestCB_MinimumRequestsNotMet_StaysClosed(t *testing.T) {
	clk := newClock()
	cfg := circuitbreaker.Config{
		Name:                 t.Name(),
		WindowType:           circuitbreaker.TimeBased,
		WindowDuration:       10 * time.Second,
		BucketDuration:       time.Second,
		FailureThreshold:     1,
		FailureRateThreshold: 0.1,
		MinimumRequests:      10,
		OpenTimeout:          time.Second,
		Clock:                clk,
	}
	cb := circuitbreaker.New(cfg)
	ctx := context.Background()

	// 3 failures but MinReqs=10 → stays closed
	for i := 0; i < 3; i++ {
		cb.Execute(ctx, fail) //nolint:errcheck
	}
	if cb.State() != circuitbreaker.StateClosed {
		t.Fatalf("expected Closed (minimum requests not met), got %s", cb.State())
	}
}

// TestCB_OpenToHalfOpen_AfterTimeout verifies Open→HalfOpen transition.
func TestCB_OpenToHalfOpen_AfterTimeout(t *testing.T) {
	clk := newClock()
	cfg := circuitbreaker.Config{
		Name:             t.Name(),
		WindowType:       circuitbreaker.CountBased,
		WindowSize:       5,
		FailureThreshold: 3,
		OpenTimeout:      500 * time.Millisecond,
		Clock:            clk,
	}
	cb := circuitbreaker.New(cfg)
	ctx := context.Background()

	// Open the circuit
	for i := 0; i < 3; i++ {
		cb.Execute(ctx, fail) //nolint:errcheck
	}
	if cb.State() != circuitbreaker.StateOpen {
		t.Fatalf("expected Open, got %s", cb.State())
	}

	// Advance past OpenTimeout
	clk.Advance(600 * time.Millisecond)

	// Next execute should transition to HalfOpen and succeed
	err := cb.Execute(ctx, succeed)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	// After success, should be Closed (SuccessThreshold=1 by default)
	if cb.State() != circuitbreaker.StateClosed {
		t.Fatalf("expected Closed after half-open success, got %s", cb.State())
	}
}

// TestCB_HalfOpen_ProbeSucceeds_CloseCircuit verifies HalfOpen→Closed after the
// configured number of consecutive successful probes.
func TestCB_HalfOpen_ProbeSucceeds_CloseCircuit(t *testing.T) {
	clk := newClock()
	cfg := circuitbreaker.Config{
		Name:                t.Name(),
		WindowType:          circuitbreaker.CountBased,
		WindowSize:          5,
		FailureThreshold:    3,
		OpenTimeout:         100 * time.Millisecond,
		SuccessThreshold:    2,
		HalfOpenMaxRequests: 2, // allow both probes serially without rejection
		Clock:               clk,
	}
	cb := circuitbreaker.New(cfg)
	ctx := context.Background()

	// Open circuit
	for i := 0; i < 3; i++ {
		cb.Execute(ctx, fail) //nolint:errcheck
	}
	if cb.State() != circuitbreaker.StateOpen {
		t.Fatalf("expected Open, got %s", cb.State())
	}

	clk.Advance(200 * time.Millisecond)

	// First probe success: transitions Open→HalfOpen but must NOT yet close
	// because SuccessThreshold=2.
	if err := cb.Execute(ctx, succeed); err != nil {
		t.Fatalf("first probe should run, got %v", err)
	}
	if cb.State() != circuitbreaker.StateHalfOpen {
		t.Fatalf("expected HalfOpen after 1 of 2 required successes, got %s", cb.State())
	}

	// Second consecutive probe success: now the circuit must close.
	if err := cb.Execute(ctx, succeed); err != nil {
		t.Fatalf("second probe should run, got %v", err)
	}
	if cb.State() != circuitbreaker.StateClosed {
		t.Fatalf("expected Closed after 2 consecutive successes, got %s", cb.State())
	}
}

// TestCB_HalfOpen_ProbeFails_ReopenCircuit verifies HalfOpen→Open.
func TestCB_HalfOpen_ProbeFails_ReopenCircuit(t *testing.T) {
	clk := newClock()
	cfg := circuitbreaker.Config{
		Name:             t.Name(),
		WindowType:       circuitbreaker.CountBased,
		WindowSize:       5,
		FailureThreshold: 3,
		OpenTimeout:      100 * time.Millisecond,
		Clock:            clk,
	}
	cb := circuitbreaker.New(cfg)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		cb.Execute(ctx, fail) //nolint:errcheck
	}
	clk.Advance(200 * time.Millisecond)

	// Probe fails → reopen
	err := cb.Execute(ctx, fail)
	if err != errTest {
		t.Fatalf("expected errTest, got %v", err)
	}
	if cb.State() != circuitbreaker.StateOpen {
		t.Fatalf("expected Open after half-open failure, got %s", cb.State())
	}
}

// TestCB_HalfOpen_ExcessProbesRejected verifies probe limit in HalfOpen.
func TestCB_HalfOpen_ExcessProbesRejected(t *testing.T) {
	clk := newClock()
	cfg := circuitbreaker.Config{
		Name:                t.Name(),
		WindowType:          circuitbreaker.CountBased,
		WindowSize:          5,
		FailureThreshold:    3,
		OpenTimeout:         100 * time.Millisecond,
		HalfOpenMaxRequests: 1,
		Clock:               clk,
	}
	cb := circuitbreaker.New(cfg)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		cb.Execute(ctx, fail) //nolint:errcheck
	}
	clk.Advance(200 * time.Millisecond)

	// First probe: starts executing (blocks in the slow fn)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		cb.Execute(ctx, func(ctx context.Context) error { //nolint:errcheck
			time.Sleep(50 * time.Millisecond)
			return nil
		})
	}()

	time.Sleep(5 * time.Millisecond) // let first probe start

	// Second probe: should be rejected
	err := cb.Execute(ctx, succeed)
	if err == nil || !errors.Is(err, circuitbreaker.ErrTooManyRequests) {
		t.Fatalf("expected ErrTooManyRequests, got %v", err)
	}
	wg.Wait()
}

// TestCB_ContextCancellation_NotCountedAsFailure verifies context cancel doesn't trip CB.
func TestCB_ContextCancellation_NotCountedAsFailure(t *testing.T) {
	cb := countBasedCB(t, 10, 5)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	for i := 0; i < 10; i++ {
		cb.Execute(ctx, func(ctx context.Context) error { //nolint:errcheck
			return context.Canceled
		})
	}

	if cb.State() != circuitbreaker.StateClosed {
		t.Fatalf("context cancellation should NOT trip circuit, got %s", cb.State())
	}
}

// TestCB_CustomIsFailure_FiltersErrors verifies custom error filtering.
func TestCB_CustomIsFailure_FiltersErrors(t *testing.T) {
	sentinel := errors.New("sentinel")
	cb := countBasedCB(t, 10, 3, func(cfg *circuitbreaker.Config) {
		cfg.IsFailure = func(err error) bool {
			return errors.Is(err, sentinel)
		}
	})
	ctx := context.Background()

	// Non-sentinel errors should not count
	for i := 0; i < 5; i++ {
		cb.Execute(ctx, fail) //nolint:errcheck
	}
	if cb.State() != circuitbreaker.StateClosed {
		t.Fatalf("non-sentinel errors should not trip circuit, got %s", cb.State())
	}

	// Sentinel errors should count
	for i := 0; i < 3; i++ {
		cb.Execute(ctx, func(ctx context.Context) error { //nolint:errcheck
			return sentinel
		})
	}
	if cb.State() != circuitbreaker.StateOpen {
		t.Fatalf("sentinel errors should trip circuit, got %s", cb.State())
	}
}

// TestCB_Callbacks_AllFiredCorrectly verifies all callbacks fire.
func TestCB_Callbacks_AllFiredCorrectly(t *testing.T) {
	var successes, failures, rejections atomic.Int64
	var stateChanges []string
	var mu sync.Mutex

	clk := newClock()
	cfg := circuitbreaker.Config{
		Name:             t.Name(),
		WindowType:       circuitbreaker.CountBased,
		WindowSize:       5,
		FailureThreshold: 3,
		OpenTimeout:      100 * time.Millisecond,
		Clock:            clk,
		OnSuccess: func(name string, _ time.Duration) {
			successes.Add(1)
		},
		OnFailure: func(name string, _ time.Duration, _ error) {
			failures.Add(1)
		},
		OnRejected: func(name string) {
			rejections.Add(1)
		},
		OnStateChange: func(name string, from, to circuitbreaker.State) {
			mu.Lock()
			stateChanges = append(stateChanges, from.String()+"→"+to.String())
			mu.Unlock()
		},
	}
	cb := circuitbreaker.New(cfg)
	ctx := context.Background()

	cb.Execute(ctx, succeed) //nolint:errcheck
	for i := 0; i < 3; i++ {
		cb.Execute(ctx, fail) //nolint:errcheck
	}
	cb.Execute(ctx, fail) //nolint:errcheck // rejected

	if successes.Load() != 1 {
		t.Errorf("expected 1 success, got %d", successes.Load())
	}
	if failures.Load() != 3 {
		t.Errorf("expected 3 failures, got %d", failures.Load())
	}
	if rejections.Load() != 1 {
		t.Errorf("expected 1 rejection, got %d", rejections.Load())
	}

	mu.Lock()
	defer mu.Unlock()
	if len(stateChanges) == 0 || stateChanges[0] != "closed→open" {
		t.Errorf("expected closed→open state change, got %v", stateChanges)
	}
}

// TestCB_Execute_ReturnsCircuitOpenError verifies errors.Is works.
func TestCB_Execute_ReturnsCircuitOpenError(t *testing.T) {
	cb := countBasedCB(t, 5, 3)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		cb.Execute(ctx, fail) //nolint:errcheck
	}

	err := cb.Execute(ctx, succeed)
	if !errors.Is(err, circuitbreaker.ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen, got %v", err)
	}
}

// TestCB_ExecuteWithFallback_FallbackCalledWhenOpen verifies fallback on open circuit.
func TestCB_ExecuteWithFallback_FallbackCalledWhenOpen(t *testing.T) {
	cb := countBasedCB(t, 5, 3)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		cb.Execute(ctx, fail) //nolint:errcheck
	}

	fallbackCalled := false
	err := cb.ExecuteWithFallback(ctx, succeed, func(ctx context.Context, origErr error) error {
		fallbackCalled = true
		if !errors.Is(origErr, circuitbreaker.ErrCircuitOpen) {
			t.Errorf("expected ErrCircuitOpen in fallback, got %v", origErr)
		}
		return nil
	})

	if err != nil {
		t.Fatalf("fallback should not return error: %v", err)
	}
	if !fallbackCalled {
		t.Fatal("fallback should have been called")
	}
}

// TestCB_ExecuteWithFallback_FallbackCalledOnError verifies fallback on regular error.
func TestCB_ExecuteWithFallback_FallbackCalledOnError(t *testing.T) {
	cb := countBasedCB(t, 10, 10) // high threshold so circuit stays closed
	ctx := context.Background()

	fallbackCalled := false
	cb.ExecuteWithFallback(ctx, fail, func(ctx context.Context, origErr error) error { //nolint:errcheck
		fallbackCalled = true
		return nil
	})

	if !fallbackCalled {
		t.Fatal("fallback should be called on error")
	}
}

// TestCB_Concurrent_StateTransitions_NoRace verifies no data races under high concurrency.
func TestCB_Concurrent_StateTransitions_NoRace(t *testing.T) {
	cb := countBasedCB(t, 10, 5)
	ctx := context.Background()

	var wg sync.WaitGroup
	var i atomic.Int64
	for n := 0; n < 1000; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n := i.Add(1)
			if n%3 == 0 {
				cb.Execute(ctx, fail) //nolint:errcheck
			} else {
				cb.Execute(ctx, succeed) //nolint:errcheck
			}
		}()
	}
	wg.Wait()
}

// TestCB_MetricWindow_CountBased_CorrectCount verifies count window ring buffer.
func TestCB_MetricWindow_CountBased_CorrectCount(t *testing.T) {
	cb := countBasedCB(t, 5, 5) // window=5, threshold=5
	ctx := context.Background()

	// Fill 5 successes
	for i := 0; i < 5; i++ {
		cb.Execute(ctx, succeed) //nolint:errcheck
	}
	// Now 5 failures should trip it (filling the ring with failures)
	for i := 0; i < 5; i++ {
		cb.Execute(ctx, fail) //nolint:errcheck
	}
	if cb.State() != circuitbreaker.StateOpen {
		t.Fatalf("expected Open after 5 failures in window of 5, got %s", cb.State())
	}
}

// TestCB_MetricWindow_TimeBased_SlideCorrectly verifies time window slides.
func TestCB_MetricWindow_TimeBased_SlideCorrectly(t *testing.T) {
	clk := newClock()
	cfg := circuitbreaker.Config{
		Name:                 t.Name(),
		WindowType:           circuitbreaker.TimeBased,
		WindowDuration:       5 * time.Second,
		BucketDuration:       time.Second,
		FailureThreshold:     5,
		FailureRateThreshold: 1.0,
		MinimumRequests:      1,
		OpenTimeout:          time.Second,
		Clock:                clk,
	}
	cb := circuitbreaker.New(cfg)
	ctx := context.Background()

	// 3 failures in first second
	for i := 0; i < 3; i++ {
		cb.Execute(ctx, fail) //nolint:errcheck
	}

	// Advance 6 seconds — those 3 failures slide out of window
	clk.Advance(6 * time.Second)

	// Now 2 more failures — total in window should be 2 (below threshold 5)
	for i := 0; i < 2; i++ {
		cb.Execute(ctx, fail) //nolint:errcheck
	}
	if cb.State() != circuitbreaker.StateClosed {
		t.Fatalf("old failures should have slid out, expected Closed, got %s", cb.State())
	}
}

// TestCB_RequestTimeout_CountsAsFailure verifies CB-imposed timeout counts as failure.
func TestCB_RequestTimeout_CountsAsFailure(t *testing.T) {
	cfg := circuitbreaker.Config{
		Name:             t.Name(),
		WindowType:       circuitbreaker.CountBased,
		WindowSize:       5,
		FailureThreshold: 3,
		OpenTimeout:      time.Second,
		RequestTimeout:   10 * time.Millisecond,
		Clock:            clock.RealClock{},
	}
	cb := circuitbreaker.New(cfg)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		cb.Execute(ctx, func(ctx context.Context) error { //nolint:errcheck
			time.Sleep(100 * time.Millisecond) // longer than RequestTimeout
			return ctx.Err()
		})
	}

	if cb.State() != circuitbreaker.StateOpen {
		t.Fatalf("CB timeout should count as failure, expected Open, got %s", cb.State())
	}
}

// TestCB_Registry_GetOrCreate verifies registry returns same instance.
func TestCB_Registry_GetOrCreate(t *testing.T) {
	reg := circuitbreaker.NewRegistry()
	cfg := circuitbreaker.Config{WindowType: circuitbreaker.CountBased, WindowSize: 10, FailureThreshold: 5}

	cb1 := reg.GetOrCreate("my-service", cfg)
	cb2 := reg.GetOrCreate("my-service", cfg)

	if cb1 != cb2 {
		t.Fatal("GetOrCreate should return the same instance for the same name")
	}
}

// TestCB_Registry_Snapshot verifies registry snapshot.
func TestCB_Registry_Snapshot(t *testing.T) {
	reg := circuitbreaker.NewRegistry()
	cfg := circuitbreaker.Config{WindowType: circuitbreaker.CountBased, WindowSize: 10, FailureThreshold: 5}
	reg.GetOrCreate("svc-a", cfg)
	reg.GetOrCreate("svc-b", cfg)

	snap := reg.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("expected 2 snapshots, got %d", len(snap))
	}
}

// TestCB_NoGoroutineLeak verifies that creating and using a CB then letting it
// go out of scope does not leak any goroutines.
func TestCB_NoGoroutineLeak(t *testing.T) {
	lc := testutil.NewLeakChecker(t)
	defer lc.Check()

	clk := newClock()
	cb := circuitbreaker.New(circuitbreaker.Config{
		Name:             "leak-test",
		WindowType:       circuitbreaker.CountBased,
		WindowSize:       5,
		FailureThreshold: 3,
		OpenTimeout:      100 * time.Millisecond,
		Clock:            clk,
	})
	ctx := context.Background()

	// Exercise it
	cb.Execute(ctx, func(_ context.Context) error { return nil })     //nolint:errcheck
	cb.Execute(ctx, func(_ context.Context) error { return errTest }) //nolint:errcheck
	cb.Snapshot()

	// CircuitBreaker has no background goroutines by design — just verify it
	// doesn't spawn any during normal use.
}

// TestCB_SuccessThreshold_RequiresConsecutiveSuccesses verifies that a CB
// with SuccessThreshold=2 only transitions to Closed after 2 consecutive
// successful probes in HalfOpen state.
func TestCB_SuccessThreshold_RequiresConsecutiveSuccesses(t *testing.T) {
	clk := newClock()
	cfg := circuitbreaker.Config{
		Name:                "thresh-test",
		WindowType:          circuitbreaker.CountBased,
		WindowSize:          5,
		FailureThreshold:    3,
		OpenTimeout:         50 * time.Millisecond,
		SuccessThreshold:    2,
		HalfOpenMaxRequests: 3,
		Clock:               clk,
	}
	cb := circuitbreaker.New(cfg)
	ctx := context.Background()

	// Trip to Open
	for i := 0; i < 3; i++ {
		cb.Execute(ctx, func(_ context.Context) error { return errTest }) //nolint:errcheck
	}
	if cb.State() != circuitbreaker.StateOpen {
		t.Fatalf("expected Open after 3 failures, got %s", cb.State())
	}

	// Advance past OpenTimeout to allow HalfOpen
	clk.Advance(100 * time.Millisecond)

	// First probe succeeds — must NOT yet be Closed (SuccessThreshold=2).
	if err := cb.Execute(ctx, func(_ context.Context) error { return nil }); err != nil {
		t.Fatalf("first probe should run, got %v", err)
	}
	if cb.State() != circuitbreaker.StateHalfOpen {
		t.Fatalf("expected HalfOpen after 1 of 2 required successes, got %s", cb.State())
	}

	// Second consecutive probe succeeds — now must be Closed.
	if err := cb.Execute(ctx, func(_ context.Context) error { return nil }); err != nil {
		t.Fatalf("second probe should run, got %v", err)
	}
	if cb.State() != circuitbreaker.StateClosed {
		t.Fatalf("expected Closed after 2 consecutive successes, got %s", cb.State())
	}
}

// TestCB_OpenRejectsImmediately verifies the open fast-path: Execute returns
// ErrCircuitOpen without calling the wrapped function.
func TestCB_OpenRejectsImmediately(t *testing.T) {
	clk := newClock()
	cb := countBasedCB(t, 5, 3)
	ctx := context.Background()

	// Trip to Open
	for i := 0; i < 3; i++ {
		cb.Execute(ctx, func(_ context.Context) error { return errTest }) //nolint:errcheck
	}

	var called atomic.Bool
	err := cb.Execute(ctx, func(_ context.Context) error {
		called.Store(true)
		return nil
	})

	if !errors.Is(err, circuitbreaker.ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen, got %v", err)
	}
	if called.Load() {
		t.Fatal("wrapped function must not be called when circuit is Open")
	}
	_ = clk
}

// TestCB_StateTransitions_Callbacks fires OnStateChange on every transition.
func TestCB_StateTransitions_Callbacks(t *testing.T) {
	clk := newClock()
	var transitions []string
	var mu sync.Mutex

	cb := circuitbreaker.New(circuitbreaker.Config{
		Name:             "callbacks",
		WindowType:       circuitbreaker.CountBased,
		WindowSize:       5,
		FailureThreshold: 3,
		OpenTimeout:      50 * time.Millisecond,
		Clock:            clk,
		OnStateChange: func(name string, from, to circuitbreaker.State) {
			mu.Lock()
			transitions = append(transitions, from.String()+"→"+to.String())
			mu.Unlock()
		},
	})
	ctx := context.Background()

	// Closed → Open
	for i := 0; i < 3; i++ {
		cb.Execute(ctx, func(_ context.Context) error { return errTest }) //nolint:errcheck
	}
	// Open → HalfOpen
	clk.Advance(100 * time.Millisecond)
	cb.Execute(ctx, func(_ context.Context) error { return nil }) //nolint:errcheck

	mu.Lock()
	defer mu.Unlock()
	if len(transitions) == 0 {
		t.Fatal("expected at least one OnStateChange callback")
	}
	// Verify first transition is Closed → Open
	if transitions[0] != "closed→open" {
		t.Fatalf("expected first transition closed→open, got %s", transitions[0])
	}
}
