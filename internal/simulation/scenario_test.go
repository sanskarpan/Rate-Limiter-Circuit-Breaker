package simulation_test

// These scenario tests are the point of the package: they take primitives that
// were previously exercised with time.Sleep-based, timing-dependent tests and
// drive them entirely on virtual time via the simulation harness + fault
// injector. Each asserts an exact, reproducible timeline.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/simulation"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/retry"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/retry/backoff"
)

// TestScenario_CircuitBreaker_TripAndRecover deterministically drives the
// breaker through Closed -> Open -> (after exactly OpenTimeout) HalfOpen ->
// Closed, using a fault injector whose failures trip it and whose scheduled
// recovery closes it. No wall-clock sleeps: the Open->HalfOpen transition is
// released purely by advancing virtual time.
func TestScenario_CircuitBreaker_TripAndRecover(t *testing.T) {
	t.Parallel()
	sim := simulation.NewSim()
	const openTimeout = 30 * time.Second

	cb := circuitbreaker.New(circuitbreaker.Config{
		Name:             "sim-cb",
		WindowType:       circuitbreaker.CountBased,
		WindowSize:       10,
		FailureThreshold: 5,
		OpenTimeout:      openTimeout,
		SuccessThreshold: 1,
		Clock:            sim.Clock(),
	})

	// First 5 calls fail (trip), the probe after OpenTimeout succeeds (recover).
	fi := simulation.NewFaultInjector(sim.Clock(), simulation.Schedule{
		FailFirstN: 5,
		SucceedAt:  []int{5}, // the half-open probe
	})
	op := fi.Fn()

	// Drive 5 failures synchronously (Execute doesn't sleep on the virtual
	// clock, so no advance is needed between them).
	for i := 0; i < 5; i++ {
		if err := cb.Execute(context.Background(), op); err == nil {
			t.Fatalf("call %d: expected injected failure", i)
		}
	}
	if got := cb.State(); got != circuitbreaker.StateOpen {
		t.Fatalf("after 5 failures state = %v, want Open", got)
	}

	// While Open, a call is rejected without invoking the op.
	callsBefore := fi.Calls()
	if err := cb.Execute(context.Background(), op); err == nil {
		t.Fatal("expected rejection while Open")
	}
	if fi.Calls() != callsBefore {
		t.Fatal("op was invoked while circuit Open; should have been rejected")
	}

	// Not yet time to probe: advance just short of OpenTimeout.
	sim.Advance(openTimeout - time.Millisecond)
	if got := cb.State(); got != circuitbreaker.StateOpen {
		t.Fatalf("before OpenTimeout state = %v, want still Open", got)
	}

	// Cross OpenTimeout. The next call is admitted as a half-open probe and,
	// per the schedule, succeeds — closing the circuit.
	sim.Advance(2 * time.Millisecond)
	if err := cb.Execute(context.Background(), op); err != nil {
		t.Fatalf("probe after OpenTimeout: unexpected err %v", err)
	}
	if got := cb.State(); got != circuitbreaker.StateClosed {
		t.Fatalf("after successful probe state = %v, want Closed", got)
	}
}

// TestScenario_Retry_BackoffTiming asserts the EXACT virtual time consumed by a
// retry policy's exponential backoff. The operation always fails, so all
// attempts are exhausted; each backoff sleep is released by a precise Advance,
// and we verify total elapsed virtual time equals the sum of the backoff
// intervals — something a wall-clock test cannot assert deterministically.
func TestScenario_Retry_BackoffTiming(t *testing.T) {
	t.Parallel()
	sim := simulation.NewSim()

	// base=10ms, so waits are Next(0)=10ms, Next(1)=20ms, Next(2)=40ms.
	// 4 attempts => 3 backoff sleeps => total 70ms.
	policy := retry.New(
		retry.WithMaxAttempts(4),
		retry.WithBackoff(backoff.Exponential(10*time.Millisecond, time.Second)),
		retry.WithClock(sim.Clock()),
	)

	fi := simulation.NewFaultInjector(sim.Clock(), simulation.Schedule{
		FailureRate: 1.0, // always fail
	})

	start := sim.Now()
	h := sim.RunOp(context.Background(), func(ctx context.Context) error {
		return policy.Do(ctx, fi.Fn())
	})

	// Release the three backoff sleeps one at a time, asserting the op stays
	// blocked until each exact interval elapses.
	waits := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 40 * time.Millisecond}
	for i, w := range waits {
		if res := h.Poll(); res.Done {
			t.Fatalf("op finished before backoff sleep %d released", i)
		}
		sim.Advance(w - time.Nanosecond)
		if res := h.Poll(); res.Done {
			t.Fatalf("op finished before full backoff %d (%v) elapsed", i, w)
		}
		sim.Advance(time.Nanosecond)
	}

	res := h.Wait()
	if res.Err == nil {
		t.Fatal("expected final failure after exhausting attempts")
	}
	if got := fi.Calls(); got != 4 {
		t.Fatalf("op called %d times, want 4 (MaxAttempts)", got)
	}
	if elapsed := sim.Now().Sub(start); elapsed != 70*time.Millisecond {
		t.Fatalf("total virtual backoff = %v, want 70ms", elapsed)
	}
}

// TestScenario_Retry_SucceedsMidway shows a mixed schedule: fail, fail, succeed.
// Only two backoff sleeps occur; the third attempt succeeds and the policy
// returns nil. Driven with RunFor so the test need not enumerate each sleep.
func TestScenario_Retry_SucceedsMidway(t *testing.T) {
	t.Parallel()
	sim := simulation.NewSim()
	policy := retry.New(
		retry.WithMaxAttempts(5),
		retry.WithBackoff(backoff.Constant(5*time.Millisecond)),
		retry.WithClock(sim.Clock()),
	)
	fi := simulation.NewFaultInjector(sim.Clock(), simulation.Schedule{FailFirstN: 2})

	h := sim.RunOp(context.Background(), func(ctx context.Context) error {
		return policy.Do(ctx, fi.Fn())
	})
	res := h.RunFor(time.Millisecond, 100*time.Millisecond)
	if !res.Done {
		t.Fatal("op did not finish within budget")
	}
	if res.Err != nil {
		t.Fatalf("expected success on 3rd attempt, got %v", res.Err)
	}
	if got := fi.Calls(); got != 3 {
		t.Fatalf("op called %d times, want 3 (fail, fail, succeed)", got)
	}
}

// TestScenario_Retry_BudgetStopsRetries proves a retry budget deterministically
// halts a retry storm: with the budget exhausted, the second attempt is denied
// and the op is not re-invoked, regardless of backoff timing.
func TestScenario_Retry_BudgetStopsRetries(t *testing.T) {
	t.Parallel()
	sim := simulation.NewSim()
	// Ratio 0 + MinPerSecond 0 => bucket capacity clamps to 1: exactly ONE
	// retry token exists and there is no refill under the frozen virtual clock.
	// So the storm is capped at 1 extra attempt regardless of MaxAttempts=5.
	budget := retry.NewBudget(
		retry.BudgetConfig{Ratio: 0, MinPerSecond: 0},
		retry.WithBudgetClock(sim.Clock()),
	)
	policy := retry.New(
		retry.WithMaxAttempts(5),
		retry.WithBackoff(backoff.Constant(time.Millisecond)),
		retry.WithClock(sim.Clock()),
		retry.WithBudget(budget),
	)
	fi := simulation.NewFaultInjector(sim.Clock(), simulation.Schedule{FailureRate: 1.0})

	h := sim.RunOp(context.Background(), func(ctx context.Context) error {
		return policy.Do(ctx, fi.Fn())
	})
	res := h.RunFor(time.Millisecond, 50*time.Millisecond)
	if !res.Done {
		t.Fatal("op did not finish")
	}
	if res.Err == nil {
		t.Fatal("expected failure")
	}
	// Initial attempt + exactly one budgeted retry, then the budget denies
	// further retries (would otherwise run all 5 attempts).
	if got := fi.Calls(); got != 2 {
		t.Fatalf("op called %d times, want 2 (budget stopped retries at 1)", got)
	}
	_ = errors.Is
}
