// Package simulation provides a deterministic, zero-dependency simulation
// harness for the resilience primitives in this repository (circuit breaker,
// retry, bulkhead, ...). It exists to convert previously timing-dependent tests
// — the kind that reach for time.Sleep and hope — into fully deterministic,
// reproducible scenarios driven by a virtual clock.
//
// # Why
//
// The chaos harness (internal/testutil) exercises concurrency and
// internal/clock.ManualClock gives time determinism, but neither injects
// *faults* (latency and errors) into the callee on a deterministic schedule.
// Without that you cannot reliably assert cross-component behaviour such as
// "the breaker trips on the 5th failure, stays open for exactly OpenTimeout,
// then a single probe recovers it", or "retry backoff sleeps for exactly
// 10ms+20ms+40ms". Real-time versions of those tests are flaky.
//
// This package closes that gap with two pieces:
//
//   - Sim: a virtual-clock-driven harness. It owns a *clock.ManualClock and
//     runs an operation on a background goroutine while the test advances
//     virtual time in controlled steps, synchronising on completion. No
//     wall-clock time elapses; a scenario that models an hour of traffic runs
//     in microseconds.
//
//   - FaultInjector (see faultinject.go): wraps a func(context.Context) error
//     so each invocation can inject deterministic latency (via the injectable
//     clock) and deterministic failures, according to a seeded, reproducible
//     Schedule.
//
// # Determinism guarantees
//
//   - Time only moves when the test calls Advance/RunFor. There is no reliance
//     on the OS scheduler for correctness of *timing*.
//   - All randomness is seeded (math/rand's deterministic generator). Given the
//     same seed and schedule, a scenario produces byte-identical outcomes.
//   - Only the standard library and internal/clock are imported — the core
//     module stays zero-dependency.
//
// # Concurrency model
//
// A resilience primitive under test (e.g. retry.Policy.Do) blocks on the
// injected clock while "sleeping". Sim runs the operation on its own goroutine
// and hands control back to the test, which advances the clock to release those
// sleeps. RunOp/Advance use a settle barrier (a bounded, clock-free yield loop)
// to let the operation goroutine make progress after each advance before the
// test observes state — this keeps the *ordering* deterministic even though
// goroutines are involved, because virtual time (the only thing timing
// decisions depend on) is fully controlled by the test.
package simulation

import (
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
)

// Epoch is the fixed virtual start time used by NewSim. Using a constant epoch
// (rather than time.Now) keeps scenarios reproducible across runs and machines.
var Epoch = time.Date(2025, time.January, 1, 0, 0, 0, 0, time.UTC)

// settleSpins is the number of runtime.Gosched yields performed by settle() to
// let a blocked operation goroutine advance after virtual time moves. It is a
// bounded, wall-clock-free barrier: it never sleeps on the OS clock, so it does
// not reintroduce timing dependence — it only trades CPU yields for goroutine
// progress. The value is generous; typical scenarios settle in a handful of
// yields.
const settleSpins = 256

// Sim is a deterministic simulation harness backed by a virtual clock.
//
// A Sim owns a *clock.ManualClock (retrievable via Clock) that must be injected
// into the primitive under test (via its WithClock option / Config.Clock).
// Virtual time advances only when the test calls Advance or RunFor, so every
// timing decision the primitive makes is under the test's control.
//
// Sim is not safe for concurrent use by multiple test goroutines; it is
// intended to be driven by a single test goroutine that starts one operation at
// a time via RunOp.
type Sim struct {
	clk *clock.ManualClock

	mu      sync.Mutex
	running bool
}

// NewSim returns a Sim whose virtual clock starts at Epoch.
func NewSim() *Sim {
	return &Sim{clk: clock.NewManualClock(Epoch)}
}

// NewSimAt returns a Sim whose virtual clock starts at start.
func NewSimAt(start time.Time) *Sim {
	return &Sim{clk: clock.NewManualClock(start)}
}

// Clock returns the harness's virtual clock. Inject it into the primitive under
// test, e.g. circuitbreaker.Config{Clock: sim.Clock()} or
// retry.WithClock(sim.Clock()).
func (s *Sim) Clock() *clock.ManualClock { return s.clk }

// Now returns the current virtual time.
func (s *Sim) Now() time.Time { return s.clk.Now() }

// Advance moves virtual time forward by d, firing any timers/tickers due in that
// span, then settles so blocked operation goroutines make progress before the
// call returns. Passing d <= 0 only settles.
func (s *Sim) Advance(d time.Duration) {
	if d > 0 {
		s.clk.Advance(d)
	}
	settle()
}

// settle yields the current goroutine repeatedly so that other goroutines
// (notably an operation blocked on the virtual clock that we just advanced) get
// a chance to run. It uses only runtime.Gosched — never a wall-clock sleep — so
// it does not reintroduce timing dependence into scenarios.
func settle() {
	for i := 0; i < settleSpins; i++ {
		runtime.Gosched()
	}
}

// OpResult captures the outcome of a simulated operation.
type OpResult struct {
	// Err is the error returned by the operation (nil on success).
	Err error
	// Done reports whether the operation goroutine finished. It is false only
	// if the operation is still running when observed (see RunOp docs).
	Done bool
}

// RunOp starts fn on its own goroutine and returns a handle that lets the test
// advance virtual time and wait for completion. Exactly one operation should be
// in flight per Sim at a time.
//
// Typical usage for a retry/backoff scenario:
//
//	sim := simulation.NewSim()
//	h := sim.RunOp(ctx, func(ctx context.Context) error { return policy.Do(ctx, op) })
//	sim.Advance(10 * time.Millisecond) // release first backoff sleep
//	sim.Advance(20 * time.Millisecond) // release second backoff sleep
//	res := h.Wait()                    // fn has returned
func (s *Sim) RunOp(ctx context.Context, fn func(context.Context) error) *OpHandle {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		panic("simulation: RunOp called while an operation is already in flight")
	}
	s.running = true
	s.mu.Unlock()

	h := &OpHandle{sim: s, done: make(chan struct{})}
	go func() {
		defer close(h.done)
		err := fn(ctx)
		h.mu.Lock()
		h.err = err
		h.finished = true
		h.mu.Unlock()
	}()
	// Let the operation reach its first blocking point (a clock sleep, or
	// return) before handing control back to the test.
	settle()
	return h
}

// OpHandle is a handle to an in-flight simulated operation started by RunOp.
type OpHandle struct {
	sim  *Sim
	done chan struct{}

	mu       sync.Mutex
	err      error
	finished bool
}

// Poll returns the current outcome without blocking. If the operation has not
// yet finished, Done is false and Err is nil.
func (h *OpHandle) Poll() OpResult {
	h.mu.Lock()
	defer h.mu.Unlock()
	return OpResult{Err: h.err, Done: h.finished}
}

// Wait blocks until the operation goroutine returns, then reports its outcome.
// Because the operation only blocks on the virtual clock, the test must have
// advanced time far enough (via Advance/RunFor) for it to complete; otherwise
// Wait blocks forever. Prefer RunFor for scenarios where the exact number of
// sleeps is not known ahead of time.
func (h *OpHandle) Wait() OpResult {
	<-h.done
	h.sim.mu.Lock()
	h.sim.running = false
	h.sim.mu.Unlock()
	h.mu.Lock()
	defer h.mu.Unlock()
	return OpResult{Err: h.err, Done: true}
}

// RunFor advances virtual time in fixed steps up to a maximum, settling after
// each step, until the in-flight operation finishes or the budget is exhausted.
// It returns the operation outcome. This is convenient when a primitive sleeps
// an a-priori-unknown number of times (e.g. retry with jittered backoff): the
// test states the step granularity and an upper bound rather than each sleep.
//
// step must be > 0. maxTotal is the maximum virtual time to advance in total.
func (h *OpHandle) RunFor(step, maxTotal time.Duration) OpResult {
	if step <= 0 {
		panic("simulation: RunFor step must be > 0")
	}
	var elapsed time.Duration
	for {
		if res := h.Poll(); res.Done {
			return h.Wait()
		}
		if elapsed >= maxTotal {
			// Give it one last settle in case it just finished.
			settle()
			if res := h.Poll(); res.Done {
				return h.Wait()
			}
			return OpResult{Done: false}
		}
		h.sim.Advance(step)
		elapsed += step
	}
}
