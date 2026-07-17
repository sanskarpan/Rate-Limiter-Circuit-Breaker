// Package circuitbreaker implements the Circuit Breaker resilience pattern.
//
// A circuit breaker monitors requests to a dependency and "opens" (stops sending
// requests) when failure rates are too high. This prevents cascading failures
// and gives the dependency time to recover.
//
// State machine:
//
//	Closed → Open:     failure threshold exceeded
//	Open → HalfOpen:  OpenTimeout elapsed (lazy, checked on next call)
//	HalfOpen → Closed: SuccessThreshold consecutive successes
//	HalfOpen → Open:  any single failure
//
// All methods are safe for concurrent use.
package circuitbreaker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// CircuitBreaker implements the circuit breaker pattern.
// All methods are safe for concurrent use.
type CircuitBreaker struct {
	cfg Config

	// State management — use atomic operations for fast reads
	state    atomic.Int32 // stores State value
	openedAt atomic.Int64 // unix nanoseconds when circuit opened, 0 if not open

	// HalfOpen probe tracking
	halfOpenInflight atomic.Int64
	consecutiveSuccesses atomic.Int64

	// Metrics window (only one is used based on WindowType)
	countWin *countWindow
	timeWin  *timeWindow

	// Protect state transitions
	mu sync.Mutex
}

// New creates a new CircuitBreaker with the given configuration.
func New(cfg Config) *CircuitBreaker {
	cfg.defaults()
	cb := &CircuitBreaker{cfg: cfg}
	cb.state.Store(int32(StateClosed))
	switch cfg.WindowType {
	case TimeBased:
		cb.timeWin = newTimeWindow(cfg.WindowDuration, cfg.BucketDuration, cfg.Clock)
	default:
		cb.countWin = newCountWindow(cfg.WindowSize)
	}
	return cb
}

// State returns the current state of the circuit breaker.
func (cb *CircuitBreaker) State() State {
	return State(cb.state.Load())
}

// Name returns the circuit breaker's name.
func (cb *CircuitBreaker) Name() string {
	return cb.cfg.Name
}

// HalfOpenInflight returns the number of half-open probe requests currently in
// flight. Exposed primarily to let callers and tests assert the invariant that
// this value always stays within [0, HalfOpenMaxRequests].
func (cb *CircuitBreaker) HalfOpenInflight() int64 {
	return cb.halfOpenInflight.Load()
}

// Execute calls fn if the circuit is closed or half-open (with probe limit).
// Returns ErrCircuitOpen if the circuit is open.
// Returns ErrTooManyRequests if in half-open and inflight >= HalfOpenMaxRequests.
func (cb *CircuitBreaker) Execute(ctx context.Context, fn func(context.Context) error) error {
	// Step 1: Check state and possibly allow. acquiredProbe records whether this
	// call took a half-open probe slot so that afterExecute can release exactly
	// that slot, regardless of any concurrent state transition (H-8).
	acquiredProbe, err := cb.beforeExecute()
	if err != nil {
		if cb.cfg.OnRejected != nil {
			cb.cfg.OnRejected(cb.cfg.Name)
		}
		return err
	}

	// Step 2: Execute with optional timeout
	start := cb.cfg.Clock.Now()
	if cb.cfg.RequestTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cb.cfg.RequestTimeout)
		defer cancel()
	}

	// Step 3: Always record the outcome and release the probe slot, even if fn
	// panics (C-3). On panic we record a failure and re-panic so the caller's
	// panic semantics are preserved, but the probe slot is never leaked.
	defer func() {
		if r := recover(); r != nil {
			duration := cb.cfg.Clock.Now().Sub(start)
			cb.afterExecute(fmt.Errorf("circuitbreaker: panic in Execute: %v", r), duration, ctx, acquiredProbe)
			panic(r)
		}
	}()

	err = fn(ctx)

	duration := cb.cfg.Clock.Now().Sub(start)
	cb.afterExecute(err, duration, ctx, acquiredProbe)

	return err
}

// ExecuteWithFallback calls fn, and if it fails or circuit is open, calls fallback.
func (cb *CircuitBreaker) ExecuteWithFallback(
	ctx context.Context,
	fn func(context.Context) error,
	fallback func(context.Context, error) error,
) error {
	err := cb.Execute(ctx, fn)
	if err != nil {
		return fallback(ctx, err)
	}
	return nil
}

// Snapshot returns a point-in-time view of the circuit breaker state.
func (cb *CircuitBreaker) Snapshot() Snapshot {
	state := cb.State()
	openedNs := cb.openedAt.Load()
	var openedAt time.Time
	var timeUntilHalfOpen time.Duration
	if openedNs > 0 {
		openedAt = time.Unix(0, openedNs)
		if state == StateOpen {
			halfOpenAt := openedAt.Add(cb.cfg.OpenTimeout)
			timeUntilHalfOpen = halfOpenAt.Sub(cb.cfg.Clock.Now())
			if timeUntilHalfOpen < 0 {
				timeUntilHalfOpen = 0
			}
		}
	}

	var failures, successes, requests int
	switch cb.cfg.WindowType {
	case TimeBased:
		f, r := cb.timeWin.counts()
		failures = int(f)
		requests = int(r)
		successes = requests - failures
	default:
		f, t := cb.countWin.counts()
		failures = f
		requests = t
		successes = requests - failures
	}

	var failureRate float64
	if requests > 0 {
		failureRate = float64(failures) / float64(requests)
	}

	return Snapshot{
		Name:              cb.cfg.Name,
		State:             state,
		Failures:          failures,
		Successes:         successes,
		Requests:          requests,
		FailureRate:       failureRate,
		OpenedAt:          openedAt,
		TimeUntilHalfOpen: timeUntilHalfOpen,
	}
}

// String returns a human-readable description.
func (cb *CircuitBreaker) String() string {
	return fmt.Sprintf("CircuitBreaker(%s, state=%s)", cb.cfg.Name, cb.State())
}

// beforeExecute checks state and returns an error if the request should be rejected.
// The returned bool reports whether this call acquired a half-open probe slot; the
// caller MUST thread it to afterExecute so exactly that slot is released later,
// without re-deriving the state (which could disagree — H-8).
func (cb *CircuitBreaker) beforeExecute() (acquiredProbe bool, err error) {
	state := cb.State()
	switch state {
	case StateOpen:
		// Check if we should transition to HalfOpen (lazy)
		if !cb.shouldAttemptReset() {
			return false, &CircuitError{Name: cb.cfg.Name, State: state, Err: ErrCircuitOpen}
		}
		// Transition to HalfOpen
		cb.transitionToHalfOpen()
		// Fall through to HalfOpen check
		fallthrough
	case StateHalfOpen:
		// Acquire a probe slot with a CAS loop so the counter never even
		// transiently exceeds HalfOpenMaxRequests (an optimistic Add-then-rollback
		// would briefly overshoot under concurrency — H-8).
		max := int64(cb.cfg.HalfOpenMaxRequests)
		for {
			cur := cb.halfOpenInflight.Load()
			if cur >= max {
				return false, &CircuitError{Name: cb.cfg.Name, State: state, Err: ErrTooManyRequests}
			}
			if cb.halfOpenInflight.CompareAndSwap(cur, cur+1) {
				break
			}
		}
		acquiredProbe = true
	}
	return acquiredProbe, nil
}

// afterExecute records the outcome and potentially transitions state.
// acquiredProbe reports whether beforeExecute took a probe slot for this call;
// the slot is released iff it was acquired, so the counter can never go negative
// or leak from a state read that disagrees with beforeExecute (H-8).
func (cb *CircuitBreaker) afterExecute(err error, duration time.Duration, ctx context.Context, acquiredProbe bool) {
	state := cb.State()

	// Check if this is a timeout caused by our RequestTimeout, not caller cancellation
	isCBTimeout := false
	if cb.cfg.RequestTimeout > 0 && ctx.Err() == context.DeadlineExceeded && err != nil {
		// This is a CB-imposed timeout
		isCBTimeout = true
	}

	// Determine if this counts as a failure
	// Context cancellation from caller does NOT count (unless it's CB's own timeout)
	isFailure := false
	if err != nil {
		if isCBTimeout {
			isFailure = true
		} else if err == context.Canceled {
			isFailure = false // caller cancelled
		} else {
			isFailure = cb.cfg.IsFailure(err)
		}
	}

	// Release the probe slot before recording the outcome. recordFailure/
	// recordSuccess may transition state; releasing first keeps the inflight
	// counter consistent with the slot this call actually acquired (H-8).
	if acquiredProbe {
		cb.halfOpenInflight.Add(-1)
	}

	if isFailure {
		cb.recordFailure(state, duration, err)
	} else {
		cb.recordSuccess(state, duration)
	}
}

// recordSuccess records a success and potentially closes the circuit.
func (cb *CircuitBreaker) recordSuccess(state State, duration time.Duration) {
	// Record in metrics window
	switch cb.cfg.WindowType {
	case TimeBased:
		cb.timeWin.record(outcomeSuccess)
	default:
		cb.countWin.record(outcomeSuccess)
	}

	if cb.cfg.OnSuccess != nil {
		cb.cfg.OnSuccess(cb.cfg.Name, duration)
	}

	switch state {
	case StateHalfOpen:
		// Track consecutive successes
		successes := cb.consecutiveSuccesses.Add(1)
		if int(successes) >= cb.cfg.SuccessThreshold {
			cb.transitionToClosed()
		}
	case StateClosed:
		// For time-based windows, check if failure rate threshold exceeded
		// even on a success (since minimum requests may now be met)
		if cb.cfg.WindowType == TimeBased && cb.shouldOpen() {
			cb.transitionToOpen()
		}
	}
}

// recordFailure records a failure and potentially opens/reopens the circuit.
func (cb *CircuitBreaker) recordFailure(state State, duration time.Duration, err error) {
	// Record in metrics window
	switch cb.cfg.WindowType {
	case TimeBased:
		cb.timeWin.record(outcomeFailure)
	default:
		cb.countWin.record(outcomeFailure)
	}

	if cb.cfg.OnFailure != nil {
		cb.cfg.OnFailure(cb.cfg.Name, duration, err)
	}

	switch state {
	case StateHalfOpen:
		// Any failure in half-open reopens the circuit
		cb.transitionToOpen()
	case StateClosed:
		// Check if thresholds are exceeded
		if cb.shouldOpen() {
			cb.transitionToOpen()
		}
	}
}

// shouldOpen returns true if the circuit should open based on current metrics.
func (cb *CircuitBreaker) shouldOpen() bool {
	switch cb.cfg.WindowType {
	case TimeBased:
		failures, requests := cb.timeWin.counts()
		if requests < int64(cb.cfg.MinimumRequests) {
			return false
		}
		failureRate := float64(failures) / float64(requests)
		return failures >= int64(cb.cfg.FailureThreshold) && failureRate >= cb.cfg.FailureRateThreshold
	default:
		failures, _ := cb.countWin.counts()
		return failures >= cb.cfg.FailureThreshold
	}
}

// shouldAttemptReset returns true if enough time has passed to try half-open.
func (cb *CircuitBreaker) shouldAttemptReset() bool {
	openedNs := cb.openedAt.Load()
	if openedNs == 0 {
		return false
	}
	openedAt := time.Unix(0, openedNs)
	return !cb.cfg.Clock.Now().Before(openedAt.Add(cb.cfg.OpenTimeout))
}

// transitionToOpen atomically transitions to the Open state.
func (cb *CircuitBreaker) transitionToOpen() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	current := cb.State()
	if current == StateOpen {
		// Already open — refresh openedAt so a re-trip restarts the OpenTimeout.
		// (M-8: the previous nested `if current == StateHalfOpen` here was dead
		// code because we are already inside the current == StateOpen branch.)
		cb.openedAt.Store(cb.cfg.Clock.Now().UnixNano())
		return
	}
	cb.state.Store(int32(StateOpen))
	cb.openedAt.Store(cb.cfg.Clock.Now().UnixNano())
	// Do NOT reset halfOpenInflight here (H-9): probes still in flight own their
	// own decrement in afterExecute. Resetting to 0 would let those decrements
	// drive the counter negative.
	cb.consecutiveSuccesses.Store(0)
	if cb.cfg.OnStateChange != nil {
		cb.cfg.OnStateChange(cb.cfg.Name, current, StateOpen)
	}
}

// transitionToHalfOpen atomically transitions to HalfOpen.
func (cb *CircuitBreaker) transitionToHalfOpen() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if cb.State() != StateOpen {
		return // another goroutine already transitioned
	}
	cb.state.Store(int32(StateHalfOpen))
	// Entering half-open from Open: no probes can be in flight (Open rejects all
	// calls), so the counter is already 0. Don't Store(0) (H-9) — it would risk
	// clobbering a decrement from a straggler released after this transition.
	cb.consecutiveSuccesses.Store(0)
	if cb.cfg.OnStateChange != nil {
		cb.cfg.OnStateChange(cb.cfg.Name, StateOpen, StateHalfOpen)
	}
}

// transitionToClosed atomically transitions to Closed and resets all metrics.
func (cb *CircuitBreaker) transitionToClosed() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	current := cb.State()
	if current == StateClosed {
		return
	}
	cb.state.Store(int32(StateClosed))
	cb.openedAt.Store(0)
	// Don't Store(0) halfOpenInflight here (H-9): a probe whose success triggered
	// this transition still owns its decrement in afterExecute.
	cb.consecutiveSuccesses.Store(0)
	// Reset all metrics
	switch cb.cfg.WindowType {
	case TimeBased:
		cb.timeWin.reset()
	default:
		cb.countWin.reset()
	}
	if cb.cfg.OnStateChange != nil {
		cb.cfg.OnStateChange(cb.cfg.Name, current, StateClosed)
	}
}

// Snapshot holds a point-in-time view of circuit breaker state.
type Snapshot struct {
	Name              string
	State             State
	Failures          int
	Successes         int
	Requests          int
	FailureRate       float64
	OpenedAt          time.Time
	TimeUntilHalfOpen time.Duration
}
