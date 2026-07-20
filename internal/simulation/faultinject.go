package simulation

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
)

// ErrInjected is the default error returned by an injected failure. Scenarios
// that need a distinct sentinel can supply their own via WithError.
var ErrInjected = errors.New("simulation: injected fault")

// FaultInjector wraps an operation (func(context.Context) error) so that each
// invocation can deterministically inject latency (advancing the caller through
// a virtual-clock sleep) and/or return an injected error, according to a
// reproducible Schedule.
//
// A FaultInjector is safe for concurrent use: invocation N (0-indexed, assigned
// atomically in call order) always sees the same latency/failure decision for a
// given schedule and seed, regardless of goroutine interleaving. This is what
// makes breaker/retry/bulkhead scenarios reproducible.
//
// Construct one with NewFaultInjector and pass its Fn to the primitive under
// test. Drive virtual time with a Sim so injected latency resolves
// deterministically.
type FaultInjector struct {
	clk clock.Clock

	mu    sync.Mutex
	rng   *rand.Rand
	sched Schedule
	calls int64

	// observed counters (read via Stats).
	total   int64
	failed  int64
	latSum  time.Duration
	lastErr error
}

// Schedule describes a deterministic latency/failure plan for a FaultInjector.
// All fields are optional; the zero Schedule injects nothing (every call
// succeeds instantly). Fields compose: FailAt/SucceedAt take precedence over
// the probabilistic FailureRate, and PerCallLatency (indexed) takes precedence
// over the fixed Latency.
type Schedule struct {
	// Latency is a fixed virtual latency injected before every call returns.
	Latency time.Duration

	// PerCallLatency, when non-nil, overrides Latency for call index i
	// (0-based) if i < len(PerCallLatency). Later calls fall back to Latency.
	PerCallLatency []time.Duration

	// FailAt lists specific call indices (0-based) that must fail. Deterministic
	// and independent of the RNG.
	FailAt []int

	// SucceedAt lists call indices that must succeed even if FailureRate or
	// FailAt would otherwise fail them. Useful to model "recovery" — e.g. the
	// probe after OpenTimeout succeeds.
	SucceedAt []int

	// FailFirstN makes the first N calls fail (indices 0..N-1), then succeed.
	// Composes with FailAt/SucceedAt (SucceedAt wins).
	FailFirstN int

	// FailureRate is the probability [0,1] that a call fails, evaluated with the
	// seeded RNG when no deterministic rule (FailAt/FailFirstN/SucceedAt)
	// applies. 0 disables probabilistic failures.
	FailureRate float64

	// Err is the error returned by an injected failure. Defaults to ErrInjected.
	Err error
}

// Stats is a snapshot of a FaultInjector's observed behaviour.
type Stats struct {
	// Total is the number of times the wrapped operation was invoked.
	Total int64
	// Failed is the number of invocations that returned an injected error.
	Failed int64
	// InjectedLatency is the sum of virtual latency injected across all calls.
	InjectedLatency time.Duration
}

// Option configures a FaultInjector.
type Option func(*FaultInjector)

// WithSeed sets the RNG seed used for probabilistic (FailureRate) decisions.
// The same seed + schedule always produces the same sequence of outcomes.
// Defaults to 1.
func WithSeed(seed int64) Option {
	return func(f *FaultInjector) { f.rng = rand.New(rand.NewSource(seed)) }
}

// WithError sets the default injected error (equivalent to Schedule.Err but
// applied after construction; Schedule.Err, if non-nil, still wins).
func WithError(err error) Option {
	return func(f *FaultInjector) {
		if err != nil && f.sched.Err == nil {
			f.sched.Err = err
		}
	}
}

// NewFaultInjector builds a FaultInjector that injects virtual latency using
// clk and fails according to sched. clk should be the Sim's clock so injected
// latency resolves under the test's control. A nil clk uses clock.RealClock
// (only appropriate outside a Sim).
func NewFaultInjector(clk clock.Clock, sched Schedule, opts ...Option) *FaultInjector {
	if clk == nil {
		clk = clock.RealClock{}
	}
	f := &FaultInjector{
		clk:   clk,
		rng:   rand.New(rand.NewSource(1)),
		sched: sched,
	}
	for _, o := range opts {
		o(f)
	}
	if f.sched.Err == nil {
		f.sched.Err = ErrInjected
	}
	return f
}

// Fn returns the wrapped operation to hand to the primitive under test. Each
// invocation is assigned the next call index (in the order calls begin),
// injects that index's scheduled latency via the virtual clock, honours ctx
// cancellation during the latency sleep, and returns the scheduled error (or
// nil).
func (f *FaultInjector) Fn() func(context.Context) error {
	return func(ctx context.Context) error {
		idx := f.nextIndex()
		lat := f.latencyFor(idx)
		fail := f.decideFailure(idx)

		if lat > 0 {
			if err := sleepCtx(ctx, f.clk, lat); err != nil {
				// Context cancelled/timed out mid-latency: report that, don't
				// mask it with the injected error. Still count the call.
				f.record(lat, false, nil)
				return err
			}
		}

		if fail {
			f.record(lat, true, f.sched.Err)
			return f.sched.Err
		}
		f.record(lat, false, nil)
		return nil
	}
}

// Calls returns how many times the wrapped op has been invoked so far.
func (f *FaultInjector) Calls() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.total
}

// Stats returns a snapshot of observed behaviour.
func (f *FaultInjector) Stats() Stats {
	f.mu.Lock()
	defer f.mu.Unlock()
	return Stats{Total: f.total, Failed: f.failed, InjectedLatency: f.latSum}
}

func (f *FaultInjector) nextIndex() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	i := f.calls
	f.calls++
	return i
}

func (f *FaultInjector) latencyFor(idx int64) time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	if idx >= 0 && int(idx) < len(f.sched.PerCallLatency) {
		return f.sched.PerCallLatency[idx]
	}
	return f.sched.Latency
}

// decideFailure resolves whether call idx should fail. Deterministic rules take
// precedence over the probabilistic FailureRate. SucceedAt is the highest
// priority (forced recovery).
func (f *FaultInjector) decideFailure(idx int64) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, s := range f.sched.SucceedAt {
		if int64(s) == idx {
			return false
		}
	}
	for _, fa := range f.sched.FailAt {
		if int64(fa) == idx {
			return true
		}
	}
	if idx < int64(f.sched.FailFirstN) {
		return true
	}
	if f.sched.FailureRate > 0 {
		return f.rng.Float64() < f.sched.FailureRate
	}
	return false
}

func (f *FaultInjector) record(lat time.Duration, failed bool, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.total++
	f.latSum += lat
	if failed {
		f.failed++
		f.lastErr = err
	}
}

// sleepCtx sleeps for d on the given clock, returning early with ctx.Err() if
// the context is cancelled first. It mirrors the retry package's internal
// sleepWithContext so injected latency behaves identically under virtual time.
func sleepCtx(ctx context.Context, clk clock.Clock, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := clk.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
