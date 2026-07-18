package circuitbreaker_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cb "github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
)

func manualClock() *clock.ManualClock {
	return clock.NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
}

// TestFixedProbeStrategy_MaxConcurrentProbes verifies the default strategy is a
// flat cap at MaxRequests regardless of the rest of the context.
func TestFixedProbeStrategy_MaxConcurrentProbes(t *testing.T) {
	s := cb.FixedProbeStrategy{}
	if s.Name() != "fixed" {
		t.Errorf("Name = %q, want fixed", s.Name())
	}
	tests := []struct {
		pc   cb.ProbeContext
		want int
	}{
		{cb.ProbeContext{MaxRequests: 1}, 1},
		{cb.ProbeContext{MaxRequests: 5, ConsecutiveSuccesses: 0}, 5},
		{cb.ProbeContext{MaxRequests: 5, ConsecutiveSuccesses: 100}, 5},
		{cb.ProbeContext{MaxRequests: 3, Inflight: 2}, 3},
	}
	for _, tt := range tests {
		if got := s.MaxConcurrentProbes(tt.pc); got != tt.want {
			t.Errorf("MaxConcurrentProbes(%+v) = %d, want %d", tt.pc, got, tt.want)
		}
	}
}

// TestRampProbeStrategy_MaxConcurrentProbes table-drives the ramp math for
// linear and exponential growth, including clamping to [1, MaxRequests].
func TestRampProbeStrategy_MaxConcurrentProbes(t *testing.T) {
	tests := []struct {
		name string
		s    cb.RampProbeStrategy
		pc   cb.ProbeContext
		want int
	}{
		// Linear: start + n*step, clamped to MaxRequests.
		{"linear n=0", cb.RampProbeStrategy{Type: cb.RampLinear, Start: 1, Step: 1}, cb.ProbeContext{MaxRequests: 8, ConsecutiveSuccesses: 0}, 1},
		{"linear n=1", cb.RampProbeStrategy{Type: cb.RampLinear, Start: 1, Step: 1}, cb.ProbeContext{MaxRequests: 8, ConsecutiveSuccesses: 1}, 2},
		{"linear n=3", cb.RampProbeStrategy{Type: cb.RampLinear, Start: 1, Step: 1}, cb.ProbeContext{MaxRequests: 8, ConsecutiveSuccesses: 3}, 4},
		{"linear step2", cb.RampProbeStrategy{Type: cb.RampLinear, Start: 1, Step: 2}, cb.ProbeContext{MaxRequests: 10, ConsecutiveSuccesses: 3}, 7},
		{"linear clamp to max", cb.RampProbeStrategy{Type: cb.RampLinear, Start: 1, Step: 1}, cb.ProbeContext{MaxRequests: 4, ConsecutiveSuccesses: 100}, 4},
		{"linear start2", cb.RampProbeStrategy{Type: cb.RampLinear, Start: 2, Step: 1}, cb.ProbeContext{MaxRequests: 8, ConsecutiveSuccesses: 2}, 4},

		// Exponential: start * 2^(n/step), clamped.
		{"exp n=0", cb.RampProbeStrategy{Type: cb.RampExponential, Start: 1, Step: 1}, cb.ProbeContext{MaxRequests: 16, ConsecutiveSuccesses: 0}, 1},
		{"exp n=1", cb.RampProbeStrategy{Type: cb.RampExponential, Start: 1, Step: 1}, cb.ProbeContext{MaxRequests: 16, ConsecutiveSuccesses: 1}, 2},
		{"exp n=3", cb.RampProbeStrategy{Type: cb.RampExponential, Start: 1, Step: 1}, cb.ProbeContext{MaxRequests: 16, ConsecutiveSuccesses: 3}, 8},
		{"exp step2 n=4", cb.RampProbeStrategy{Type: cb.RampExponential, Start: 1, Step: 2}, cb.ProbeContext{MaxRequests: 16, ConsecutiveSuccesses: 4}, 4},
		{"exp clamp", cb.RampProbeStrategy{Type: cb.RampExponential, Start: 1, Step: 1}, cb.ProbeContext{MaxRequests: 4, ConsecutiveSuccesses: 10}, 4},

		// Non-positive start/step normalized to 1 (zero value acts as linear 1,1).
		{"zero value", cb.RampProbeStrategy{}, cb.ProbeContext{MaxRequests: 8, ConsecutiveSuccesses: 2}, 3},
		{"max<1 floors to 1", cb.RampProbeStrategy{Type: cb.RampLinear, Start: 1, Step: 1}, cb.ProbeContext{MaxRequests: 0, ConsecutiveSuccesses: 5}, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.s.MaxConcurrentProbes(tt.pc); got != tt.want {
				t.Errorf("MaxConcurrentProbes = %d, want %d", got, tt.want)
			}
		})
	}
}

// TestRampProbeStrategy_MinInterval verifies time-based pacing: before
// MinInterval elapses the allowance is pinned at Start even with successes.
func TestRampProbeStrategy_MinInterval(t *testing.T) {
	s := cb.RampProbeStrategy{Type: cb.RampLinear, Start: 1, Step: 1, MinInterval: time.Second}
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	// Successes accrued but only 500ms elapsed → pinned at Start=1.
	pcEarly := cb.ProbeContext{MaxRequests: 8, ConsecutiveSuccesses: 5, HalfOpenSince: base, Now: base.Add(500 * time.Millisecond)}
	if got := s.MaxConcurrentProbes(pcEarly); got != 1 {
		t.Errorf("early allowance = %d, want 1 (pinned)", got)
	}
	// After the interval elapses the ramp applies.
	pcLate := cb.ProbeContext{MaxRequests: 8, ConsecutiveSuccesses: 5, HalfOpenSince: base, Now: base.Add(2 * time.Second)}
	if got := s.MaxConcurrentProbes(pcLate); got != 6 {
		t.Errorf("late allowance = %d, want 6", got)
	}
}

// TestNewRampProbeStrategy_Normalizes verifies the constructor floors start/step.
func TestNewRampProbeStrategy_Normalizes(t *testing.T) {
	s := cb.NewRampProbeStrategy(cb.RampExponential, 0, -3, 0)
	if s.Start != 1 || s.Step != 1 {
		t.Errorf("got Start=%d Step=%d, want 1,1", s.Start, s.Step)
	}
	if s.Name() != "ramp-exponential" {
		t.Errorf("Name = %q, want ramp-exponential", s.Name())
	}
}

// openBreaker trips a fresh count-based breaker to Open and advances the clock
// past OpenTimeout so the next Execute enters half-open.
func openBreaker(t *testing.T, b *cb.CircuitBreaker, clk *clock.ManualClock, openTimeout time.Duration, failures int) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < failures; i++ {
		_ = b.Execute(ctx, fail)
	}
	if b.State() != cb.StateOpen {
		t.Fatalf("expected Open, got %s", b.State())
	}
	clk.Advance(openTimeout + time.Millisecond)
}

// TestHalfOpen_DefaultStrategy_MatchesLegacy verifies a nil HalfOpenStrategy
// reproduces the fixed-cap behavior exactly: with HalfOpenMaxRequests=1, one
// concurrent probe runs and a second concurrent probe is rejected.
func TestHalfOpen_DefaultStrategy_MatchesLegacy(t *testing.T) {
	clk := manualClock()
	b := cb.New(cb.Config{
		Name:                t.Name(),
		WindowType:          cb.CountBased,
		WindowSize:          5,
		FailureThreshold:    3,
		OpenTimeout:         100 * time.Millisecond,
		HalfOpenMaxRequests: 1,
		SuccessThreshold:    100, // stay half-open
		Clock:               clk,
	})
	openBreaker(t, b, clk, 100*time.Millisecond, 3)

	ctx := context.Background()
	release := make(chan struct{})
	started := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = b.Execute(ctx, func(context.Context) error {
			close(started)
			<-release
			return nil
		})
	}()
	<-started // first probe is executing and holds the only slot

	// Second concurrent probe must be rejected.
	err := b.Execute(ctx, succeed)
	if !cb.IsTooManyRequests(err) {
		t.Fatalf("second probe err = %v, want ErrTooManyRequests", err)
	}
	close(release)
	wg.Wait()
}

// TestHalfOpen_RampStrategy_AdmitsProgressively proves the linear ramp admits
// more concurrent probes as consecutive successes accrue, and that it never
// exceeds HalfOpenMaxRequests. It uses blocking probes so concurrency is
// controlled deterministically.
func TestHalfOpen_RampStrategy_AdmitsProgressively(t *testing.T) {
	clk := manualClock()
	const maxReq = 4
	b := cb.New(cb.Config{
		Name:                t.Name(),
		WindowType:          cb.CountBased,
		WindowSize:          10,
		FailureThreshold:    3,
		OpenTimeout:         100 * time.Millisecond,
		HalfOpenMaxRequests: maxReq,
		SuccessThreshold:    1000, // never close during the test
		HalfOpenStrategy:    cb.RampProbeStrategy{Type: cb.RampLinear, Start: 1, Step: 1},
		Clock:               clk,
	})
	openBreaker(t, b, clk, 100*time.Millisecond, 3)
	ctx := context.Background()

	// launchBlocking starts a probe that signals admitted once it is running and
	// then blocks until release is closed. This lets the test hold a precise
	// number of probes concurrently in flight.
	launchBlocking := func(release <-chan struct{}, admitted chan<- struct{}) {
		go func() {
			_ = b.Execute(ctx, func(context.Context) error {
				admitted <- struct{}{}
				<-release
				return nil
			})
		}()
	}
	waitInflightZero := func() {
		for b.HalfOpenInflight() != 0 {
			time.Sleep(time.Millisecond)
		}
	}

	// Monotonic sweep. consecutiveSuccesses starts at 0 (allowance 1). At each
	// step the current allowance is min(1+successes, maxReq) for linear
	// start=1,step=1. We fill exactly that many concurrent blocking probes,
	// assert one more is rejected, then release them. Each released probe
	// succeeds, so consecutiveSuccesses grows by `wantAllowance`, advancing the
	// ramp for the next step — proving progressive admission end to end.
	successes := int64(0)
	for step := 0; step < 4; step++ {
		wantAllowance := int(successes) + 1
		if wantAllowance > maxReq {
			wantAllowance = maxReq
		}

		release := make(chan struct{})
		admitted := make(chan struct{}, wantAllowance)
		for i := 0; i < wantAllowance; i++ {
			launchBlocking(release, admitted)
			<-admitted // this probe is now actually running
		}
		if got := b.HalfOpenInflight(); got != int64(wantAllowance) {
			t.Fatalf("step=%d successes=%d inflight=%d, want %d", step, successes, got, wantAllowance)
		}
		// Allowance is saturated: one more concurrent probe must be rejected.
		if err := b.Execute(ctx, succeed); !cb.IsTooManyRequests(err) {
			t.Fatalf("step=%d: extra probe err=%v, want ErrTooManyRequests", step, err)
		}

		close(release)
		waitInflightZero()
		successes += int64(wantAllowance)

		if b.State() != cb.StateHalfOpen {
			t.Fatalf("step=%d expected still HalfOpen, got %s", step, b.State())
		}
	}
}

// TestHalfOpen_RampStrategy_ClosesOnThreshold verifies that with a ramp strategy
// the circuit still closes after SuccessThreshold consecutive successes.
func TestHalfOpen_RampStrategy_ClosesOnThreshold(t *testing.T) {
	clk := manualClock()
	b := cb.New(cb.Config{
		Name:                t.Name(),
		WindowType:          cb.CountBased,
		WindowSize:          10,
		FailureThreshold:    3,
		OpenTimeout:         100 * time.Millisecond,
		HalfOpenMaxRequests: 4,
		SuccessThreshold:    3,
		HalfOpenStrategy:    cb.NewRampProbeStrategy(cb.RampExponential, 1, 1, 0),
		Clock:               clk,
	})
	openBreaker(t, b, clk, 100*time.Millisecond, 3)
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if err := b.Execute(ctx, succeed); err != nil {
			t.Fatalf("probe %d err=%v", i, err)
		}
		if b.State() != cb.StateHalfOpen {
			t.Fatalf("after %d successes expected HalfOpen, got %s", i+1, b.State())
		}
	}
	// Third consecutive success closes the circuit.
	if err := b.Execute(ctx, succeed); err != nil {
		t.Fatalf("closing probe err=%v", err)
	}
	if b.State() != cb.StateClosed {
		t.Fatalf("expected Closed after 3 successes, got %s", b.State())
	}
}

// TestHalfOpen_RampStrategy_FailureReopens verifies a failed probe reopens the
// circuit under a ramp strategy just like the fixed default.
func TestHalfOpen_RampStrategy_FailureReopens(t *testing.T) {
	clk := manualClock()
	b := cb.New(cb.Config{
		Name:                t.Name(),
		WindowType:          cb.CountBased,
		WindowSize:          10,
		FailureThreshold:    3,
		OpenTimeout:         100 * time.Millisecond,
		HalfOpenMaxRequests: 4,
		SuccessThreshold:    5,
		HalfOpenStrategy:    cb.RampProbeStrategy{Type: cb.RampLinear, Start: 1, Step: 1},
		Clock:               clk,
	})
	openBreaker(t, b, clk, 100*time.Millisecond, 3)
	ctx := context.Background()
	if err := b.Execute(ctx, fail); err == nil {
		t.Fatal("expected the probe's own error")
	}
	if b.State() != cb.StateOpen {
		t.Fatalf("expected Open after probe failure, got %s", b.State())
	}
}

// TestHalfOpen_RampStrategy_ConcurrentAdmission is the -race concurrency test:
// many goroutines hammer a half-open breaker while probes complete, and the
// invariant HalfOpenInflight() in [0, HalfOpenMaxRequests] must always hold.
func TestHalfOpen_RampStrategy_ConcurrentAdmission(t *testing.T) {
	clk := manualClock()
	const maxReq = 6
	var maxSeen atomic.Int64
	b := cb.New(cb.Config{
		Name:                t.Name(),
		WindowType:          cb.CountBased,
		WindowSize:          50,
		FailureThreshold:    3,
		OpenTimeout:         100 * time.Millisecond,
		HalfOpenMaxRequests: maxReq,
		SuccessThreshold:    1 << 30, // never close: keep exercising half-open
		HalfOpenStrategy:    cb.RampProbeStrategy{Type: cb.RampExponential, Start: 1, Step: 1},
		Clock:               clk,
	})
	openBreaker(t, b, clk, 100*time.Millisecond, 3)
	ctx := context.Background()

	var wg sync.WaitGroup
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				_ = b.Execute(ctx, func(context.Context) error {
					if v := b.HalfOpenInflight(); v > maxSeen.Load() {
						maxSeen.Store(v)
					}
					return nil
				})
				if v := b.HalfOpenInflight(); v < 0 || v > maxReq {
					t.Errorf("inflight invariant violated: %d", v)
					return
				}
			}
		}()
	}
	wg.Wait()

	if got := b.HalfOpenInflight(); got != 0 {
		t.Errorf("final inflight = %d, want 0", got)
	}
	if maxSeen.Load() > maxReq {
		t.Errorf("max concurrent probes seen = %d, exceeds cap %d", maxSeen.Load(), maxReq)
	}
}
