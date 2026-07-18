package circuitbreaker

import (
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
)

// ProbeContext is the read-only view a HalfOpenStrategy receives when deciding
// whether to admit the next half-open probe. It is constructed by the breaker
// for each admission attempt and is safe to inspect but must not be retained.
type ProbeContext struct {
	// Inflight is the number of probe requests currently executing in the
	// half-open state (not counting the probe being decided).
	Inflight int64

	// ConsecutiveSuccesses is the number of consecutive successful probes
	// observed since the circuit entered the current half-open episode. It grows
	// as probes succeed and is what ramp strategies key off of.
	ConsecutiveSuccesses int64

	// HalfOpenSince is the time at which the circuit entered the current
	// half-open episode. Combined with Now it lets time-based strategies reason
	// about elapsed recovery time.
	HalfOpenSince time.Time

	// Now is the current time as read from the breaker's injected clock. Always
	// use this rather than time.Now() so strategies remain deterministic under
	// the ManualClock used in tests.
	Now time.Time

	// MaxRequests is the configured HalfOpenMaxRequests ceiling. A strategy's
	// dynamic allowance should never exceed this value, and the breaker also
	// clamps the returned allowance to it as a safety net.
	MaxRequests int
}

// HalfOpenStrategy decides how many concurrent probe requests the circuit
// breaker admits while in the half-open state. It lets callers choose between
// the default fixed concurrent-probe cap and gradual-ramp / probe-budget
// schemes that admit an increasing amount of traffic as recovery progresses.
//
// Implementations must be safe for concurrent use: MaxConcurrentProbes may be
// called from many goroutines simultaneously during half-open admission. They
// should be deterministic given their ProbeContext so they remain testable
// under an injected clock.
type HalfOpenStrategy interface {
	// MaxConcurrentProbes returns the maximum number of probes that may be
	// in flight concurrently at this instant, given the supplied ProbeContext.
	// The breaker admits a probe only while the current inflight count is below
	// this value. Returning a value <= 0 rejects all probes; the breaker clamps
	// the result to at most pc.MaxRequests.
	MaxConcurrentProbes(pc ProbeContext) int

	// Name returns a short identifier for the strategy, used in diagnostics.
	Name() string
}

// FixedProbeStrategy is the default half-open strategy and reproduces the
// legacy behavior exactly: it admits up to HalfOpenMaxRequests concurrent
// probes at all times, regardless of how recovery is progressing. The circuit
// still closes once SuccessThreshold consecutive successes accrue.
//
// The zero value is a valid FixedProbeStrategy. A nil Config.HalfOpenStrategy is
// treated as FixedProbeStrategy{}, so existing configurations are unchanged.
type FixedProbeStrategy struct{}

// MaxConcurrentProbes always returns the configured HalfOpenMaxRequests ceiling,
// yielding the classic fixed concurrent-probe cap.
func (FixedProbeStrategy) MaxConcurrentProbes(pc ProbeContext) int {
	return pc.MaxRequests
}

// Name returns "fixed".
func (FixedProbeStrategy) Name() string { return "fixed" }

// RampType selects how a RampProbeStrategy grows its probe allowance as
// consecutive successes accrue.
type RampType int

const (
	// RampLinear grows the allowance linearly: the allowance after n consecutive
	// successes is Start + n*Step (clamped to HalfOpenMaxRequests).
	RampLinear RampType = iota

	// RampExponential doubles the allowance for every Step consecutive
	// successes: allowance = Start * 2^(n/Step) (clamped to HalfOpenMaxRequests).
	RampExponential
)

// String returns a human-readable name for the ramp type.
func (r RampType) String() string {
	switch r {
	case RampLinear:
		return "linear"
	case RampExponential:
		return "exponential"
	default:
		return "unknown"
	}
}

// RampProbeStrategy implements a gradual-ramp / probe-budget half-open strategy.
// Instead of admitting the full HalfOpenMaxRequests concurrency immediately, it
// starts by admitting a small number of concurrent probes and increases the
// allowance as consecutive probes succeed, up to HalfOpenMaxRequests. This lets
// a still-fragile dependency recover faster than a single probe while avoiding
// slamming it with full concurrency the instant the open timeout elapses.
//
// The allowance is a pure function of ProbeContext.ConsecutiveSuccesses, so it
// is fully deterministic and testable. An optional MinInterval further paces the
// ramp by time: the allowance is only permitted to exceed Start once at least
// MinInterval has elapsed since the half-open episode began (using the injected
// clock via ProbeContext.Now), giving a slow-start pacing in addition to the
// success-driven growth.
//
// Construct one with NewRampProbeStrategy for validated defaults, or use a zero
// value (which behaves like a linear ramp of Start=1, Step=1).
type RampProbeStrategy struct {
	// Type selects linear vs exponential growth. The zero value is RampLinear.
	Type RampType

	// Start is the initial concurrent-probe allowance at the start of a
	// half-open episode (before any successes). Values <= 0 are treated as 1.
	Start int

	// Step controls growth speed. For RampLinear it is the number of extra
	// probes allowed per consecutive success. For RampExponential it is the
	// number of consecutive successes required to double the allowance. Values
	// <= 0 are treated as 1.
	Step int

	// MinInterval, if > 0, is the minimum time that must elapse since the
	// half-open episode began before the allowance may grow beyond Start. This
	// paces the ramp by wall-clock time in addition to success count. Zero
	// disables time-based pacing.
	MinInterval time.Duration
}

// NewRampProbeStrategy returns a RampProbeStrategy with the given parameters,
// normalizing non-positive start/step to 1. It is a small ergonomic helper; the
// struct may also be constructed directly.
func NewRampProbeStrategy(t RampType, start, step int, minInterval time.Duration) RampProbeStrategy {
	if start <= 0 {
		start = 1
	}
	if step <= 0 {
		step = 1
	}
	return RampProbeStrategy{Type: t, Start: start, Step: step, MinInterval: minInterval}
}

// MaxConcurrentProbes computes the current concurrent-probe allowance from the
// consecutive-success count (and, if MinInterval is set, elapsed time), clamped
// to [1, HalfOpenMaxRequests].
func (s RampProbeStrategy) MaxConcurrentProbes(pc ProbeContext) int {
	start := s.Start
	if start <= 0 {
		start = 1
	}
	step := s.Step
	if step <= 0 {
		step = 1
	}

	ceiling := pc.MaxRequests
	if ceiling < 1 {
		ceiling = 1
	}

	// Time-based pacing: until MinInterval has elapsed since half-open began, the
	// allowance is pinned at Start regardless of successes.
	if s.MinInterval > 0 && !pc.HalfOpenSince.IsZero() {
		if pc.Now.Sub(pc.HalfOpenSince) < s.MinInterval {
			return clampProbe(start, ceiling)
		}
	}

	n := pc.ConsecutiveSuccesses
	if n < 0 {
		n = 0
	}

	var allowance int
	switch s.Type {
	case RampExponential:
		// Double for every `step` consecutive successes: start * 2^(n/step).
		doublings := n / int64(step)
		allowance = start
		for i := int64(0); i < doublings && allowance < ceiling; i++ {
			allowance *= 2
		}
	default: // RampLinear
		grown := int64(start) + n*int64(step)
		if grown > int64(ceiling) {
			grown = int64(ceiling)
		}
		allowance = int(grown)
	}

	return clampProbe(allowance, ceiling)
}

// Name returns "ramp-" plus the ramp type, e.g. "ramp-linear".
func (s RampProbeStrategy) Name() string { return "ramp-" + s.Type.String() }

// clampProbe bounds v to [1, ceiling].
func clampProbe(v, ceiling int) int {
	if v < 1 {
		return 1
	}
	if v > ceiling {
		return ceiling
	}
	return v
}

// halfOpenStrategy returns the effective strategy, defaulting a nil config value
// to the legacy FixedProbeStrategy.
func (cb *CircuitBreaker) halfOpenStrategy() HalfOpenStrategy {
	if cb.cfg.HalfOpenStrategy == nil {
		return FixedProbeStrategy{}
	}
	return cb.cfg.HalfOpenStrategy
}

// probeContext builds the ProbeContext for the current admission attempt.
// inflight is the current in-flight probe count read by the caller. clk is the
// breaker's injected clock.
func (cb *CircuitBreaker) probeContext(inflight int64, clk clock.Clock) ProbeContext {
	var since time.Time
	if ns := cb.halfOpenAt.Load(); ns > 0 {
		since = time.Unix(0, ns)
	}
	return ProbeContext{
		Inflight:             inflight,
		ConsecutiveSuccesses: cb.consecutiveSuccesses.Load(),
		HalfOpenSince:        since,
		Now:                  clk.Now(),
		MaxRequests:          cb.cfg.HalfOpenMaxRequests,
	}
}
