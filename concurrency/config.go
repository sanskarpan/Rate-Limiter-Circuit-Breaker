// Package concurrency provides an adaptive concurrency limiter in the style of
// Netflix's concurrency-limits library. Where a rate limiter caps the request
// *rate*, a concurrency limiter caps the amount of *in-flight work* and adapts
// that cap from measured round-trip latency (RTT) and drops. This is the single
// most effective defence against latency-induced overload: as a dependency slows
// down, queueing delay rises, and the limiter shrinks the number of permitted
// concurrent requests, shedding load before the dependency collapses.
//
// The limiter is split into two pieces:
//
//   - Limiter: the fast, allocation-light admission gate. Acquire is
//     non-blocking and returns ok=false once inflight reaches the current limit.
//     Wait is a bounded-blocking variant. Both hand back a ReleaseFunc that must
//     be called exactly once with the request Outcome.
//
//   - LimitStrategy: the pluggable control loop that consumes each Outcome and
//     recomputes the limit. Three strategies are provided — AIMD, Gradient2 and
//     Vegas — all of which clamp to [MinLimit, MaxLimit], guard against
//     divide-by-zero/NaN, and track a decaying windowed minimum RTT (the
//     baseRTT) so they adapt to shifting latency baselines.
//
// Basic usage:
//
//	lim := concurrency.NewGradient2(concurrency.Config{
//		InitialLimit: 20, MaxLimit: 1000, MinLimit: 4, RTTTolerance: 1.5,
//	})
//	release, ok := lim.Acquire(ctx)
//	if !ok {
//		// shed: at the current concurrency limit
//		return errBusy
//	}
//	defer release(concurrency.Outcome{RTT: measured, Dropped: false})
package concurrency

import (
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
)

// Default configuration values. These mirror the sensible defaults used by
// Netflix concurrency-limits and are applied by normalise for any zero field.
const (
	defaultInitialLimit = 20
	defaultMaxLimit     = 1000
	defaultMinLimit     = 4
	defaultRTTTolerance = 1.5

	// defaultBaseRTTDecay is how quickly the measured minimum RTT (baseRTT) is
	// allowed to drift upward when the true baseline shifts. Every this-many
	// samples the running minimum is nudged toward the recent observed RTT so a
	// permanently-elevated baseline does not pin the limiter shut forever.
	defaultBaseRTTDecayWindow = 600
)

// Config holds the tunables shared by every strategy plus the strategy-specific
// smoothing knob RTTTolerance. Any zero-valued field is replaced with its
// default by normalise, so a zero Config is valid.
type Config struct {
	// InitialLimit is the concurrency limit the limiter starts at.
	InitialLimit int
	// MaxLimit is the hard ceiling the adaptive limit may never exceed.
	MaxLimit int
	// MinLimit is the floor the adaptive limit may never drop below. A non-zero
	// floor is essential: it prevents the limiter deadlocking itself to zero
	// under sustained noisy latency.
	MinLimit int
	// RTTTolerance is the Vegas/Gradient smoothing knob. For Gradient2 and AIMD
	// it scales the latency threshold; for Vegas it seeds alpha/beta. Values
	// below 1 are clamped to 1 by the strategies that use it as a multiplier.
	RTTTolerance float64
}

// normalise returns a copy of cfg with defaults filled in and invariants
// enforced (MinLimit <= InitialLimit <= MaxLimit, positive limits).
func (c Config) normalise() Config {
	if c.InitialLimit <= 0 {
		c.InitialLimit = defaultInitialLimit
	}
	if c.MaxLimit <= 0 {
		c.MaxLimit = defaultMaxLimit
	}
	if c.MinLimit <= 0 {
		c.MinLimit = defaultMinLimit
	}
	if c.RTTTolerance <= 0 {
		c.RTTTolerance = defaultRTTTolerance
	}
	if c.MinLimit > c.MaxLimit {
		c.MinLimit = c.MaxLimit
	}
	if c.InitialLimit < c.MinLimit {
		c.InitialLimit = c.MinLimit
	}
	if c.InitialLimit > c.MaxLimit {
		c.InitialLimit = c.MaxLimit
	}
	return c
}

// options carries settings that are not part of the public Config struct but are
// wired through functional Option values (e.g. the test clock).
type options struct {
	clk clock.Clock
}

// Option customises a Limiter or strategy at construction time.
type Option func(*options)

// WithClock injects a clock.Clock. Use clock.NewManualClock in tests for fully
// deterministic behaviour; production code should leave this unset (RealClock).
func WithClock(clk clock.Clock) Option {
	return func(o *options) {
		if clk != nil {
			o.clk = clk
		}
	}
}

func newOptions(opts ...Option) options {
	o := options{clk: clock.RealClock{}}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// clamp constrains x to [lo, hi].
func clamp(x, lo, hi float64) float64 {
	if x < lo {
		return lo
	}
	if x > hi {
		return hi
	}
	return x
}

// safeRatio returns num/den, or fallback when den is non-positive or the result
// would be NaN/Inf. This is the single choke point for divide-by-zero safety.
func safeRatio(num, den, fallback float64) float64 {
	if den <= 0 {
		return fallback
	}
	r := num / den
	if r != r { // NaN
		return fallback
	}
	return r
}
