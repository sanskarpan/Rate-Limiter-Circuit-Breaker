package circuitbreaker

import (
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/metric"
)

// Config configures a CircuitBreaker.
//
// Zero value: the zero Config is valid. circuitbreaker.New(Config{}) returns a
// working count-based breaker; every unset field is normalised to its documented
// default by New (WindowType→CountBased, WindowSize 10, FailureThreshold 5,
// OpenTimeout 30s, HalfOpenMaxRequests 1, SuccessThreshold 1, Clock RealClock,
// Recorder metric.Default(), IsFailure = "all non-nil errors fail"). Set only the
// fields you want to override.
type Config struct {
	// Name is a human-readable identifier for this circuit breaker.
	Name string

	// WindowType determines how failures are tracked (CountBased or TimeBased).
	WindowType WindowType

	// --- Count-based settings ---

	// WindowSize is the number of requests tracked in the ring buffer (CountBased only).
	// Default: 10.
	WindowSize int

	// FailureThreshold is the number of failures required to open the circuit.
	// Default: 5.
	FailureThreshold int

	// --- Time-based settings ---

	// WindowDuration is the total time window for failure tracking (TimeBased only).
	// Default: 60 seconds.
	WindowDuration time.Duration

	// BucketDuration is the width of each time bucket (TimeBased only).
	// Default: 1 second.
	BucketDuration time.Duration

	// FailureRateThreshold is the minimum failure rate (0.0-1.0) to open the circuit.
	// Only used with TimeBased window. Default: 0.5 (50%).
	FailureRateThreshold float64

	// MinimumRequests is the minimum number of requests before evaluating failure rate.
	// Only used with TimeBased window. Default: 10.
	MinimumRequests int

	// --- Half-open settings ---

	// OpenTimeout is how long the circuit stays open before transitioning to half-open.
	// Default: 30 seconds.
	OpenTimeout time.Duration

	// HalfOpenMaxRequests is the maximum number of concurrent probe requests in half-open.
	// Default: 1.
	HalfOpenMaxRequests int

	// SuccessThreshold is how many consecutive successes in half-open closes the circuit.
	// Default: 1.
	SuccessThreshold int

	// HalfOpenStrategy selects how concurrent probe admission is paced while the
	// circuit is half-open. A nil value (the default) uses FixedProbeStrategy,
	// which admits up to HalfOpenMaxRequests concurrent probes at all times and
	// preserves the legacy behavior exactly. Use RampProbeStrategy for a
	// gradual-ramp / probe-budget scheme that admits an increasing amount of
	// traffic as consecutive probes succeed. The returned allowance is always
	// clamped to HalfOpenMaxRequests; the circuit still closes on
	// SuccessThreshold consecutive successes regardless of strategy.
	HalfOpenStrategy HalfOpenStrategy

	// --- Request handling ---

	// RequestTimeout limits how long a single Execute() call can take.
	// Zero means no timeout. A timeout counts as a failure.
	RequestTimeout time.Duration

	// IsFailure determines if an error counts as a circuit breaker failure.
	// nil = all errors count as failures.
	// Context cancellation (context.Canceled, context.DeadlineExceeded) should
	// generally NOT count unless caused by RequestTimeout.
	IsFailure func(err error) bool

	// --- Callbacks ---

	// OnStateChange is called when the state changes.
	OnStateChange func(name string, from, to State)

	// OnSuccess is called after each successful request.
	OnSuccess func(name string, duration time.Duration)

	// OnFailure is called after each failed request.
	OnFailure func(name string, duration time.Duration, err error)

	// OnRejected is called when a request is rejected (circuit open or half-open limit).
	OnRejected func(name string)

	// Clock is the time source used for timing open-state transitions and
	// request latency. The default (nil) uses clock.RealClock{}. Override
	// in tests with a ManualClock from the internal/clock package or any
	// value that satisfies the clock.Clock interface, which is importable
	// at "github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/clock".
	Clock clock.Clock

	// Recorder receives observability events (state gauge, result counters,
	// execution latency, transition counters). Defaults to metric.Default() (a
	// no-op) when nil, so the breaker stays observability-agnostic and
	// allocation-free unless an adapter is wired. Prefer WithRecorder to set it.
	Recorder metric.Recorder
}

// WithRecorder returns a copy of the Config with the metric.Recorder set. It is
// a small ergonomic helper for the common wiring pattern
//
//	cb := circuitbreaker.New(cfg.WithRecorder(rec))
//
// A nil recorder leaves the config unchanged (defaults apply).
func (c Config) WithRecorder(rec metric.Recorder) Config {
	if rec != nil {
		c.Recorder = rec
	}
	return c
}

// defaults applies default values for unset fields.
func (c *Config) defaults() {
	if c.Clock == nil {
		c.Clock = clock.RealClock{}
	}
	if c.WindowSize <= 0 {
		c.WindowSize = 10
	}
	if c.FailureThreshold <= 0 {
		c.FailureThreshold = 5
	}
	if c.WindowDuration <= 0 {
		c.WindowDuration = 60 * time.Second
	}
	if c.BucketDuration <= 0 {
		c.BucketDuration = time.Second
	}
	if c.FailureRateThreshold <= 0 {
		c.FailureRateThreshold = 0.5
	}
	if c.MinimumRequests <= 0 {
		c.MinimumRequests = 10
	}
	if c.OpenTimeout <= 0 {
		c.OpenTimeout = 30 * time.Second
	}
	if c.HalfOpenMaxRequests <= 0 {
		c.HalfOpenMaxRequests = 1
	}
	if c.SuccessThreshold <= 0 {
		c.SuccessThreshold = 1
	}
	if c.IsFailure == nil {
		c.IsFailure = defaultIsFailure
	}
	if c.Recorder == nil {
		c.Recorder = metric.Default()
	}
}

// defaultIsFailure counts all non-nil errors as failures.
// Context cancellation is NOT counted by default per the spec.
func defaultIsFailure(err error) bool {
	return err != nil
}
