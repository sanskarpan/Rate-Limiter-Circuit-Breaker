package circuitbreaker

import (
	"time"

	"github.com/sanskarpan/resilience/internal/clock"
)

// Config configures a CircuitBreaker.
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

	// Clock is the time source (override for testing).
	Clock clock.Clock
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
}

// defaultIsFailure counts all non-nil errors as failures.
// Context cancellation is NOT counted by default per the spec.
func defaultIsFailure(err error) bool {
	if err == nil {
		return false
	}
	return true
}
