package circuitbreaker

import (
	"errors"
	"time"
)

// ErrCircuitOpen is returned when the circuit is open and a request is rejected.
// Use errors.Is to check for this error.
var ErrCircuitOpen = errors.New("circuit breaker is open")

// ErrTooManyRequests is returned when HalfOpen and inflight >= HalfOpenMaxRequests.
var ErrTooManyRequests = errors.New("circuit breaker: too many requests in half-open state")

// CircuitError wraps a circuit breaker rejection with structured, inspectable
// context. It carries the breaker Name, the State observed at rejection time,
// and, when the circuit is open, the estimated TimeUntilHalfOpen remaining
// before the breaker will admit its next probe.
//
// CircuitError implements errors.Is against the ErrCircuitOpen and
// ErrTooManyRequests sentinels (via the wrapped Err) and errors.As so callers
// can recover the full struct:
//
//	var ce *circuitbreaker.CircuitError
//	if errors.As(err, &ce) {
//		log.Printf("rejected by %s in state %s, retry after %s",
//			ce.GetName(), ce.CircuitState(), ce.RetryAfter())
//	}
//
// Prefer the IsOpen / IsTooManyRequests predicates for simple classification.
type CircuitError struct {
	// Name is the circuit breaker's name.
	Name string
	// State is the breaker state observed at the moment of rejection.
	State State
	// TimeUntilHalfOpen is the estimated duration until the open circuit will
	// next admit a half-open probe. It is only meaningful when Err is
	// ErrCircuitOpen (State == StateOpen); it is zero otherwise and may be zero
	// for an open circuit whose OpenTimeout has already elapsed.
	TimeUntilHalfOpen time.Duration
	// Err is the wrapped sentinel (ErrCircuitOpen or ErrTooManyRequests).
	Err error
}

// Error implements the error interface.
func (e *CircuitError) Error() string {
	return e.Err.Error() + " [circuit=" + e.Name + ", state=" + e.State.String() + "]"
}

// Is reports whether target is one of the circuit breaker sentinels wrapped by
// this error. This keeps errors.Is(err, ErrCircuitOpen) and
// errors.Is(err, ErrTooManyRequests) working against the concrete Err.
func (e *CircuitError) Is(target error) bool {
	return target == e.Err
}

// Unwrap returns the wrapped sentinel error so errors.Is/As traverse the chain.
func (e *CircuitError) Unwrap() error {
	return e.Err
}

// GetName returns the name of the circuit breaker that produced the rejection.
func (e *CircuitError) GetName() string {
	return e.Name
}

// CircuitState returns the breaker state observed at the moment of rejection.
func (e *CircuitError) CircuitState() State {
	return e.State
}

// RetryAfter returns the estimated duration until the open circuit will next
// admit a probe (transition to half-open). It returns 0 when the breaker is not
// open or when the open timeout has already elapsed. Callers may use it as a
// hint for a Retry-After header or backoff delay.
func (e *CircuitError) RetryAfter() time.Duration {
	if e.TimeUntilHalfOpen < 0 {
		return 0
	}
	return e.TimeUntilHalfOpen
}

// IsOpen reports whether err was caused by the circuit being open
// (equivalent to errors.Is(err, ErrCircuitOpen)).
func IsOpen(err error) bool {
	return errors.Is(err, ErrCircuitOpen)
}

// IsTooManyRequests reports whether err was caused by the half-open probe limit
// being reached (equivalent to errors.Is(err, ErrTooManyRequests)).
func IsTooManyRequests(err error) bool {
	return errors.Is(err, ErrTooManyRequests)
}

// AsCircuitError extracts the *CircuitError from err's chain via errors.As.
// It returns the error and true on success, or nil and false if no
// *CircuitError is present.
func AsCircuitError(err error) (*CircuitError, bool) {
	var ce *CircuitError
	if errors.As(err, &ce) {
		return ce, true
	}
	return nil, false
}
