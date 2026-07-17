package circuitbreaker

import "errors"

// ErrCircuitOpen is returned when the circuit is open and a request is rejected.
// Use errors.Is to check for this error.
var ErrCircuitOpen = errors.New("circuit breaker is open")

// ErrTooManyRequests is returned when HalfOpen and inflight >= HalfOpenMaxRequests.
var ErrTooManyRequests = errors.New("circuit breaker: too many requests in half-open state")

// CircuitError wraps a circuit breaker error with context.
type CircuitError struct {
	Name  string
	State State
	Err   error
}

func (e *CircuitError) Error() string {
	return e.Err.Error() + " [circuit=" + e.Name + ", state=" + e.State.String() + "]"
}

func (e *CircuitError) Is(target error) bool {
	return target == ErrCircuitOpen || target == ErrTooManyRequests
}

func (e *CircuitError) Unwrap() error {
	return e.Err
}
