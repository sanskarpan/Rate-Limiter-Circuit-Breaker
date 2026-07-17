package circuitbreaker

// State represents the circuit breaker's current state.
type State int32

const (
	// StateClosed means the circuit is operating normally. Requests pass through.
	StateClosed State = iota

	// StateHalfOpen means the circuit is testing if the service has recovered.
	// A limited number of probe requests are allowed through.
	StateHalfOpen

	// StateOpen means the circuit is tripped. Requests are rejected immediately.
	StateOpen
)

// String returns a human-readable name for the state.
func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// outcome is the result of a single request.
type outcome int8

const (
	outcomeSuccess outcome = iota
	outcomeFailure
)
