// Package metric defines the zero-dependency Recorder interface that the core
// resilience packages use to emit observability signals (rate-limit decisions,
// circuit-breaker state, bulkhead saturation). The core imports ONLY this
// package (stdlib-only), preserving the zero-external-dependency guarantee;
// concrete adapters (Prometheus, OpenTelemetry) live in sub-packages that are
// free to import heavy deps.
//
// Labels are intentionally BOUNDED: methods take an algorithm/breaker name, never
// a per-request key, to avoid unbounded metric cardinality (a cost/DoS risk).
package metric

import "time"

// Recorder receives observability events from the core resilience components.
// All methods must be safe for concurrent use and cheap on the hot path.
type Recorder interface {
	// Rate limiter decisions.
	IncAllowed(algorithm string)
	IncDenied(algorithm string)
	ObserveDecision(algorithm string, d time.Duration)

	// Circuit breaker.
	RecordCBState(name, state string) // state: "closed" | "half-open" | "open"
	IncCBResult(name, result string)  // result: "success" | "failure" | "rejected"
	ObserveCBExecution(name string, d time.Duration)
	IncCBTransition(name, from, to string)

	// Bulkhead saturation.
	SetBulkheadInflight(name string, n int)
	IncBulkheadRejected(name string)
}

// Nop is a Recorder that discards everything. It is the default so the core
// stays observability-agnostic and allocation-free when no adapter is wired.
type Nop struct{}

func (Nop) IncAllowed(string)                        {}
func (Nop) IncDenied(string)                         {}
func (Nop) ObserveDecision(string, time.Duration)    {}
func (Nop) RecordCBState(string, string)             {}
func (Nop) IncCBResult(string, string)               {}
func (Nop) ObserveCBExecution(string, time.Duration) {}
func (Nop) IncCBTransition(string, string, string)   {}
func (Nop) SetBulkheadInflight(string, int)          {}
func (Nop) IncBulkheadRejected(string)               {}

// nop is a shared instance for defaults.
var nop Recorder = Nop{}

// Default returns a shared no-op Recorder, used as the zero value for options.
func Default() Recorder { return nop }
