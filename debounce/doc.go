// Package debounce provides lightweight debounce and throttle primitives for
// coalescing rapid function invocations, in the spirit of Lodash's debounce
// and throttle.
//
// # Debounce
//
// A Debouncer collapses a burst of rapid Trigger calls into a single invocation
// of the wrapped work. By default it fires on the trailing edge: the work runs
// once the caller has been quiet for the configured delay. Optional leading-edge
// firing runs the work immediately on the first call of a burst, and an optional
// max-wait guarantees the work runs at least once per that interval even under a
// sustained stream of calls that would otherwise reset the timer forever.
//
// Use a Debouncer when only the final event of a burst matters — for example,
// reacting to a settling stream of change notifications, config reloads, or
// "save on idle" behaviour.
//
// # Throttle
//
// A Throttler guarantees the wrapped work runs at most once per interval. The
// leading edge fires immediately; subsequent calls within the same interval are
// coalesced and, if trailing is enabled (the default), a single trailing call
// runs at the end of the interval to capture the most recent request.
//
// Use a Throttler to cap the rate of a repeated action — for example, limiting
// how often an expensive refresh or a progress update actually executes.
//
// # Determinism
//
// Both primitives take their time source from the internal clock package via the
// WithClock option, so their timing behaviour is fully testable with a
// ManualClock and never relies on time.Sleep. In production the default
// clock.RealClock is used.
//
// # Concurrency
//
// Debouncer and Throttler are safe for concurrent use by multiple goroutines.
// The wrapped work always runs on a separate goroutine (never on the caller's
// goroutine), so Trigger/Do never block on the work itself.
package debounce
