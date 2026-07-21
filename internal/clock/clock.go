// Package clock is a backward-compatibility shim that re-exports the public
// clock package. All types and constructors are type aliases or thin wrappers
// so existing internal importers continue to compile unchanged.
//
// New code should import "github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/clock"
// directly.
package clock

import (
	publicclock "github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/clock"
)

// Clock is the time source interface used by all rate limiting algorithms.
// Use RealClock in production and ManualClock in tests.
type Clock = publicclock.Clock

// Timer is the interface wrapping time.Timer.
type Timer = publicclock.Timer

// Ticker is the interface wrapping time.Ticker.
type Ticker = publicclock.Ticker

// RealClock is the production implementation using the stdlib time package.
type RealClock = publicclock.RealClock

// ManualClock is a test double whose time only advances when Advance() is called.
type ManualClock = publicclock.ManualClock

// NewManualClock creates a ManualClock starting at the given time.
var NewManualClock = publicclock.NewManualClock
