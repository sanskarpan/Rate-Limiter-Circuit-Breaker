// Package adaptive export shims for white-box testing.
// This file is compiled only during `go test`; it does NOT widen the
// production API surface.
package adaptive

// ForceAdjust exposes forceAdjust to external (_test) test packages.
func (al *AdaptiveLimiter) ForceAdjust() { al.forceAdjust() }
