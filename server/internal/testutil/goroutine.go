// Package testutil provides testing helpers for the demo server module.
//
// The server is a separate Go module (github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/server),
// so it cannot import the root module's internal/testutil (Go's internal-package
// visibility rule forbids it across the module boundary). This is a small,
// self-contained copy of the goroutine leak checker used by server tests.
package testutil

import (
	"runtime"
	"testing"
	"time"
)

// LeakChecker records goroutine count before a test and verifies no net
// increase after. A difference of ±2 is tolerated for Go scheduler goroutines.
type LeakChecker struct {
	before int
	t      testing.TB
}

// NewLeakChecker creates a new LeakChecker, capturing the current goroutine count.
// Call Check() in a deferred statement at the start of your test.
//
// Usage:
//
//	func TestSomething(t *testing.T) {
//	    lc := testutil.NewLeakChecker(t)
//	    defer lc.Check()
//	    // ... test body ...
//	}
func NewLeakChecker(t testing.TB) *LeakChecker {
	t.Helper()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	return &LeakChecker{
		before: runtime.NumGoroutine(),
		t:      t,
	}
}

// Check verifies that the goroutine count has not grown by more than 2.
// This should be called via defer at the start of the test.
func (c *LeakChecker) Check() {
	c.t.Helper()
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	after := runtime.NumGoroutine()
	if diff := after - c.before; diff > 2 {
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		c.t.Errorf("goroutine leak detected: +%d goroutines (before=%d, after=%d). Stack:\n%s",
			diff, c.before, after, buf[:n])
	}
}
