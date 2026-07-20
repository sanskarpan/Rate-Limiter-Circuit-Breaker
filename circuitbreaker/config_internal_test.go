package circuitbreaker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/metric"
)

var errTestConfig = errors.New("config test error")

// TestConfig_ZeroValue_Defaults asserts that every unset field of a zero-value
// Config normalizes to its documented default (§2.7). This locks the zero-value
// contract: circuitbreaker.New(circuitbreaker.Config{}) must yield a working,
// count-based breaker with the defaults promised in the Config field godoc.
func TestConfig_ZeroValue_Defaults(t *testing.T) {
	var c Config
	c.defaults()

	if c.Clock == nil {
		t.Fatal("Clock default not applied")
	}
	if _, ok := c.Clock.(clock.RealClock); !ok {
		t.Fatalf("Clock default = %T, want clock.RealClock", c.Clock)
	}
	if c.WindowSize != 10 {
		t.Fatalf("WindowSize default = %d, want 10", c.WindowSize)
	}
	if c.FailureThreshold != 5 {
		t.Fatalf("FailureThreshold default = %d, want 5", c.FailureThreshold)
	}
	if c.WindowDuration != 60*time.Second {
		t.Fatalf("WindowDuration default = %v, want 60s", c.WindowDuration)
	}
	if c.BucketDuration != time.Second {
		t.Fatalf("BucketDuration default = %v, want 1s", c.BucketDuration)
	}
	if c.FailureRateThreshold != 0.5 {
		t.Fatalf("FailureRateThreshold default = %v, want 0.5", c.FailureRateThreshold)
	}
	if c.MinimumRequests != 10 {
		t.Fatalf("MinimumRequests default = %d, want 10", c.MinimumRequests)
	}
	if c.OpenTimeout != 30*time.Second {
		t.Fatalf("OpenTimeout default = %v, want 30s", c.OpenTimeout)
	}
	if c.HalfOpenMaxRequests != 1 {
		t.Fatalf("HalfOpenMaxRequests default = %d, want 1", c.HalfOpenMaxRequests)
	}
	if c.SuccessThreshold != 1 {
		t.Fatalf("SuccessThreshold default = %d, want 1", c.SuccessThreshold)
	}
	if c.IsFailure == nil {
		t.Fatal("IsFailure default not applied")
	}
	// Default IsFailure counts every non-nil error and ignores nil.
	if c.IsFailure(nil) {
		t.Fatal("default IsFailure(nil) = true, want false")
	}
	if !c.IsFailure(errTestConfig) {
		t.Fatal("default IsFailure(err) = false, want true")
	}
	if c.Recorder == nil {
		t.Fatal("Recorder default not applied")
	}
	if c.Recorder != metric.Default() {
		t.Fatalf("Recorder default = %v, want metric.Default()", c.Recorder)
	}
	// WindowType zero value is CountBased (iota 0) and is a valid working mode.
	if c.WindowType != CountBased {
		t.Fatalf("zero WindowType = %v, want CountBased", c.WindowType)
	}
}

// TestNew_ZeroValueConfig_Works asserts the end-to-end zero-value contract: a
// breaker built from Config{} executes, tracks failures with the default
// count-based window, and trips at the default FailureThreshold of 5 (§2.7).
func TestNew_ZeroValueConfig_Works(t *testing.T) {
	cb := New(Config{})
	if got := cb.State(); got != StateClosed {
		t.Fatalf("fresh zero-value breaker state = %v, want Closed", got)
	}
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		_ = cb.Execute(ctx, func(context.Context) error { return errTestConfig })
	}
	if got := cb.State(); got != StateOpen {
		t.Fatalf("zero-value breaker did not trip at default FailureThreshold=5, state=%v", got)
	}
}
