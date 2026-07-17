package circuitbreaker

import (
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
)

// TestL10_NewTimeWindow_ZeroBucketWidth_NoPanic calls newTimeWindow directly with
// a zero bucketWidth (bypassing Config.defaults) and asserts it neither panics
// nor produces a zero-bucket window.
func TestL10_NewTimeWindow_ZeroBucketWidth_NoPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("newTimeWindow panicked with zero bucketWidth: %v", r)
		}
	}()
	clk := clock.NewManualClock(time.Unix(0, 0))
	w := newTimeWindow(10*time.Second, 0, clk)
	if w.numBuckets < 1 {
		t.Fatalf("expected >=1 bucket, got %d", w.numBuckets)
	}
	// Must be usable.
	w.record(outcomeFailure)
	if f, r := w.counts(); f != 1 || r != 1 {
		t.Fatalf("expected (1,1), got (%d,%d)", f, r)
	}
}

// TestL10_NewTimeWindow_ZeroBoth_NoPanic covers both windowDuration and
// bucketWidth being non-positive.
func TestL10_NewTimeWindow_ZeroBoth_NoPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("newTimeWindow panicked with zero window and bucket: %v", r)
		}
	}()
	clk := clock.NewManualClock(time.Unix(0, 0))
	w := newTimeWindow(0, 0, clk)
	if w.numBuckets < 1 {
		t.Fatalf("expected >=1 bucket, got %d", w.numBuckets)
	}
	w.record(outcomeSuccess)
}
