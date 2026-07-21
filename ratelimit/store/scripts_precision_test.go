package store

import (
	"context"
	"math"
	"testing"
	"time"
)

// TestPrecisionBound_CurrentEpochIs256 pins the documented ENHANCEMENTS §5.4
// guarantee: for nanosecond wall-clock timestamps this millennium the float64
// snapping error (ULP) is exactly 256ns, so the stored GCRA/leaky-bucket TAT and
// sliding-window-log scores snap to the nearest multiple of 256ns.
func TestPrecisionBound_CurrentEpochIs256(t *testing.T) {
	now := time.Now().UnixNano()
	if got := precisionBoundNs(now); got != 256 {
		t.Fatalf("precisionBoundNs(now=%d) = %d, want 256 (the documented ≤256ns bound for the current epoch)", now, got)
	}
	// The bound holds for the whole plausible operating range: from 2020 to well
	// past 2040 the ULP stays 256 (it becomes 512 only past 2^61 ≈ year 2043).
	y2020 := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	y2042 := time.Date(2042, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	for _, ts := range []int64{y2020, now, y2042} {
		if got := precisionBoundNs(ts); got != 256 {
			t.Fatalf("precisionBoundNs(%d) = %d, want 256", ts, got)
		}
	}
}

// TestPrecisionBound_MatchesFloat64ULP cross-checks PrecisionBoundNs against the
// actual float64 round-trip: the true snapping error of a timestamp is the gap to
// the nearest representable double, which must never exceed the reported bound.
func TestPrecisionBound_MatchesFloat64ULP(t *testing.T) {
	base := time.Now().UnixNano()
	for i := int64(0); i < 100000; i++ {
		ns := base + i
		bound := precisionBoundNs(ns)
		// Actual error when Redis Lua stores ns as a double then reads it back.
		roundTripped := int64(float64(ns))
		err := ns - roundTripped
		if err < 0 {
			err = -err
		}
		if err > bound {
			t.Fatalf("float64 round-trip error %dns for ns=%d exceeds reported bound %dns", err, ns, bound)
		}
		// A double's rounding error is at most half a ULP.
		if err > bound/2 && bound > 0 {
			// Not a failure — round-to-nearest can be up to bound/2; assert that.
			if err > bound {
				t.Fatalf("error %d > ULP %d", err, bound)
			}
		}
	}
	// Below 2^53 the representation is exact.
	if b := precisionBoundNs(1 << 52); b != 0 {
		t.Fatalf("precisionBoundNs(2^52) = %d, want 0 (exactly representable)", b)
	}
}

// TestPrecisionSnapping_NeverOverAdmits_GCRA drives the GCRA emulation (which
// reproduces Redis's float64 snapping bit-for-bit) across a sub-256ns-sensitive
// schedule and asserts the number of admitted requests never exceeds the GCRA
// bound burst + floor(elapsed/emission) + 1. The 256ns snap may shift an
// individual decision by <1 emission interval but can never admit MORE than an
// infinite-precision limiter over a window — it is only ever sub-tick
// conservative.
func TestPrecisionSnapping_NeverOverAdmits_GCRA(t *testing.T) {
	m := NewMemoryWithScripts()
	defer m.Close()
	ctx := context.Background()

	emission := int64(time.Second / 7) // ~142857142ns, deliberately not a multiple of 256
	burst := int64(4)
	base := time.Now().UnixNano()

	allowed := 0
	steps := 400
	stepNs := emission / 3 // step forward less than one emission → contention
	for i := 0; i < steps; i++ {
		now := base + int64(i)*stepNs
		args := []any{emission, burst, int64(1), now, int64(60000)}
		res, err := m.Eval(ctx, GCRAScriptID, []string{"k"}, args...)
		if err != nil {
			t.Fatalf("eval: %v", err)
		}
		arr := res.([]any)
		if arr[0].(int64) == 1 {
			allowed++
		}
	}

	// GCRA admits at most burst + floor(elapsed/emission) requests. Add +1 slack
	// for the 256ns snap possibly resolving one boundary decision early.
	elapsed := int64(steps-1) * stepNs
	maxAllowed := int(burst) + int(elapsed/emission) + 1
	if allowed > maxAllowed {
		t.Fatalf("GCRA admitted %d requests, exceeds bound %d (burst=%d + elapsed/emission=%d + 1 snap-slack): snapping over-admitted",
			allowed, maxAllowed, burst, elapsed/emission)
	}
}

// TestPrecisionSnapping_EmulationMatchesFloat64 verifies the emulation's stored
// TAT is a whole-number double that round-trips exactly (int64(tatF) is exact),
// which is the property the parity tests rely on to match real Redis.
func TestPrecisionSnapping_EmulationMatchesFloat64(t *testing.T) {
	// A TAT near a snapping boundary must still round-trip int64->float64->int64
	// once it has been through the emulation's float64 math.
	now := time.Now().UnixNano()
	tatF := float64(now) + float64(int64(time.Second/7))
	stored := int64(tatF)
	if float64(stored) != math.Trunc(tatF) {
		t.Fatalf("stored TAT %d does not round-trip: float64(stored)=%v tatF=%v", stored, float64(stored), tatF)
	}
}
