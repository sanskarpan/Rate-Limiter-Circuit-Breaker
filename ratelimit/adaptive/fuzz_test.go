package adaptive_test

import (
	"context"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/adaptive"
)

// FuzzAdaptive drives the adaptive limiter over fuzzed configuration and signal
// values, asserting invariants across interleaved Allow and adjust cycles:
//   - never panics on any (valid) input,
//   - Allowed implies Remaining >= 0,
//   - the effective CurrentLimit always stays within [minLimit, maxLimit],
//   - Algorithm is always "adaptive".
//
// A ManualClock is injected so adjustment cadence is deterministic and no
// background wall-clock timing influences the run.
func FuzzAdaptive(f *testing.F) {
	f.Add(100, 1, 1000, 10.0, 0.0, int64(10*time.Millisecond))
	f.Add(50, 5, 200, 90.0, 0.2, int64(time.Second))
	f.Add(10, 1, 10, 55.0, 0.02, int64(300*time.Millisecond))
	f.Add(1000, 100, 100000, 0.0, 0.0, int64(0))

	f.Fuzz(func(t *testing.T, initial, minLimit, maxLimit int, cpu, errRate float64, p99Ns int64) {
		// The constructor panics on invalid bounds by contract; keep the fuzzer to
		// the valid domain so we exercise behaviour, not the documented panics.
		if minLimit < 1 || minLimit > 1_000_000 {
			return
		}
		if maxLimit < minLimit || maxLimit > 1_000_000 {
			return
		}
		if initial < 0 || initial > 1_000_000 {
			return
		}
		// Clamp signal values into their documented domains.
		if cpu < 0 {
			cpu = 0
		}
		if cpu > 100 {
			cpu = 100
		}
		if errRate < 0 {
			errRate = 0
		}
		if errRate > 1 {
			errRate = 1
		}
		if p99Ns < 0 {
			p99Ns = -p99Ns
		}
		if p99Ns > int64(time.Hour) {
			p99Ns %= int64(time.Hour) + 1
		}

		signals := adaptive.NewStaticSignals(cpu, errRate, time.Duration(p99Ns))
		clk := clock.NewManualClock(time.Unix(0, 0))
		al := adaptive.New(initial, minLimit, maxLimit, signals, adaptive.WithClock(clk))
		defer al.Close() //nolint:errcheck

		ctx := context.Background()

		for step := 0; step < 8; step++ {
			res := al.Allow(ctx, "key")
			if res.Remaining < 0 {
				t.Errorf("step %d: Remaining=%d < 0", step, res.Remaining)
			}
			if res.Algorithm != "adaptive" {
				t.Errorf("step %d: Algorithm=%q, want adaptive", step, res.Algorithm)
			}
			// Drive an adjustment cycle, then verify the limit stayed in-band.
			al.ForceAdjust()
			cur := al.CurrentLimit()
			if cur < minLimit || cur > maxLimit {
				t.Errorf("step %d: CurrentLimit=%d out of [%d, %d]", step, cur, minLimit, maxLimit)
			}
		}
	})
}
