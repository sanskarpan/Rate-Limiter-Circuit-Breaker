package adaptive_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/testutil"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/adaptive"
)

// TestAdaptive_BasicAllow verifies initial limit allows requests.
func TestAdaptive_BasicAllow(t *testing.T) {
	signals := adaptive.NewStaticSignals(20, 0.05, 10*time.Millisecond)
	al := adaptive.New(100, 10, 1000, signals, adaptive.WithClock(clock.RealClock{}))
	defer al.Close()
	ctx := context.Background()

	result := al.Allow(ctx, "key")
	if !result.Allowed {
		t.Fatal("first request should be allowed")
	}
	if result.Algorithm != "adaptive" {
		t.Fatalf("expected algorithm 'adaptive', got %q", result.Algorithm)
	}
}

// TestAdaptive_HighStress_LimitDecreases verifies limit decreases when error rate is high.
func TestAdaptive_HighStress_LimitDecreases(t *testing.T) {
	// CPU=80, ErrorRate=1.0 (> 0.05), P99=10s (>> threshold) → stress → decrease 10%.
	signals := adaptive.NewStaticSignals(80, 1.0, 10*time.Second) // p99=10s >> critical=500ms
	al := adaptive.New(100, 10, 1000, signals, adaptive.WithClock(clock.RealClock{}))
	defer al.Close()

	initialLimit := al.CurrentLimit()

	// Trigger 3 adjustment cycles synchronously
	for i := 0; i < 3; i++ {
		al.ForceAdjust()
	}

	finalLimit := al.CurrentLimit()
	if finalLimit >= initialLimit {
		t.Fatalf("expected limit to decrease under high stress (score≈0.94): initial=%d, final=%d",
			initialLimit, finalLimit)
	}
}

// TestAdaptive_LowStress_LimitIncreases verifies limit increases when system is healthy.
func TestAdaptive_LowStress_LimitIncreases(t *testing.T) {
	// SPEC increase rule: CPU<50 AND ErrorRate<0.01 AND P99<threshold*0.5.
	// CPU=5, ErrorRate=0.005 (< 0.01), P99=5ms (< 250ms) → all healthy → increase.
	signals := adaptive.NewStaticSignals(5, 0.005, 5*time.Millisecond)
	al := adaptive.New(100, 10, 1000, signals, adaptive.WithClock(clock.RealClock{}))
	defer al.Close()

	initialLimit := al.CurrentLimit()

	// Trigger 3 adjustment cycles synchronously
	for i := 0; i < 3; i++ {
		al.ForceAdjust()
	}

	finalLimit := al.CurrentLimit()
	if finalLimit <= initialLimit {
		t.Fatalf("expected limit to increase under low stress: initial=%d, final=%d",
			initialLimit, finalLimit)
	}
}

// TestAdaptive_LimitClamped_NeverBelowMin verifies limit never goes below minLimit.
func TestAdaptive_LimitClamped_NeverBelowMin(t *testing.T) {
	// Extreme stress: cpu=100%, errorRate=1.0, p99=10s >> critical → score ≈ 1.0
	signals := adaptive.NewStaticSignals(100, 1.0, 10*time.Second)
	al := adaptive.New(100, 50, 1000, signals, adaptive.WithClock(clock.RealClock{}))
	defer al.Close()

	// Run many adjustment cycles synchronously
	for i := 0; i < 20; i++ {
		al.ForceAdjust()
	}

	if al.CurrentLimit() < 50 {
		t.Fatalf("limit %d went below minimum 50", al.CurrentLimit())
	}
}

// TestAdaptive_LimitClamped_NeverAboveMax verifies limit never exceeds maxLimit.
func TestAdaptive_LimitClamped_NeverAboveMax(t *testing.T) {
	// Very healthy → limit should increase but not above max
	signals := adaptive.NewStaticSignals(0, 0, 0)
	al := adaptive.New(100, 10, 150, signals, adaptive.WithClock(clock.RealClock{}))
	defer al.Close()

	for i := 0; i < 20; i++ {
		al.ForceAdjust()
	}

	if al.CurrentLimit() > 150 {
		t.Fatalf("limit %d exceeded maximum 150", al.CurrentLimit())
	}
}

// TestAdaptive_GradientSmoothing verifies no oscillation between min and max.
func TestAdaptive_GradientSmoothing(t *testing.T) {
	// Start healthy (all signals below the SPEC increase thresholds).
	signals := adaptive.NewStaticSignals(5, 0.005, 5*time.Millisecond)
	al := adaptive.New(100, 10, 1000, signals, adaptive.WithClock(clock.RealClock{}))
	defer al.Close()

	var limits []int
	for i := 0; i < 10; i++ {
		al.ForceAdjust()
		limits = append(limits, al.CurrentLimit())
	}

	// Check no wild oscillation: no consecutive pair should jump more than 20%
	for i := 1; i < len(limits); i++ {
		diff := limits[i] - limits[i-1]
		if diff < 0 {
			diff = -diff
		}
		maxChange := limits[i-1] / 5 // 20% max per cycle
		if maxChange < 1 {
			maxChange = 1
		}
		if diff > maxChange*2 { // allow 2x the expected change for test robustness
			t.Logf("limits: %v", limits)
			t.Fatalf("oscillation detected: limit changed by %d between cycle %d and %d (max expected ~%d)",
				diff, i-1, i, maxChange)
		}
	}
}

// TestAdaptive_Concurrent_NoRace verifies no data races under high concurrency.
func TestAdaptive_Concurrent_NoRace(t *testing.T) {
	signals := adaptive.NewStaticSignals(20, 0.05, 10*time.Millisecond)
	al := adaptive.New(1000, 100, 10000, signals, adaptive.WithClock(clock.RealClock{}))
	defer al.Close()
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			al.Allow(ctx, "key") //nolint:errcheck
		}()
	}
	wg.Wait()
}

// TestAdaptive_Close_StopsAdjustment verifies Close() stops background goroutine.
func TestAdaptive_Close_StopsAdjustment(t *testing.T) {
	lc := testutil.NewLeakChecker(t)
	defer lc.Check()

	signals := adaptive.NewStaticSignals(20, 0.05, 10*time.Millisecond)
	al := adaptive.New(100, 10, 1000, signals)
	if err := al.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestAdaptive_Peek_IncludesSignalData verifies Peek includes current signal values.
func TestAdaptive_Peek_IncludesSignalData(t *testing.T) {
	signals := adaptive.NewStaticSignals(42.0, 0.15, 200*time.Millisecond)
	al := adaptive.New(100, 10, 1000, signals, adaptive.WithClock(clock.RealClock{}))
	defer al.Close()
	ctx := context.Background()

	state := al.Peek(ctx, "key")
	if state.Algorithm != "adaptive" {
		t.Fatalf("expected algorithm 'adaptive', got %q", state.Algorithm)
	}
	if state.Extra == nil {
		t.Fatal("Extra should not be nil")
	}
	if _, ok := state.Extra["cpu_percent"]; !ok {
		t.Error("Extra should contain 'cpu_percent'")
	}
	if _, ok := state.Extra["error_rate"]; !ok {
		t.Error("Extra should contain 'error_rate'")
	}
	if _, ok := state.Extra["p99_latency"]; !ok {
		t.Error("Extra should contain 'p99_latency'")
	}
}

func TestAdaptive_String(t *testing.T) {
	signals := adaptive.NewStaticSignals(20, 0.05, 10*time.Millisecond)
	al := adaptive.New(100, 10, 1000, signals, adaptive.WithClock(clock.RealClock{}))
	defer al.Close()

	str := al.String()
	if str == "" {
		t.Fatal("String should not be empty")
	}
	t.Logf("String(): %s", str)
}

func TestAdaptive_Close_Idempotent(t *testing.T) {
	signals := adaptive.NewStaticSignals(20, 0.05, 10*time.Millisecond)
	al := adaptive.New(100, 10, 1000, signals, adaptive.WithClock(clock.RealClock{}))

	al.Close() //nolint:errcheck
	al.Close() //nolint:errcheck
	al.Close() //nolint:errcheck
}

func TestAdaptive_CurrentLimit(t *testing.T) {
	signals := adaptive.NewStaticSignals(20, 0.05, 10*time.Millisecond)
	al := adaptive.New(100, 10, 1000, signals, adaptive.WithClock(clock.RealClock{}))
	defer al.Close()

	limit := al.CurrentLimit()
	if limit != 100 {
		t.Fatalf("expected initial limit 100, got %d", limit)
	}
}

func TestAdaptive_AllowN(t *testing.T) {
	signals := adaptive.NewStaticSignals(20, 0.05, 10*time.Millisecond)
	al := adaptive.New(100, 10, 1000, signals, adaptive.WithClock(clock.RealClock{}))
	defer al.Close()
	ctx := context.Background()

	result := al.AllowN(ctx, "key", 5)
	if !result.Allowed {
		t.Fatal("AllowN(5) should be allowed")
	}
	if result.Limit != 100 {
		t.Fatalf("expected limit 100, got %d", result.Limit)
	}
}

func TestAdaptive_Reset(t *testing.T) {
	signals := adaptive.NewStaticSignals(20, 0.05, 10*time.Millisecond)
	al := adaptive.New(100, 10, 1000, signals, adaptive.WithClock(clock.RealClock{}))
	defer al.Close()
	ctx := context.Background()

	al.Allow(ctx, "key") //nolint:errcheck
	al.Reset(ctx, "key") //nolint:errcheck
}

// TestAdaptive_NeutralZone_NoAdjustment verifies the threshold rules hold the
// limit when signals are neither stressed nor fully healthy. With the default
// P99 threshold of 500ms: CPU=60 (not >80, not <50), ErrorRate=0.03 (not >0.05,
// not <0.01), P99=300ms (not >500ms, not <250ms) → neither rule fires.
func TestAdaptive_NeutralZone_NoAdjustment(t *testing.T) {
	signals := adaptive.NewStaticSignals(60, 0.03, 300*time.Millisecond)
	al := adaptive.New(100, 10, 1000, signals, adaptive.WithClock(clock.RealClock{}))
	defer al.Close()

	initialLimit := al.CurrentLimit()
	al.ForceAdjust()
	finalLimit := al.CurrentLimit()

	if initialLimit != finalLimit {
		t.Fatalf("neutral-zone signals should not adjust: %d -> %d", initialLimit, finalLimit)
	}
}

// TestAdaptive_ErrorRateAlone_Decreases is the H-11 regression test: a high
// error rate ALONE (CPU and P99 healthy) must trigger a decrease. Under the old
// weighted-score model, ErrorRate=0.1 with cpu=5/p99=1ms scored ~0.05 and would
// have (wrongly) increased the limit.
func TestAdaptive_ErrorRateAlone_Decreases(t *testing.T) {
	// ErrorRate=0.1 > 0.05, but CPU=5 (<80) and P99=1ms (<threshold) are healthy.
	signals := adaptive.NewStaticSignals(5, 0.1, 1*time.Millisecond)
	al := adaptive.New(100, 10, 1000, signals, adaptive.WithClock(clock.RealClock{}))
	defer al.Close()

	initialLimit := al.CurrentLimit()
	al.ForceAdjust()
	finalLimit := al.CurrentLimit()

	if finalLimit >= initialLimit {
		t.Fatalf("ErrorRate=0.1 alone must trigger a decrease (H-11): %d -> %d", initialLimit, finalLimit)
	}
}

// TestAdaptive_SmallLimit_IncreasesByAtLeastOne is the C-6 regression test: a
// small current limit under healthy signals must keep increasing rather than
// stalling on integer truncation of the smoothed value.
func TestAdaptive_SmallLimit_IncreasesByAtLeastOne(t *testing.T) {
	// Healthy signals so the increase rule fires every cycle.
	signals := adaptive.NewStaticSignals(5, 0.005, 1*time.Millisecond)
	// initial=10, min=10, max=1000. current*0.9+target*0.1 with a 5% target
	// rounds back to 10 for such a small limit, so without the min-step it sticks.
	al := adaptive.New(10, 10, 1000, signals, adaptive.WithClock(clock.RealClock{}))
	defer al.Close()

	start := al.CurrentLimit()
	al.ForceAdjust()
	afterOne := al.CurrentLimit()
	if afterOne <= start {
		t.Fatalf("small limit did not increase after one adjust (C-6): %d -> %d", start, afterOne)
	}

	// And it should keep climbing over successive cycles (not stuck).
	for i := 0; i < 5; i++ {
		al.ForceAdjust()
	}
	if al.CurrentLimit() <= afterOne {
		t.Fatalf("small limit stalled over successive adjusts (C-6): %d then %d", afterOne, al.CurrentLimit())
	}
}

// TestAdaptive_SmallLimit_DecreasesByAtLeastOne verifies the symmetric C-6
// guarantee for decreases.
func TestAdaptive_SmallLimit_DecreasesByAtLeastOne(t *testing.T) {
	signals := adaptive.NewStaticSignals(5, 0.1, 1*time.Millisecond) // error rate → decrease
	al := adaptive.New(10, 1, 1000, signals, adaptive.WithClock(clock.RealClock{}))
	defer al.Close()

	start := al.CurrentLimit()
	al.ForceAdjust()
	if al.CurrentLimit() >= start {
		t.Fatalf("small limit did not decrease by at least one (C-6): %d -> %d", start, al.CurrentLimit())
	}
}

func TestAdaptive_Peek_DoesNotConsume(t *testing.T) {
	signals := adaptive.NewStaticSignals(20, 0.05, 10*time.Millisecond)
	al := adaptive.New(100, 10, 1000, signals, adaptive.WithClock(clock.RealClock{}))
	defer al.Close()
	ctx := context.Background()

	state1 := al.Peek(ctx, "key")
	state2 := al.Peek(ctx, "key")
	if state1.Remaining != state2.Remaining {
		t.Fatal("Peek should not consume tokens")
	}
}

// TestAdaptive_Adjust_PreservesBucketState is the H-12 regression test: an
// adjustment must retune the existing token bucket in place rather than
// rebuilding it. A key that was drained before the adjust must stay drained
// (not handed a fresh full burst), and no in-flight bucket is closed.
func TestAdaptive_Adjust_PreservesBucketState(t *testing.T) {
	// Healthy signals so ForceAdjust triggers an increase (a change occurs).
	signals := adaptive.NewStaticSignals(5, 0.005, 1*time.Millisecond)
	al := adaptive.New(50, 10, 1000, signals, adaptive.WithClock(clock.RealClock{}))
	defer al.Close()
	ctx := context.Background()

	// Drain the key completely.
	for i := 0; i < 50; i++ {
		if !al.Allow(ctx, "client-a").Allowed {
			break
		}
	}
	if al.Allow(ctx, "client-a").Allowed {
		t.Fatal("key should be drained before adjust")
	}

	before := al.CurrentLimit()
	al.ForceAdjust()
	after := al.CurrentLimit()
	if after == before {
		t.Fatalf("expected the limit to change on adjust (got %d both times)", before)
	}

	// If the bucket had been rebuilt, the drained key would be refilled to a full
	// burst and this would be allowed. With in-place SetLimit it stays drained.
	if al.Allow(ctx, "client-a").Allowed {
		t.Fatal("drained key was refilled after adjust — bucket state was wiped (H-12)")
	}
}

// TestRuntimeSignals_EMAWarmup is the L-7 regression test: the error-rate EMA is
// seeded toward a neutral prior with a warmup, so a single early sample must not
// pin the reported error rate to 0.0 or 1.0.
func TestRuntimeSignals_EMAWarmup(t *testing.T) {
	// One lone error on a cold signal source must NOT read as a 100% error rate.
	sErr := adaptive.NewRuntimeSignals()
	sErr.RecordError(5 * time.Millisecond)
	if r := sErr.ErrorRate(); r >= 1.0 {
		t.Fatalf("single early error should not read as 1.0 error rate (L-7 warmup), got %v", r)
	}
	if r := sErr.ErrorRate(); r <= 0.0 {
		t.Fatalf("error rate after an error should be > 0, got %v", r)
	}

	// One lone success must NOT read as a 0% error rate either.
	sOK := adaptive.NewRuntimeSignals()
	sOK.RecordSuccess(5 * time.Millisecond)
	if r := sOK.ErrorRate(); r <= 0.0 {
		t.Fatalf("single early success should not read as 0.0 error rate (L-7 warmup), got %v", r)
	}

	// After many consistent successes the estimate should converge low (warmup ends).
	sConv := adaptive.NewRuntimeSignals()
	for i := 0; i < 200; i++ {
		sConv.RecordSuccess(time.Millisecond)
	}
	if r := sConv.ErrorRate(); r > 0.1 {
		t.Fatalf("error rate should converge low after many successes, got %v", r)
	}
}
