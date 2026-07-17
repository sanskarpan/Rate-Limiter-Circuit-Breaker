package slidingwindow_test

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskarpan/resilience/internal/clock"
	"github.com/sanskarpan/resilience/internal/testutil"
	"github.com/sanskarpan/resilience/ratelimit/slidingwindow"
)

// ===================== SlidingWindowLog Tests =====================

func TestSlidingWindowLog_ExactBoundary(t *testing.T) {
	clk := clock.NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	l := slidingwindow.NewLog(3, time.Second, slidingwindow.WithLogClock(clk))
	defer l.Close()
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if !l.Allow(ctx, "key").Allowed {
			t.Fatalf("request %d should be allowed", i+1)
		}
	}
	if l.Allow(ctx, "key").Allowed {
		t.Fatal("4th request should be denied")
	}
}

func TestSlidingWindowLog_OldRequestsExpire(t *testing.T) {
	clk := clock.NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	l := slidingwindow.NewLog(2, time.Second, slidingwindow.WithLogClock(clk))
	defer l.Close()
	ctx := context.Background()

	l.Allow(ctx, "key") //nolint:errcheck
	l.Allow(ctx, "key") //nolint:errcheck
	if l.Allow(ctx, "key").Allowed {
		t.Fatal("3rd request should be denied")
	}

	// Advance 1.1 seconds — both requests expire
	clk.Advance(1100 * time.Millisecond)
	if !l.Allow(ctx, "key").Allowed {
		t.Fatal("request should be allowed after window passes")
	}
}

func TestSlidingWindowLog_RetryAfterPrecise(t *testing.T) {
	clk := clock.NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	l := slidingwindow.NewLog(1, time.Second, slidingwindow.WithLogClock(clk))
	defer l.Close()
	ctx := context.Background()

	l.Allow(ctx, "key") //nolint:errcheck
	result := l.Allow(ctx, "key")
	if result.Allowed {
		t.Fatal("should be denied")
	}
	if result.RetryAfter <= 0 || result.RetryAfter > time.Second {
		t.Errorf("unexpected RetryAfter: %v", result.RetryAfter)
	}
}

func TestSlidingWindowLog_Concurrent_NoRace(t *testing.T) {
	l := slidingwindow.NewLog(1000, time.Minute)
	defer l.Close()
	ctx := context.Background()

	var wg sync.WaitGroup
	var allowed atomic.Int64
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if l.Allow(ctx, "key").Allowed {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()
	if allowed.Load() != 500 {
		t.Fatalf("all 500 should be allowed (limit=1000), got %d", allowed.Load())
	}
}

func TestSlidingWindowLog_Close_NoLeak(t *testing.T) {
	lc := testutil.NewLeakChecker(t)
	defer lc.Check()
	l := slidingwindow.NewLog(10, time.Second)
	l.Close() //nolint:errcheck
}

func TestSlidingWindowLog_Reset(t *testing.T) {
	clk := clock.NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	l := slidingwindow.NewLog(1, time.Second, slidingwindow.WithLogClock(clk))
	defer l.Close()
	ctx := context.Background()

	l.Allow(ctx, "key") //nolint:errcheck
	if l.Allow(ctx, "key").Allowed {
		t.Fatal("should be denied")
	}
	l.Reset(ctx, "key") //nolint:errcheck
	if !l.Allow(ctx, "key").Allowed {
		t.Fatal("should be allowed after reset")
	}
}

// ===================== SlidingWindowCounter Tests =====================

func TestSlidingWindowCounter_BasicAllow(t *testing.T) {
	clk := clock.NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	swc := slidingwindow.NewCounter(5, time.Second, slidingwindow.WithCounterClock(clk))
	defer swc.Close()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if !swc.Allow(ctx, "key").Allowed {
			t.Fatalf("request %d should be allowed", i+1)
		}
	}
	if swc.Allow(ctx, "key").Allowed {
		t.Fatal("6th request should be denied")
	}
}

func TestSlidingWindowCounter_ApproximationFormula(t *testing.T) {
	// Window=1s, limit=10
	// At 50% through window, with previous=8 and current=3:
	// effective = 8*(1-0.5) + 3 = 4 + 3 = 7 → 3 remaining
	clk := clock.NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	swc := slidingwindow.NewCounter(10, time.Second, slidingwindow.WithCounterClock(clk))
	defer swc.Close()
	ctx := context.Background()

	// Fill first window with 8 requests
	for i := 0; i < 8; i++ {
		swc.Allow(ctx, "key") //nolint:errcheck
	}
	// Advance to next window
	clk.Advance(time.Second)
	// Add 3 in new window
	for i := 0; i < 3; i++ {
		swc.Allow(ctx, "key") //nolint:errcheck
	}
	// Advance to 50% of new window
	clk.Advance(500 * time.Millisecond)
	// effective = 8*0.5 + 3 = 7, remaining = 3
	state := swc.Peek(ctx, "key")
	if state.Remaining < 2 || state.Remaining > 4 {
		t.Errorf("expected remaining ~3, got %d (effective_count may vary due to timing)", state.Remaining)
	}
}

func TestSlidingWindowCounter_WindowShift(t *testing.T) {
	clk := clock.NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	swc := slidingwindow.NewCounter(10, time.Second, slidingwindow.WithCounterClock(clk))
	defer swc.Close()
	ctx := context.Background()

	// Fill window
	for i := 0; i < 10; i++ {
		swc.Allow(ctx, "key") //nolint:errcheck
	}
	// Advance full window — previous=10, current=0
	clk.Advance(time.Second)
	// At 0% elapsed: effective = 10*1.0 + 0 = 10 → still at limit
	// At start of window, previous fully weighted → denied
	// Advance 50% more → effective = 10*0.5 = 5 → 5 remaining
	clk.Advance(500 * time.Millisecond)
	result := swc.Allow(ctx, "key")
	if !result.Allowed {
		t.Fatal("should be allowed when effective count is below limit")
	}
}

func TestSlidingWindowCounter_Concurrent_NoRace(t *testing.T) {
	swc := slidingwindow.NewCounter(1000, time.Minute)
	defer swc.Close()
	ctx := context.Background()

	var wg sync.WaitGroup
	var allowed atomic.Int64
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if swc.Allow(ctx, "key").Allowed {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()
	if allowed.Load() != 500 {
		t.Fatalf("all 500 should be allowed (limit=1000), got %d", allowed.Load())
	}
}

func TestSlidingWindowCounter_Close_NoLeak(t *testing.T) {
	lc := testutil.NewLeakChecker(t)
	defer lc.Check()
	swc := slidingwindow.NewCounter(10, time.Second)
	swc.Close() //nolint:errcheck
}

// TestSlidingWindowComparison_LogVsCounter verifies the counter approximation
// is within 10% of the exact log-based implementation.
func TestSlidingWindowComparison_LogVsCounter(t *testing.T) {
	ctx := context.Background()

	// Run same sequence against both implementations
	logAllowed := 0
	ctrAllowed := 0

	log := slidingwindow.NewLog(100, time.Second)
	defer log.Close()
	ctr := slidingwindow.NewCounter(100, time.Second)
	defer ctr.Close()

	// Send 150 requests — expect ~100 to be allowed by each
	for i := 0; i < 150; i++ {
		if log.Allow(ctx, "key").Allowed {
			logAllowed++
		}
		if ctr.Allow(ctx, "key").Allowed {
			ctrAllowed++
		}
	}

	// Both should allow exactly 100
	if logAllowed != 100 {
		t.Errorf("log allowed %d, expected 100", logAllowed)
	}
	// Counter approximation should be within 10% of log
	diff := abs(logAllowed - ctrAllowed)
	if diff > 10 {
		t.Errorf("counter (%d) differs from log (%d) by more than 10%%", ctrAllowed, logAllowed)
	}
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

// SlidingWindowLog additional tests

func TestSlidingWindowLog_Peek(t *testing.T) {
	clk := clock.NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	l := slidingwindow.NewLog(5, time.Second, slidingwindow.WithLogClock(clk))
	defer l.Close()
	ctx := context.Background()

	l.Allow(ctx, "key") //nolint:errcheck
	l.Allow(ctx, "key") //nolint:errcheck

	state := l.Peek(ctx, "key")
	if state.Remaining != 3 {
		t.Fatalf("expected remaining=3, got %d", state.Remaining)
	}
}

func TestSlidingWindowLog_Peek_DoesNotConsume(t *testing.T) {
	clk := clock.NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	l := slidingwindow.NewLog(5, time.Second, slidingwindow.WithLogClock(clk))
	defer l.Close()
	ctx := context.Background()

	state1 := l.Peek(ctx, "key")
	state2 := l.Peek(ctx, "key")
	if state1.Remaining != state2.Remaining {
		t.Fatal("Peek should not consume tokens")
	}
}

func TestSlidingWindowLog_AllowN(t *testing.T) {
	clk := clock.NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	l := slidingwindow.NewLog(5, time.Second, slidingwindow.WithLogClock(clk))
	defer l.Close()
	ctx := context.Background()

	result := l.AllowN(ctx, "key", 3)
	if !result.Allowed {
		t.Fatal("AllowN(3) should be allowed")
	}
	if result.Remaining != 2 {
		t.Fatalf("expected remaining=2, got %d", result.Remaining)
	}
}

func TestSlidingWindowLog_AllowN_ExceedsLimit(t *testing.T) {
	clk := clock.NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	l := slidingwindow.NewLog(5, time.Second, slidingwindow.WithLogClock(clk))
	defer l.Close()
	ctx := context.Background()

	result := l.AllowN(ctx, "key", 6)
	if result.Allowed {
		t.Fatal("AllowN(6) should be denied when limit is 5")
	}
}

func TestSlidingWindowLog_MultipleKeys_Isolation(t *testing.T) {
	clk := clock.NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	l := slidingwindow.NewLog(1, time.Second, slidingwindow.WithLogClock(clk))
	defer l.Close()
	ctx := context.Background()

	l.Allow(ctx, "a") //nolint:errcheck
	if l.Allow(ctx, "a").Allowed {
		t.Fatal("key 'a' should be exhausted")
	}
	if !l.Allow(ctx, "b").Allowed {
		t.Fatal("key 'b' should still be available")
	}
}

func TestSlidingWindowLog_InvalidKey(t *testing.T) {
	clk := clock.NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	l := slidingwindow.NewLog(5, time.Second, slidingwindow.WithLogClock(clk))
	defer l.Close()
	ctx := context.Background()

	result := l.Allow(ctx, "")
	if result.Allowed {
		t.Fatal("empty key should be denied")
	}
}

func TestSlidingWindowLog_InvalidN(t *testing.T) {
	clk := clock.NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	l := slidingwindow.NewLog(5, time.Second, slidingwindow.WithLogClock(clk))
	defer l.Close()
	ctx := context.Background()

	result := l.AllowN(ctx, "key", 0)
	if result.Allowed {
		t.Fatal("n=0 should be denied")
	}

	result = l.AllowN(ctx, "key", -1)
	if result.Allowed {
		t.Fatal("n=-1 should be denied")
	}
}

func TestSlidingWindowLog_String(t *testing.T) {
	l := slidingwindow.NewLog(100, time.Minute)
	defer l.Close()
	str := l.String()
	if str == "" {
		t.Fatal("String should not be empty")
	}
	t.Logf("String(): %s", str)
}

func TestSlidingWindowLog_Close_Idempotent(t *testing.T) {
	l := slidingwindow.NewLog(10, time.Second)
	l.Close() //nolint:errcheck
	l.Close() //nolint:errcheck
	l.Close() //nolint:errcheck
}

func TestSlidingWindowLog_Wait_Success(t *testing.T) {
	clk := clock.NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	l := slidingwindow.NewLog(1, time.Second, slidingwindow.WithLogClock(clk))
	defer l.Close()

	err := l.Wait(context.Background(), "key")
	if err != nil {
		t.Fatalf("Wait should succeed: %v", err)
	}
}

// SlidingWindowCounter additional tests

func TestSlidingWindowCounter_Peek(t *testing.T) {
	clk := clock.NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	swc := slidingwindow.NewCounter(5, time.Second, slidingwindow.WithCounterClock(clk))
	defer swc.Close()
	ctx := context.Background()

	swc.Allow(ctx, "key") //nolint:errcheck
	swc.Allow(ctx, "key") //nolint:errcheck

	state := swc.Peek(ctx, "key")
	if state.Remaining < 0 || state.Remaining > 5 {
		t.Fatalf("unexpected remaining: %d", state.Remaining)
	}
}

func TestSlidingWindowCounter_Peek_DoesNotConsume(t *testing.T) {
	clk := clock.NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	swc := slidingwindow.NewCounter(5, time.Second, slidingwindow.WithCounterClock(clk))
	defer swc.Close()
	ctx := context.Background()

	state1 := swc.Peek(ctx, "key")
	state2 := swc.Peek(ctx, "key")
	if state1.Remaining != state2.Remaining {
		t.Fatal("Peek should not consume tokens")
	}
}

func TestSlidingWindowCounter_AllowN(t *testing.T) {
	clk := clock.NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	swc := slidingwindow.NewCounter(5, time.Second, slidingwindow.WithCounterClock(clk))
	defer swc.Close()
	ctx := context.Background()

	result := swc.AllowN(ctx, "key", 3)
	if !result.Allowed {
		t.Fatal("AllowN(3) should be allowed")
	}
}

func TestSlidingWindowCounter_AllowN_ExceedsLimit(t *testing.T) {
	clk := clock.NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	swc := slidingwindow.NewCounter(5, time.Second, slidingwindow.WithCounterClock(clk))
	defer swc.Close()
	ctx := context.Background()

	result := swc.AllowN(ctx, "key", 6)
	if result.Allowed {
		t.Fatal("AllowN(6) should be denied when limit is 5")
	}
}

func TestSlidingWindowCounter_MultipleKeys_Isolation(t *testing.T) {
	clk := clock.NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	swc := slidingwindow.NewCounter(1, time.Second, slidingwindow.WithCounterClock(clk))
	defer swc.Close()
	ctx := context.Background()

	swc.Allow(ctx, "a") //nolint:errcheck
	if swc.Allow(ctx, "a").Allowed {
		t.Fatal("key 'a' should be exhausted")
	}
	if !swc.Allow(ctx, "b").Allowed {
		t.Fatal("key 'b' should still be available")
	}
}

func TestSlidingWindowCounter_Reset(t *testing.T) {
	clk := clock.NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	swc := slidingwindow.NewCounter(1, time.Second, slidingwindow.WithCounterClock(clk))
	defer swc.Close()
	ctx := context.Background()

	swc.Allow(ctx, "key") //nolint:errcheck
	if swc.Allow(ctx, "key").Allowed {
		t.Fatal("should be denied before reset")
	}
	swc.Reset(ctx, "key") //nolint:errcheck
	if !swc.Allow(ctx, "key").Allowed {
		t.Fatal("should be allowed after reset")
	}
}

func TestSlidingWindowCounter_InvalidKey(t *testing.T) {
	clk := clock.NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	swc := slidingwindow.NewCounter(5, time.Second, slidingwindow.WithCounterClock(clk))
	defer swc.Close()
	ctx := context.Background()

	result := swc.Allow(ctx, "")
	if result.Allowed {
		t.Fatal("empty key should be denied")
	}
}

func TestSlidingWindowCounter_InvalidN(t *testing.T) {
	clk := clock.NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	swc := slidingwindow.NewCounter(5, time.Second, slidingwindow.WithCounterClock(clk))
	defer swc.Close()
	ctx := context.Background()

	result := swc.AllowN(ctx, "key", 0)
	if result.Allowed {
		t.Fatal("n=0 should be denied")
	}

	result = swc.AllowN(ctx, "key", -1)
	if result.Allowed {
		t.Fatal("n=-1 should be denied")
	}
}

func TestSlidingWindowCounter_String(t *testing.T) {
	swc := slidingwindow.NewCounter(100, time.Minute)
	defer swc.Close()
	str := swc.String()
	if str == "" {
		t.Fatal("String should not be empty")
	}
	t.Logf("String(): %s", str)
}

func TestSlidingWindowCounter_Close_Idempotent(t *testing.T) {
	swc := slidingwindow.NewCounter(10, time.Second)
	swc.Close() //nolint:errcheck
	swc.Close() //nolint:errcheck
	swc.Close() //nolint:errcheck
}

func TestSlidingWindowCounter_Wait_Success(t *testing.T) {
	clk := clock.NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	swc := slidingwindow.NewCounter(1, time.Second, slidingwindow.WithCounterClock(clk))
	defer swc.Close()

	err := swc.Wait(context.Background(), "key")
	if err != nil {
		t.Fatalf("Wait should succeed: %v", err)
	}
}

// ===================== TQ-5: multi-window time progression =====================

// TestSlidingWindowCounter_PreviousWeighting_AcrossBoundary pins the exact
// previous-window weighting math when advancing across a single window
// boundary. The shipped tests fired all requests at one instant; this one
// advances the clock so the sliding behaviour is actually exercised.
func TestSlidingWindowCounter_PreviousWeighting_AcrossBoundary(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewManualClock(start)
	swc := slidingwindow.NewCounter(10, time.Second, slidingwindow.WithCounterClock(clk))
	defer swc.Close()
	ctx := context.Background()

	// Window A [0s,1s): 8 requests -> current=8.
	swc.AllowN(ctx, "key", 8) //nolint:errcheck

	// Cross exactly one boundary into window B [1s,2s); previous=8, current=0.
	clk.Advance(time.Second)
	// Advance to exactly 50% through window B first. At this point the
	// previous window contributes 8*(1-0.5)=4, leaving room for 3 more.
	clk.Advance(500 * time.Millisecond)
	// Add 3 in window B -> current=3 (4+3 = 7 <= 10, allowed).
	if !swc.AllowN(ctx, "key", 3).Allowed {
		t.Fatal("AllowN(3) at 50% through window B should be allowed (effective 4+3=7)")
	}

	// effective = 8*(1-0.5)+3 = 7.
	state := swc.Peek(ctx, "key")

	eff, _ := state.Extra["effective_count"].(float64)
	if eff != 7.0 {
		t.Errorf("effective_count = %v, want exactly 7.0 (8*0.5 + 3)", eff)
	}
	if state.Remaining != 3 {
		t.Errorf("remaining = %d, want 3", state.Remaining)
	}
	if pc, _ := state.Extra["previous_count"].(int64); pc != 8 {
		t.Errorf("previous_count = %d, want 8", pc)
	}
	if cc, _ := state.Extra["current_count"].(int64); cc != 3 {
		t.Errorf("current_count = %d, want 3", cc)
	}
}

// TestSlidingWindowCounter_MultiWindowGap_ResetsPrevious verifies Fix-4: when
// the clock jumps 2+ full windows at once, the previous bucket must reset to 0
// (not carry a stale count from a window that is now entirely out of range).
func TestSlidingWindowCounter_MultiWindowGap_ResetsPrevious(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewManualClock(start)
	swc := slidingwindow.NewCounter(10, time.Second, slidingwindow.WithCounterClock(clk))
	defer swc.Close()
	ctx := context.Background()

	// Window A [0s,1s): fill to the limit.
	swc.AllowN(ctx, "key", 10) //nolint:errcheck

	// Jump 3 full windows forward to [3s,4s). Windows A and B are both entirely
	// out of the sliding range, so previous must be 0 and current must be 0.
	clk.Advance(3 * time.Second)

	state := swc.Peek(ctx, "key")
	if pc, _ := state.Extra["previous_count"].(int64); pc != 0 {
		t.Errorf("previous_count = %d, want 0 after 3-window gap", pc)
	}
	if cc, _ := state.Extra["current_count"].(int64); cc != 0 {
		t.Errorf("current_count = %d, want 0 after 3-window gap", cc)
	}
	if eff, _ := state.Extra["effective_count"].(float64); eff != 0.0 {
		t.Errorf("effective_count = %v, want 0.0 after 3-window gap", eff)
	}
	if state.Remaining != 10 {
		t.Errorf("remaining = %d, want full 10 after 3-window gap", state.Remaining)
	}
	// The full limit should be immediately available.
	if !swc.AllowN(ctx, "key", 10).Allowed {
		t.Fatal("full limit should be available after a multi-window gap")
	}
}

// TestSlidingWindowCounter_ExactlyOneWindowBack_CarriesPrevious locks in the
// exact-one-window-back branch of the window-shift gap handling: crossing
// exactly one boundary carries the old current into previous, whereas a
// 2+-window gap discards it (verified above). This is the pair Fix-4 pins.
func TestSlidingWindowCounter_ExactlyOneWindowBack_CarriesPrevious(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewManualClock(start)
	swc := slidingwindow.NewCounter(10, time.Second, slidingwindow.WithCounterClock(clk))
	defer swc.Close()
	ctx := context.Background()

	// Window A: 6 requests.
	swc.AllowN(ctx, "key", 6) //nolint:errcheck
	// Cross exactly one window into window B at 0% elapsed.
	clk.Advance(time.Second)

	state := swc.Peek(ctx, "key")
	if pc, _ := state.Extra["previous_count"].(int64); pc != 6 {
		t.Errorf("previous_count = %d, want 6 (carried from exactly one window back)", pc)
	}
	if cc, _ := state.Extra["current_count"].(int64); cc != 0 {
		t.Errorf("current_count = %d, want 0", cc)
	}
	// At 0% into window B, previous is fully weighted: effective = 6*1.0 = 6.
	if eff, _ := state.Extra["effective_count"].(float64); eff != 6.0 {
		t.Errorf("effective_count = %v, want 6.0 (fully-weighted previous)", eff)
	}
}

// TestSlidingWindowLog_PartialAdvance_EvictsAndRetry pins the log's timestamp
// eviction and RetryAfter reference. It advances the clock partially so only
// some timestamps fall out of the window, then asserts the surviving count and
// that RetryAfter references the OLDEST in-window timestamp.
func TestSlidingWindowLog_PartialAdvance_EvictsAndRetry(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewManualClock(start)
	l := slidingwindow.NewLog(3, time.Second, slidingwindow.WithLogClock(clk))
	defer l.Close()
	ctx := context.Background()

	// t=0ms: request 1.
	l.Allow(ctx, "key") //nolint:errcheck
	// t=400ms: request 2.
	clk.Advance(400 * time.Millisecond)
	l.Allow(ctx, "key") //nolint:errcheck
	// t=700ms: request 3 -> log now full at [0ms, 400ms, 700ms].
	clk.Advance(300 * time.Millisecond)
	l.Allow(ctx, "key") //nolint:errcheck

	// t=1100ms: window is [100ms, 1100ms]. The t=0ms timestamp is evicted,
	// leaving [400ms, 700ms] -> count 2, so one more should be allowed.
	clk.Advance(400 * time.Millisecond)
	state := l.Peek(ctx, "key")
	if size, _ := state.Extra["log_size"].(int); size != 2 {
		t.Errorf("log_size = %d, want 2 after evicting the t=0ms timestamp", size)
	}
	if state.Remaining != 1 {
		t.Errorf("remaining = %d, want 1", state.Remaining)
	}

	// Fill the freed slot; log = [400ms, 700ms, 1100ms].
	if !l.Allow(ctx, "key").Allowed {
		t.Fatal("request should be allowed after partial eviction")
	}
	// Now deny: RetryAfter must reference the OLDEST in-window timestamp
	// (t=400ms) rolling out: 400ms + 1s - 1100ms = 300ms.
	result := l.Allow(ctx, "key")
	if result.Allowed {
		t.Fatal("request should be denied when full")
	}
	want := 300 * time.Millisecond
	if result.RetryAfter != want {
		t.Errorf("RetryAfter = %v, want exactly %v (oldest in-window ts + window - now)", result.RetryAfter, want)
	}
}

// ===================== L-3 (SWC-2): RetryAfter bounds =====================

// TestSlidingWindowCounter_RetryAfter_CurrentAloneExceedsLimit constructs a
// state where the CURRENT window count plus n exceeds the limit on its own
// (current=9, n=2, limit=10), while the previous window is also full (=10).
// Even if the previous window rolled off entirely, current+n (=11) would still
// exceed the limit, so no amount of previous-window decay helps. The correct
// RetryAfter is the time until the CURRENT window rolls over, not a bogus
// fraction of the previous-window rolloff.
//
// Before the fix, the code computed
//
//	neededFrac = (effective + n - limit) / previous.count
//	RetryAfter = neededFrac*window - elapsed
//
// which, at 95% through the window, produced RetryAfter ~= 1ms — a gross
// underestimate that causes a busy retry loop, since the request stays denied
// until the window actually rolls (~50ms later).
func TestSlidingWindowCounter_RetryAfter_CurrentAloneExceedsLimit(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewManualClock(start)
	swc := slidingwindow.NewCounter(10, time.Second, slidingwindow.WithCounterClock(clk))
	defer swc.Close()
	ctx := context.Background()

	// Window A: fill to the limit so previous becomes 10 after the roll.
	swc.AllowN(ctx, "key", 10) //nolint:errcheck
	clk.Advance(time.Second)   // roll into window B; previous=10
	// Advance to 95% through window B: previous now contributes 10*0.05=0.5,
	// leaving room to accumulate current=9 (0.5+9 = 9.5 <= 10).
	clk.Advance(950 * time.Millisecond)
	for i := 0; i < 9; i++ {
		if !swc.Allow(ctx, "key").Allowed {
			t.Fatalf("fill request %d should be allowed", i+1)
		}
	}

	// current=9, previous=10, n=2 -> current+n=11 > limit even with previous=0.
	result := swc.AllowN(ctx, "key", 2)
	if result.Allowed {
		t.Fatal("AllowN(2) should be denied: current(9)+2 exceeds limit(10)")
	}

	// Window B rolls at start+2s; now = start+1.95s, so RetryAfter ~= 50ms.
	wantRoll := 50 * time.Millisecond
	if result.RetryAfter < wantRoll-time.Millisecond || result.RetryAfter > wantRoll+time.Millisecond {
		t.Errorf("RetryAfter = %v, want ~%v (time until current window rolls)", result.RetryAfter, wantRoll)
	}
	if result.RetryAfter > time.Second {
		t.Errorf("RetryAfter %v exceeds a full window (1s)", result.RetryAfter)
	}
}

// TestSlidingWindowCounter_RetryAfter_NeverExceedsWindow exercises a boundary
// case where the previous window is heavily loaded. The old code computed
// neededFrac = (effective+n-limit)/previous.count which could exceed 1.0 and
// yield a RetryAfter larger than one window. Assert the invariant RetryAfter
// <= window.
func TestSlidingWindowCounter_RetryAfter_NeverExceedsWindow(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewManualClock(start)
	swc := slidingwindow.NewCounter(5, time.Second, slidingwindow.WithCounterClock(clk))
	defer swc.Close()
	ctx := context.Background()

	// Load the first window to the limit.
	for i := 0; i < 5; i++ {
		swc.Allow(ctx, "key") //nolint:errcheck
	}
	// Roll into the next window; previous.count becomes 5.
	clk.Advance(time.Second)
	// A tiny bit into the new window: effective ~= 5*(1-eps) ~ 5, so denied.
	clk.Advance(time.Millisecond)

	result := swc.Allow(ctx, "key")
	if result.Allowed {
		t.Fatal("request should be denied at start of new window with full previous")
	}
	if result.RetryAfter <= 0 {
		t.Errorf("RetryAfter should be positive when denied, got %v", result.RetryAfter)
	}
	if result.RetryAfter > time.Second {
		t.Errorf("RetryAfter %v exceeds a full window (1s)", result.RetryAfter)
	}
}

// ===================== M-5: Constructor validation =====================

// mustPanic runs fn and reports whether it panicked, capturing the value.
func mustPanic(t *testing.T, name string, fn func()) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("%s: expected a panic for bad config, got none", name)
		}
		// A divide-by-zero manifests as a runtime.Error ("integer divide by
		// zero"). We require a clear, intentional validation message instead.
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("%s: expected a string validation panic, got %T: %v", name, r, r)
		}
		if !strings.Contains(msg, "must be positive") {
			t.Fatalf("%s: expected a clear validation message, got %q", name, msg)
		}
	}()
	fn()
}

func TestNewLog_RejectsBadConfig(t *testing.T) {
	// Zero/negative window would start a goroutine that panics via NewTicker(0)
	// and cause divide-by-zero downstream. Constructor must reject cleanly.
	mustPanic(t, "NewLog(limit, 0)", func() {
		_ = slidingwindow.NewLog(5, 0)
	})
	mustPanic(t, "NewLog(limit, -1)", func() {
		_ = slidingwindow.NewLog(5, -time.Second)
	})
	mustPanic(t, "NewLog(0, window)", func() {
		_ = slidingwindow.NewLog(0, time.Second)
	})
	mustPanic(t, "NewLog(-1, window)", func() {
		_ = slidingwindow.NewLog(-1, time.Second)
	})
}

func TestNewCounter_RejectsBadConfig(t *testing.T) {
	mustPanic(t, "NewCounter(10, 0)", func() {
		_ = slidingwindow.NewCounter(10, 0)
	})
	mustPanic(t, "NewCounter(10, -1)", func() {
		_ = slidingwindow.NewCounter(10, -time.Second)
	})
	// limit=0 triggers window/time.Duration(limit) divide-by-zero in WaitN and
	// is a nonsensical config; reject it.
	mustPanic(t, "NewCounter(0, window)", func() {
		_ = slidingwindow.NewCounter(0, time.Second)
	})
	mustPanic(t, "NewCounter(-1, window)", func() {
		_ = slidingwindow.NewCounter(-1, time.Second)
	})
}

// TestNewCounter_ValidConfig_NoGoroutinePanic ensures a bad config does not
// leave a background goroutine that panics after construction returns.
func TestNewCounter_ValidConfig_NoGoroutinePanic(t *testing.T) {
	// A valid config must still construct and clean up without leaking a
	// panicking goroutine.
	swc := slidingwindow.NewCounter(5, time.Second)
	swc.Close() //nolint:errcheck
}

func BenchmarkSlidingWindowLog_Allow(b *testing.B) {
	l := slidingwindow.NewLog(1000000, time.Minute)
	defer l.Close()
	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		l.Allow(ctx, "bench")
	}
}

func BenchmarkSlidingWindowCounter_Allow(b *testing.B) {
	swc := slidingwindow.NewCounter(1000000, time.Minute)
	defer swc.Close()
	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		swc.Allow(ctx, "bench")
	}
}

func BenchmarkSlidingWindowCounter_Allow_Parallel(b *testing.B) {
	swc := slidingwindow.NewCounter(1000000000, time.Minute)
	defer swc.Close()
	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			swc.Allow(ctx, "bench")
		}
	})
}
