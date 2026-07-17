package fixedwindow_test

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskarpan/resilience/internal/clock"
	"github.com/sanskarpan/resilience/internal/testutil"
	"github.com/sanskarpan/resilience/ratelimit/fixedwindow"
)

// TestNew_RejectsBadConfig verifies that constructing a FixedWindowCounter with a
// non-positive window or limit fails fast with a clear validation panic rather
// than a divide-by-zero (ns/windowNs) on the first Allow (M-5).
func TestNew_RejectsBadConfig(t *testing.T) {
	check := func(name string, fn func()) {
		t.Helper()
		defer func() {
			r := recover()
			if r == nil {
				t.Fatalf("%s: expected a panic for bad config, got none", name)
			}
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

	check("New(5, 0)", func() { _ = fixedwindow.New(5, 0) })
	check("New(5, -1)", func() { _ = fixedwindow.New(5, -time.Second) })
	check("New(0, window)", func() { _ = fixedwindow.New(0, time.Second) })
	check("New(-1, window)", func() { _ = fixedwindow.New(-1, time.Second) })
}

func newClock() *clock.ManualClock {
	return clock.NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
}

func TestFixedWindow_AllowUpToLimit(t *testing.T) {
	clk := newClock()
	fw := fixedwindow.New(5, time.Second, fixedwindow.WithClock(clk))
	defer fw.Close()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		result := fw.Allow(ctx, "key")
		if !result.Allowed {
			t.Fatalf("request %d should be allowed (limit=5)", i+1)
		}
		if result.Remaining != 5-i-1 {
			t.Fatalf("request %d: expected remaining=%d, got %d", i+1, 5-i-1, result.Remaining)
		}
	}
}

func TestFixedWindow_DenyBeyondLimit(t *testing.T) {
	clk := newClock()
	fw := fixedwindow.New(3, time.Second, fixedwindow.WithClock(clk))
	defer fw.Close()
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		fw.Allow(ctx, "key") //nolint:errcheck
	}
	result := fw.Allow(ctx, "key")
	if result.Allowed {
		t.Fatal("4th request should be denied")
	}
	if result.RetryAfter <= 0 {
		t.Fatal("RetryAfter should be positive when denied")
	}
}

func TestFixedWindow_ResetAtWindowBoundary(t *testing.T) {
	clk := newClock()
	fw := fixedwindow.New(2, time.Second, fixedwindow.WithClock(clk))
	defer fw.Close()
	ctx := context.Background()

	fw.Allow(ctx, "key") //nolint:errcheck
	fw.Allow(ctx, "key") //nolint:errcheck
	if fw.Allow(ctx, "key").Allowed {
		t.Fatal("should be denied in window 1")
	}

	// Advance past window boundary
	clk.Advance(time.Second)
	result := fw.Allow(ctx, "key")
	if !result.Allowed {
		t.Fatal("should be allowed after window reset")
	}
}

func TestFixedWindow_BoundaryBurstPossible(t *testing.T) {
	// This test DOCUMENTS THE KNOWN LIMITATION of fixed window.
	// At a window boundary, it's possible to make 2x the limit in rapid succession:
	// - limit-1 requests at end of window 1 (just before boundary)
	// - limit requests at start of window 2 (just after boundary)
	// Total: up to 2*limit requests in a short window.
	clk := clock.NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 999900000, time.UTC)) // 100µs before boundary
	fw := fixedwindow.New(10, time.Second, fixedwindow.WithClock(clk))
	defer fw.Close()
	ctx := context.Background()

	// Send limit requests just before boundary
	allowed1 := 0
	for i := 0; i < 10; i++ {
		if fw.Allow(ctx, "key").Allowed {
			allowed1++
		}
	}

	// Cross the boundary
	clk.Advance(200 * time.Microsecond)

	// Send limit more requests after boundary
	allowed2 := 0
	for i := 0; i < 10; i++ {
		if fw.Allow(ctx, "key").Allowed {
			allowed2++
		}
	}

	total := allowed1 + allowed2
	// The total allowed can be up to 2*limit because window reset
	// This is the documented limitation of fixed window counter.
	t.Logf("Boundary burst: %d allowed before boundary + %d after = %d total (limit=10, max=20)", allowed1, allowed2, total)
	if total > 20 {
		t.Errorf("more than 2x limit allowed at boundary: %d", total)
	}
}

func TestFixedWindow_RetryAfterIsCorrect(t *testing.T) {
	clk := newClock()
	fw := fixedwindow.New(1, time.Second, fixedwindow.WithClock(clk))
	defer fw.Close()
	ctx := context.Background()

	fw.Allow(ctx, "key") //nolint:errcheck
	result := fw.Allow(ctx, "key")
	if result.Allowed {
		t.Fatal("should be denied")
	}
	// RetryAfter should be close to 1 second (remaining window time)
	if result.RetryAfter <= 0 || result.RetryAfter > time.Second {
		t.Errorf("unexpected RetryAfter: %v", result.RetryAfter)
	}
}

func TestFixedWindow_Concurrent_NoRace(t *testing.T) {
	fw := fixedwindow.New(1000, time.Minute, fixedwindow.WithClock(clock.RealClock{}))
	defer fw.Close()
	ctx := context.Background()

	var wg sync.WaitGroup
	var allowed atomic.Int64
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if fw.Allow(ctx, "key").Allowed {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()

	if allowed.Load() != 500 {
		t.Fatalf("all 500 should be allowed (limit=1000), got %d", allowed.Load())
	}
}

func TestFixedWindow_Close_NoLeak(t *testing.T) {
	lc := testutil.NewLeakChecker(t)
	defer lc.Check()
	fw := fixedwindow.New(10, time.Second)
	fw.Close() //nolint:errcheck
}

func TestFixedWindow_MultipleKeys_Isolation(t *testing.T) {
	clk := newClock()
	fw := fixedwindow.New(1, time.Second, fixedwindow.WithClock(clk))
	defer fw.Close()
	ctx := context.Background()

	fw.Allow(ctx, "a") //nolint:errcheck
	if fw.Allow(ctx, "a").Allowed {
		t.Fatal("key 'a' should be exhausted")
	}
	if !fw.Allow(ctx, "b").Allowed {
		t.Fatal("key 'b' should still be available")
	}
}

func TestFixedWindow_Reset(t *testing.T) {
	clk := newClock()
	fw := fixedwindow.New(1, time.Second, fixedwindow.WithClock(clk))
	defer fw.Close()
	ctx := context.Background()

	fw.Allow(ctx, "key") //nolint:errcheck
	if fw.Allow(ctx, "key").Allowed {
		t.Fatal("should be denied")
	}

	fw.Reset(ctx, "key") //nolint:errcheck
	if !fw.Allow(ctx, "key").Allowed {
		t.Fatal("should be allowed after reset")
	}
}

func BenchmarkFixedWindow_Allow(b *testing.B) {
	fw := fixedwindow.New(1000000, time.Minute, fixedwindow.WithClock(clock.RealClock{}))
	defer fw.Close()
	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		fw.Allow(ctx, "bench")
	}
}

func BenchmarkFixedWindow_Allow_Parallel(b *testing.B) {
	fw := fixedwindow.New(1000000000, time.Minute, fixedwindow.WithClock(clock.RealClock{}))
	defer fw.Close()
	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			fw.Allow(ctx, "bench")
		}
	})
}

func TestFixedWindow_Wait_Success(t *testing.T) {
	clk := newClock()
	fw := fixedwindow.New(1, time.Second, fixedwindow.WithClock(clk))
	defer fw.Close()

	err := fw.Wait(context.Background(), "key")
	if err != nil {
		t.Fatalf("Wait should succeed: %v", err)
	}
}

func TestFixedWindow_WaitN(t *testing.T) {
	clk := newClock()
	fw := fixedwindow.New(5, time.Second, fixedwindow.WithClock(clk))
	defer fw.Close()

	err := fw.WaitN(context.Background(), "key", 3)
	if err != nil {
		t.Fatalf("WaitN(3) should succeed: %v", err)
	}

	state := fw.Peek(context.Background(), "key")
	if state.Remaining != 2 {
		t.Fatalf("expected 2 remaining, got %d", state.Remaining)
	}
}

func TestFixedWindow_AllowN(t *testing.T) {
	clk := newClock()
	fw := fixedwindow.New(5, time.Second, fixedwindow.WithClock(clk))
	defer fw.Close()
	ctx := context.Background()

	result := fw.AllowN(ctx, "key", 3)
	if !result.Allowed {
		t.Fatal("AllowN(3) should be allowed")
	}
	if result.Remaining != 2 {
		t.Fatalf("expected 2 remaining, got %d", result.Remaining)
	}
}

func TestFixedWindow_AllowN_ExceedsLimit(t *testing.T) {
	clk := newClock()
	fw := fixedwindow.New(5, time.Second, fixedwindow.WithClock(clk))
	defer fw.Close()
	ctx := context.Background()

	result := fw.AllowN(ctx, "key", 6)
	if result.Allowed {
		t.Fatal("AllowN(6) should be denied when limit is 5")
	}
}

func TestFixedWindow_Peek(t *testing.T) {
	clk := newClock()
	fw := fixedwindow.New(10, time.Second, fixedwindow.WithClock(clk))
	defer fw.Close()
	ctx := context.Background()

	fw.Allow(ctx, "key") //nolint:errcheck
	fw.Allow(ctx, "key") //nolint:errcheck

	state := fw.Peek(ctx, "key")
	if state.Remaining != 8 {
		t.Fatalf("expected 8 remaining, got %d", state.Remaining)
	}
	if state.Limit != 10 {
		t.Fatalf("expected limit 10, got %d", state.Limit)
	}
}

func TestFixedWindow_Peek_DoesNotConsume(t *testing.T) {
	clk := newClock()
	fw := fixedwindow.New(2, time.Second, fixedwindow.WithClock(clk))
	defer fw.Close()
	ctx := context.Background()

	state1 := fw.Peek(ctx, "key")
	state2 := fw.Peek(ctx, "key")
	if state1.Remaining != state2.Remaining {
		t.Fatal("Peek should not consume tokens")
	}
}

func TestFixedWindow_Peek_DifferentKeys(t *testing.T) {
	clk := newClock()
	fw := fixedwindow.New(1, time.Second, fixedwindow.WithClock(clk))
	defer fw.Close()
	ctx := context.Background()

	fw.Allow(ctx, "key1") //nolint:errcheck

	state1 := fw.Peek(ctx, "key1")
	state2 := fw.Peek(ctx, "key2")

	if state1.Remaining != 0 {
		t.Fatalf("key1 should have 0 remaining, got %d", state1.Remaining)
	}
	if state2.Remaining != 1 {
		t.Fatalf("key2 should have 1 remaining, got %d", state2.Remaining)
	}
}

func TestFixedWindow_InvalidKey(t *testing.T) {
	clk := newClock()
	fw := fixedwindow.New(5, time.Second, fixedwindow.WithClock(clk))
	defer fw.Close()
	ctx := context.Background()

	result := fw.Allow(ctx, "")
	if result.Allowed {
		t.Fatal("empty key should be denied")
	}
}

func TestFixedWindow_InvalidN(t *testing.T) {
	clk := newClock()
	fw := fixedwindow.New(5, time.Second, fixedwindow.WithClock(clk))
	defer fw.Close()
	ctx := context.Background()

	result := fw.AllowN(ctx, "key", 0)
	if result.Allowed {
		t.Fatal("n=0 should be denied")
	}

	result = fw.AllowN(ctx, "key", -1)
	if result.Allowed {
		t.Fatal("n=-1 should be denied")
	}
}

func TestFixedWindow_Close_Idempotent(t *testing.T) {
	fw := fixedwindow.New(10, time.Second)
	fw.Close() //nolint:errcheck
	fw.Close() //nolint:errcheck
	fw.Close() //nolint:errcheck
}

func TestFixedWindow_String(t *testing.T) {
	fw := fixedwindow.New(100, time.Minute)
	defer fw.Close()
	str := fw.String()
	if str == "" {
		t.Fatal("String should not be empty")
	}
	t.Logf("String(): %s", str)
}
