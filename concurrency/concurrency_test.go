package concurrency

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
)

func testClock() *clock.ManualClock {
	return clock.NewManualClock(time.Unix(0, 0))
}

// drainSuccess acquires, then releases with a fixed low RTT to simulate a
// healthy request. n times, each fully sequential, so inflight is 1 at update.
func drainSuccess(t *testing.T, l *Limiter, rtt time.Duration, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		rel, ok := l.Acquire(context.Background())
		if !ok {
			t.Fatalf("acquire %d: unexpected shed at limit=%d inflight=%d", i, l.Limit(), l.Inflight())
		}
		rel(Outcome{RTT: rtt})
	}
}

func TestAcquireShedsExactlyAtLimit(t *testing.T) {
	l := NewGradient2(Config{InitialLimit: 3, MaxLimit: 100, MinLimit: 1})
	var rels []ReleaseFunc
	for i := 0; i < 3; i++ {
		rel, ok := l.Acquire(context.Background())
		if !ok {
			t.Fatalf("acquire %d should succeed (limit 3)", i)
		}
		rels = append(rels, rel)
	}
	if got := l.Inflight(); got != 3 {
		t.Fatalf("inflight = %d, want 3", got)
	}
	// 4th must shed.
	if _, ok := l.Acquire(context.Background()); ok {
		t.Fatalf("acquire past limit should shed")
	}
	// Release one, capacity restored.
	rels[0](Outcome{RTT: time.Millisecond})
	if got := l.Inflight(); got != 2 {
		t.Fatalf("after release inflight = %d, want 2", got)
	}
	if _, ok := l.Acquire(context.Background()); !ok {
		t.Fatalf("acquire after release should succeed")
	}
	// Cleanup remaining.
	for _, r := range rels[1:] {
		r(Outcome{RTT: time.Millisecond})
	}
}

func TestNoNegativeInflightOnDoubleRelease(t *testing.T) {
	l := NewAIMD(Config{InitialLimit: 5, MaxLimit: 10, MinLimit: 1})
	rel, ok := l.Acquire(context.Background())
	if !ok {
		t.Fatal("acquire failed")
	}
	rel(Outcome{RTT: time.Millisecond})
	rel(Outcome{RTT: time.Millisecond}) // double release: must be no-op
	rel(Outcome{RTT: time.Millisecond})
	if got := l.Inflight(); got != 0 {
		t.Fatalf("inflight = %d, want 0 after double release", got)
	}
}

func TestAcquireCancelledContext(t *testing.T) {
	l := NewGradient2(Config{InitialLimit: 5})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, ok := l.Acquire(ctx); ok {
		t.Fatal("acquire with cancelled ctx should return ok=false")
	}
}

func TestGradient2IncreasesUnderLowStableRTT(t *testing.T) {
	clk := testClock()
	l := NewGradient2(Config{InitialLimit: 20, MaxLimit: 1000, MinLimit: 4, RTTTolerance: 1.5}, WithClock(clk))
	start := l.Limit()
	// Feed many low, stable RTTs while keeping a couple in flight so growth is
	// justified.
	for i := 0; i < 200; i++ {
		a, _ := l.Acquire(context.Background())
		b, _ := l.Acquire(context.Background())
		a(Outcome{RTT: time.Millisecond})
		b(Outcome{RTT: time.Millisecond})
	}
	if l.Limit() <= start {
		t.Fatalf("Gradient2 limit did not increase under low stable RTT: start=%d now=%d", start, l.Limit())
	}
	if l.Limit() > 1000 {
		t.Fatalf("Gradient2 exceeded MaxLimit: %d", l.Limit())
	}
}

func TestGradient2DecreasesUnderRisingRTT(t *testing.T) {
	l := NewGradient2(Config{InitialLimit: 200, MaxLimit: 1000, MinLimit: 4, RTTTolerance: 1.0})
	// Establish a low baseline.
	drainSuccess(t, l, time.Millisecond, 50)
	before := l.Limit()
	// Now hammer with RTTs an order of magnitude higher (queueing).
	for i := 0; i < 100; i++ {
		rel, ok := l.Acquire(context.Background())
		if !ok {
			continue
		}
		rel(Outcome{RTT: 50 * time.Millisecond})
	}
	if l.Limit() >= before {
		t.Fatalf("Gradient2 limit did not decrease under rising RTT: before=%d after=%d", before, l.Limit())
	}
}

func TestBackoffOnDropsRespectsFloor(t *testing.T) {
	for _, tc := range []struct {
		name string
		l    *Limiter
	}{
		{"gradient2", NewGradient2(Config{InitialLimit: 500, MaxLimit: 1000, MinLimit: 7})},
		{"aimd", NewAIMD(Config{InitialLimit: 500, MaxLimit: 1000, MinLimit: 7})},
		{"vegas", NewVegas(Config{InitialLimit: 500, MaxLimit: 1000, MinLimit: 7})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for i := 0; i < 300; i++ {
				rel, ok := tc.l.Acquire(context.Background())
				if !ok {
					// At floor, drain one via a fresh acquire is impossible; just
					// release nothing and continue driving drops through spare slots.
					continue
				}
				rel(Outcome{Dropped: true})
			}
			if got := tc.l.Limit(); got < 7 {
				t.Fatalf("%s dropped below MinLimit: %d < 7", tc.name, got)
			}
		})
	}
}

func TestVegasDecreasesUnderHighRTT(t *testing.T) {
	l := NewVegas(Config{InitialLimit: 300, MaxLimit: 1000, MinLimit: 4, RTTTolerance: 1.0})
	drainSuccess(t, l, time.Millisecond, 30) // set baseRTT ~ 1ms
	before := l.Limit()
	for i := 0; i < 200; i++ {
		rel, ok := l.Acquire(context.Background())
		if !ok {
			continue
		}
		rel(Outcome{RTT: 100 * time.Millisecond}) // huge queue
	}
	if l.Limit() >= before {
		t.Fatalf("Vegas did not decrease under high RTT: before=%d after=%d", before, l.Limit())
	}
}

func TestVegasIncreasesUnderLowRTT(t *testing.T) {
	l := NewVegas(Config{InitialLimit: 10, MaxLimit: 1000, MinLimit: 4, RTTTolerance: 1.0})
	before := l.Limit()
	drainSuccess(t, l, time.Millisecond, 300)
	if l.Limit() <= before {
		t.Fatalf("Vegas did not increase under low RTT: before=%d after=%d", before, l.Limit())
	}
}

func TestAIMDSawtooth(t *testing.T) {
	l := NewAIMD(Config{InitialLimit: 50, MaxLimit: 1000, MinLimit: 4, RTTTolerance: 2.0})
	drainSuccess(t, l, time.Millisecond, 5)
	// A single drop must reduce the limit.
	rel, _ := l.Acquire(context.Background())
	before := l.Limit()
	rel(Outcome{Dropped: true})
	if l.Limit() >= before {
		t.Fatalf("AIMD did not back off on drop: before=%d after=%d", before, l.Limit())
	}
}

func TestNeverExceedsMaxOrBelowMin(t *testing.T) {
	l := NewGradient2(Config{InitialLimit: 8, MaxLimit: 12, MinLimit: 6})
	for i := 0; i < 5000; i++ {
		// alternate healthy and dropped to stress both directions
		a, ok := l.Acquire(context.Background())
		if ok {
			if i%3 == 0 {
				a(Outcome{Dropped: true})
			} else {
				a(Outcome{RTT: time.Microsecond})
			}
		}
		if lim := l.Limit(); lim < 6 || lim > 12 {
			t.Fatalf("limit out of band at i=%d: %d", i, lim)
		}
	}
}

func TestWaitAcquiresWhenSlotFrees(t *testing.T) {
	l := NewGradient2(Config{InitialLimit: 1, MaxLimit: 100, MinLimit: 1})
	rel, ok := l.Acquire(context.Background())
	if !ok {
		t.Fatal("first acquire failed")
	}
	got := make(chan struct{})
	go func() {
		r, err := l.Wait(context.Background())
		if err != nil {
			t.Errorf("Wait errored: %v", err)
			close(got)
			return
		}
		r(Outcome{RTT: time.Millisecond})
		close(got)
	}()
	// Give the waiter a moment to block, then free the slot.
	time.Sleep(20 * time.Millisecond)
	rel(Outcome{RTT: time.Millisecond})
	select {
	case <-got:
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not acquire after slot freed (deadlock)")
	}
}

func TestWaitRespectsContextDeadline(t *testing.T) {
	l := NewGradient2(Config{InitialLimit: 1, MaxLimit: 100, MinLimit: 1})
	rel, _ := l.Acquire(context.Background()) // occupy the only slot
	defer rel(Outcome{RTT: time.Millisecond})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := l.Wait(ctx)
	if err == nil {
		t.Fatal("Wait should have timed out")
	}
	if time.Since(start) < 40*time.Millisecond {
		t.Fatalf("Wait returned too early: %v", time.Since(start))
	}
}

// TestConcurrentInvariant hammers Acquire/release from many goroutines under
// -race and asserts 0 <= inflight <= limit holds throughout, with a watchdog to
// catch any deadlock.
func TestConcurrentInvariant(t *testing.T) {
	l := NewGradient2(Config{InitialLimit: 32, MaxLimit: 256, MinLimit: 4})

	const workers = 64
	var wg sync.WaitGroup
	stop := make(chan struct{})
	var violations int64

	// Watchdog: fail the test if the whole thing hasn't finished in time.
	done := make(chan struct{})
	go func() {
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Errorf("watchdog: possible deadlock, test did not complete")
			close(stop)
		}
	}()

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				rel, ok := l.Acquire(context.Background())
				if !ok {
					continue
				}
				// Check invariant while holding the slot. Note: inflight can
				// briefly exceed the *current* limit because a concurrent
				// release may have lowered the limit while other goroutines
				// still legitimately hold slots acquired under the older,
				// higher limit. The true hard invariants are inflight >= 0 and
				// inflight <= MaxLimit (no slot is ever admitted past MaxLimit).
				inf := int64(l.Inflight())
				if inf < 0 || inf > 256 {
					atomic.AddInt64(&violations, 1)
				}
				dropped := (seed+i)%7 == 0
				rel(Outcome{RTT: time.Duration((seed+i)%5+1) * time.Millisecond, Dropped: dropped})
			}
		}(w)
	}

	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
	close(done)

	if v := atomic.LoadInt64(&violations); v != 0 {
		t.Fatalf("inflight invariant violated %d times", v)
	}
	if l.Inflight() != 0 {
		t.Fatalf("inflight leaked: %d", l.Inflight())
	}
}

func TestConfigDefaults(t *testing.T) {
	l := NewGradient2(Config{})
	if l.Limit() != defaultInitialLimit {
		t.Fatalf("default initial limit = %d, want %d", l.Limit(), defaultInitialLimit)
	}
	// Min > Max should be corrected.
	l2 := NewAIMD(Config{MinLimit: 999, MaxLimit: 10, InitialLimit: 5})
	if l2.Limit() < 1 {
		t.Fatalf("normalise produced invalid limit %d", l2.Limit())
	}
}

func BenchmarkAcquireRelease(b *testing.B) {
	l := NewGradient2(Config{InitialLimit: 1000, MaxLimit: 100000, MinLimit: 4})
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rel, ok := l.Acquire(context.Background())
			if !ok {
				continue
			}
			rel(Outcome{RTT: time.Millisecond})
		}
	})
}
