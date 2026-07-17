package loadshed

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
)

const (
	testTarget   = 5 * time.Millisecond
	testInterval = 100 * time.Millisecond
)

func testClock() *clock.ManualClock {
	return clock.NewManualClock(time.Unix(0, 0))
}

func newTestShedder(clk *clock.ManualClock) *Shedder {
	return New(Config{Target: testTarget, Interval: testInterval, PriorityStep: 2}, WithClock(clk))
}

// feedSojourn simulates one completed request with the given sojourn: Admit,
// advance the clock by d, then done(). It returns whether the request was
// admitted. The clock advance between Admit and done() is what the controller
// measures as the sojourn.
func feedSojourn(t *testing.T, s *Shedder, clk *clock.ManualClock, ctx context.Context, d time.Duration) bool {
	t.Helper()
	accept, done := s.Admit(ctx)
	if accept {
		clk.Advance(d)
		done()
	}
	return accept
}

func TestAdmitsFreelyBelowTarget(t *testing.T) {
	clk := testClock()
	s := newTestShedder(clk)
	ctx := context.Background()

	// All sojourns well below target: never shed, never enter dropping.
	for i := 0; i < 50; i++ {
		if !feedSojourn(t, s, clk, ctx, testTarget/5) {
			t.Fatalf("request %d shed while below target", i)
		}
		if s.Dropping() {
			t.Fatalf("entered dropping state at request %d with low sojourn", i)
		}
		clk.Advance(time.Millisecond)
	}
}

func TestEntersSheddingWhenSojournStaysAboveTarget(t *testing.T) {
	clk := testClock()
	s := newTestShedder(clk)
	ctx := context.Background()

	// One over-target sample marks the standing queue as starting now, but we
	// must stay above target for a full interval before shedding kicks in.
	feedSojourn(t, s, clk, ctx, testTarget*2)
	if s.Dropping() {
		t.Fatal("dropping too early: standing queue not yet older than interval")
	}

	// Keep feeding over-target samples while advancing past the interval.
	shed := false
	for i := 0; i < 20; i++ {
		clk.Advance(testInterval / 4)
		accept := feedSojourn(t, s, clk, ctx, testTarget*2)
		if !accept {
			shed = true
			break
		}
	}
	if !shed {
		t.Fatal("never shed despite sustained over-target sojourn beyond interval")
	}
	if !s.Dropping() {
		t.Fatal("expected controller in dropping state after a shed")
	}
}

func TestRecoversWhenLatencyDrops(t *testing.T) {
	clk := testClock()
	s := newTestShedder(clk)
	ctx := context.Background()

	// Drive it into the dropping state.
	feedSojourn(t, s, clk, ctx, testTarget*2)
	for i := 0; i < 20 && !s.Dropping(); i++ {
		clk.Advance(testInterval / 4)
		feedSojourn(t, s, clk, ctx, testTarget*2)
	}
	if !s.Dropping() {
		t.Fatal("setup: expected dropping state")
	}

	// Now feed a below-target sample: the standing queue clears (firstAbove
	// reset). The next admission observes a drained queue and leaves dropping.
	feedSojourn(t, s, clk, ctx, testTarget/10)
	// A subsequent admission should be accepted and clear the dropping state.
	clk.Advance(time.Millisecond)
	if !feedSojourn(t, s, clk, ctx, testTarget/10) {
		t.Fatal("request shed after latency recovered below target")
	}
	if s.Dropping() {
		t.Fatal("still dropping after latency recovered")
	}
}

func TestPriorityAdmitsHighWhileSheddingLow(t *testing.T) {
	clk := testClock()
	s := newTestShedder(clk)

	lowCtx := WithPriority(context.Background(), PriorityLow)
	critCtx := WithPriority(context.Background(), PriorityCritical)

	// Drive into dropping with a standing queue.
	feedSojourn(t, s, clk, context.Background(), testTarget*2)
	for i := 0; i < 20 && !s.Dropping(); i++ {
		clk.Advance(testInterval / 4)
		// Feed over-target latency via a neutral high-priority request so the
		// feedback loop keeps the queue standing.
		accept, done := s.Admit(critCtx)
		if accept {
			clk.Advance(testTarget * 2)
			done()
		}
	}
	if !s.Dropping() {
		t.Fatal("setup: expected dropping state")
	}

	// While dropping, low priority must be shed at least once and critical must
	// be admitted. Probe several times to cross a drop instant.
	sawLowShed := false
	critAlwaysAdmitted := true
	for i := 0; i < 20; i++ {
		clk.Advance(testInterval / 4)
		if lowAccept, _ := s.Admit(lowCtx); !lowAccept {
			sawLowShed = true
		}
		if critAccept, cdone := s.Admit(critCtx); critAccept {
			cdone()
		} else {
			critAlwaysAdmitted = false
		}
	}
	if !sawLowShed {
		t.Fatal("low-priority request never shed while dropping")
	}
	if !critAlwaysAdmitted {
		t.Fatal("critical-priority request was shed while dropping (priority not honoured)")
	}
}

func TestControlLawDropsGrowMoreFrequent(t *testing.T) {
	clk := testClock()
	// Small priorityStep so the priority gate never blocks default-priority
	// drops while count grows; we want to count raw drop events.
	s := New(Config{Target: testTarget, Interval: testInterval, PriorityStep: 1000}, WithClock(clk))
	ctx := context.Background()

	// Enter dropping.
	feedSojourn(t, s, clk, ctx, testTarget*2)
	for i := 0; i < 40 && !s.Dropping(); i++ {
		clk.Advance(testInterval / 4)
		feedSojourn(t, s, clk, ctx, testTarget*2)
	}
	if !s.Dropping() {
		t.Fatal("setup: expected dropping state")
	}

	// Under sustained overload, advance in fixed steps and count drops in the
	// first half vs the second half of a fixed horizon. The CoDel control law
	// (dropNext shrinks by interval/sqrt(count)) makes drops more frequent, so
	// the second half should see at least as many drops as the first.
	start := s.DropCount()
	step := testInterval / 10
	const half = 200

	countDrops := func(iters int) int {
		before := s.DropCount()
		for i := 0; i < iters; i++ {
			clk.Advance(step)
			// keep the queue standing above target
			accept, done := s.Admit(ctx)
			if accept {
				// admitted between drops: record over-target latency
				clk.Advance(testTarget * 2)
				done()
			}
		}
		return s.DropCount() - before
	}

	first := countDrops(half)
	second := countDrops(half)

	if s.DropCount() <= start {
		t.Fatalf("drop count did not grow under sustained overload: start=%d now=%d", start, s.DropCount())
	}
	if second < first {
		t.Fatalf("drops did not become more frequent: first-half=%d second-half=%d", first, second)
	}
}

func TestAdmitSimple(t *testing.T) {
	clk := testClock()
	s := newTestShedder(clk)
	if !s.AdmitSimple(context.Background()) {
		t.Fatal("AdmitSimple should admit when healthy")
	}
}

func TestDefaultsApplied(t *testing.T) {
	s := New(Config{})
	if s.target != DefaultTarget {
		t.Fatalf("target = %v, want default %v", s.target, DefaultTarget)
	}
	if s.interval != DefaultInterval {
		t.Fatalf("interval = %v, want default %v", s.interval, DefaultInterval)
	}
	if s.priorityStep != defaultPriorityStep {
		t.Fatalf("priorityStep = %d, want default %d", s.priorityStep, defaultPriorityStep)
	}
}

func TestPriorityFromContextDefault(t *testing.T) {
	if got := PriorityFromContext(context.Background()); got != PriorityDefault {
		t.Fatalf("default priority = %d, want %d", got, PriorityDefault)
	}
	ctx := WithPriority(context.Background(), 7)
	if got := PriorityFromContext(ctx); got != 7 {
		t.Fatalf("priority = %d, want 7", got)
	}
}

// TestConcurrentAdmitDone hammers the shedder from many goroutines under -race.
// A watchdog fails the test if it deadlocks.
func TestConcurrentAdmitDone(t *testing.T) {
	// Use a real clock here: goroutines race on wall time, exercising the mutex
	// and the sync.Once in done under the race detector.
	s := New(Config{Target: 100 * time.Microsecond, Interval: time.Millisecond})

	const goroutines = 64
	const perG = 500

	done := make(chan struct{})
	var wg sync.WaitGroup
	var admitted, shed int64

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			ctx := WithPriority(context.Background(), g%4-1) // priorities -1..2
			for i := 0; i < perG; i++ {
				accept, fin := s.Admit(ctx)
				if accept {
					atomic.AddInt64(&admitted, 1)
					// Call done twice to exercise the once-guard.
					fin()
					fin()
				} else {
					atomic.AddInt64(&shed, 1)
					fin() // no-op done, must be safe
				}
			}
		}(g)
	}

	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("deadlock: concurrent Admit/done did not finish in time")
	}

	total := atomic.LoadInt64(&admitted) + atomic.LoadInt64(&shed)
	if total != goroutines*perG {
		t.Fatalf("lost requests: total=%d want=%d", total, goroutines*perG)
	}
}

func BenchmarkAdmitHealthy(b *testing.B) {
	s := New(Config{Target: 5 * time.Millisecond, Interval: 100 * time.Millisecond})
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		accept, done := s.Admit(ctx)
		if accept {
			done()
		}
	}
}

func BenchmarkAdmitParallel(b *testing.B) {
	s := New(Config{Target: 5 * time.Millisecond, Interval: 100 * time.Millisecond})
	ctx := WithPriority(context.Background(), PriorityDefault)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			accept, done := s.Admit(ctx)
			if accept {
				done()
			}
		}
	})
}
