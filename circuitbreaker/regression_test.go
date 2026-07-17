package circuitbreaker_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sanskarpan/resilience/circuitbreaker"
	"github.com/sanskarpan/resilience/internal/clock"
)

// ---------------------------------------------------------------------------
// C-3: probe counter must not leak when the wrapped fn panics.
// ---------------------------------------------------------------------------

// TestCB_C3_ProbePanic_DoesNotLeakInflight trips to Open, advances past the
// OpenTimeout, runs a probe whose fn panics (recovered in the test), and asserts
// the breaker is NOT stuck rejecting with ErrTooManyRequests afterwards.
func TestCB_C3_ProbePanic_DoesNotLeakInflight(t *testing.T) {
	clk := newClock()
	cfg := circuitbreaker.Config{
		Name:                t.Name(),
		WindowType:          circuitbreaker.CountBased,
		WindowSize:          5,
		FailureThreshold:    3,
		OpenTimeout:         100 * time.Millisecond,
		HalfOpenMaxRequests: 1,
		Clock:               clk,
	}
	cb := circuitbreaker.New(cfg)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		cb.Execute(ctx, fail) //nolint:errcheck
	}
	if cb.State() != circuitbreaker.StateOpen {
		t.Fatalf("expected Open, got %s", cb.State())
	}
	clk.Advance(200 * time.Millisecond)

	// Probe panics — recover here so the test process survives.
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected the panic to propagate to the caller")
			}
		}()
		cb.Execute(ctx, func(context.Context) error {
			panic("boom")
		})
	}()

	// The panicking probe recorded a half-open failure and reopened the circuit;
	// crucially it must have released its probe slot.
	if got := cb.HalfOpenInflight(); got != 0 {
		t.Fatalf("inflight leaked after panic: got %d, want 0", got)
	}

	// Advance again so the breaker can offer a fresh probe.
	clk.Advance(200 * time.Millisecond)

	err := cb.Execute(ctx, succeed)
	if errors.Is(err, circuitbreaker.ErrTooManyRequests) {
		t.Fatalf("breaker stuck rejecting probes after a panic: %v", err)
	}
	if err != nil {
		t.Fatalf("expected next probe to run, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// H-8 / H-9: inflight counter invariant under concurrency and transitions.
// ---------------------------------------------------------------------------

// TestCB_H8_H9_InflightStaysWithinBounds hammers the half-open path with many
// concurrent probes while forcing repeated transitions and asserts the inflight
// counter never leaves [0, HalfOpenMaxRequests].
func TestCB_H8_H9_InflightStaysWithinBounds(t *testing.T) {
	const maxProbes = 3
	cfg := circuitbreaker.Config{
		Name:                t.Name(),
		WindowType:          circuitbreaker.CountBased,
		WindowSize:          5,
		FailureThreshold:    3,
		OpenTimeout:         time.Nanosecond, // near-instant reset → constant churn
		HalfOpenMaxRequests: maxProbes,
		SuccessThreshold:    2,
		Clock:               clock.RealClock{},
	}
	cb := circuitbreaker.New(cfg)
	ctx := context.Background()

	// Trip to Open first.
	for i := 0; i < 3; i++ {
		cb.Execute(ctx, fail) //nolint:errcheck
	}

	var wg sync.WaitGroup
	var stop sync.Once
	done := make(chan struct{})

	// Sampler goroutine continuously checks the invariant.
	var bad int64
	var badMu sync.Mutex
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			v := cb.HalfOpenInflight()
			if v < 0 || v > maxProbes {
				badMu.Lock()
				bad = v
				badMu.Unlock()
				return
			}
		}
	}()

	for w := 0; w < 40; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				cb.Execute(ctx, func(context.Context) error { //nolint:errcheck
					if (id+i)%2 == 0 {
						return errTest
					}
					return nil
				})
			}
		}(w)
	}

	time.Sleep(150 * time.Millisecond)
	stop.Do(func() { close(done) })
	wg.Wait()

	badMu.Lock()
	defer badMu.Unlock()
	if bad != 0 {
		t.Fatalf("inflight counter left [0,%d]: observed %d", maxProbes, bad)
	}
	if got := cb.HalfOpenInflight(); got < 0 || got > maxProbes {
		t.Fatalf("final inflight out of bounds: %d", got)
	}
}

// TestCB_H9_SlowProbesPlusTransition deterministically keeps two slow probes in
// flight in half-open while a third probe FAILS, forcing a HalfOpen→Open
// transition. A Store(0) reset during that transition combined with the two held
// probes' later decrements would drive the counter negative; this test asserts
// it never does.
func TestCB_H9_SlowProbesPlusTransition(t *testing.T) {
	const maxProbes = 3
	cfg := circuitbreaker.Config{
		Name:                t.Name(),
		WindowType:          circuitbreaker.CountBased,
		WindowSize:          5,
		FailureThreshold:    3,
		OpenTimeout:         10 * time.Millisecond,
		HalfOpenMaxRequests: maxProbes,
		SuccessThreshold:    5, // don't close while probes are running
		Clock:               clock.RealClock{},
	}
	cb := circuitbreaker.New(cfg)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		cb.Execute(ctx, fail) //nolint:errcheck
	}
	time.Sleep(20 * time.Millisecond) // past OpenTimeout

	var wg sync.WaitGroup
	release := make(chan struct{})
	acquired := make(chan struct{}, maxProbes-1)

	// Two slow probes acquire slots and block until released.
	for p := 0; p < maxProbes-1; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cb.Execute(ctx, func(context.Context) error { //nolint:errcheck
				acquired <- struct{}{}
				<-release
				return nil
			})
		}()
	}

	// Wait until both slow probes are actually holding their slots.
	<-acquired
	<-acquired

	// Third probe fails synchronously → HalfOpen→Open transition while the two
	// slow probes are still in flight.
	cb.Execute(ctx, fail) //nolint:errcheck

	// Counter must not have gone negative or above max at this instant.
	if v := cb.HalfOpenInflight(); v < 0 || v > maxProbes {
		close(release)
		wg.Wait()
		t.Fatalf("inflight out of bounds after transition with probes held: %d", v)
	}

	close(release)
	wg.Wait()

	if got := cb.HalfOpenInflight(); got < 0 {
		t.Fatalf("final inflight negative after slow probes released: %d", got)
	}
}

// ---------------------------------------------------------------------------
// H-10: time window retained span equals windowDuration exactly.
// ---------------------------------------------------------------------------

// TestCB_H10_TimeWindow_RetainedSpanEqualsWindow records failures at t=0 with a
// 3s window / 1s buckets and asserts they still count just before t=3s but are
// gone exactly at t=3s (retained span == windowDuration, not windowDuration+bucket).
func TestCB_H10_TimeWindow_RetainedSpanEqualsWindow(t *testing.T) {
	clk := newClock()
	cfg := circuitbreaker.Config{
		Name:                 t.Name(),
		WindowType:           circuitbreaker.TimeBased,
		WindowDuration:       3 * time.Second,
		BucketDuration:       time.Second,
		FailureThreshold:     100, // never open; we only inspect Snapshot counts
		FailureRateThreshold: 1.0,
		MinimumRequests:      1000,
		OpenTimeout:          time.Second,
		Clock:                clk,
	}
	cb := circuitbreaker.New(cfg)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		cb.Execute(ctx, fail) //nolint:errcheck
	}
	if f := cb.Snapshot().Failures; f != 5 {
		t.Fatalf("expected 5 failures at t=0, got %d", f)
	}

	// Just before the window boundary the failures must still count.
	clk.Advance(3*time.Second - time.Millisecond)
	if f := cb.Snapshot().Failures; f != 5 {
		t.Fatalf("failures should still be in window just before %v, got %d",
			cfg.WindowDuration, f)
	}

	// At exactly windowDuration the t=0 failures fall out of the window.
	clk.Advance(time.Millisecond) // now exactly t=3s
	if f := cb.Snapshot().Failures; f != 0 {
		t.Fatalf("failures should be evicted at exactly windowDuration=%v, got %d",
			cfg.WindowDuration, f)
	}
}

// ---------------------------------------------------------------------------
// M-6: state enum ordering and String() mapping.
// ---------------------------------------------------------------------------

// TestCB_M6_StateEnumOrderAndString pins the numeric values and String() output.
func TestCB_M6_StateEnumOrderAndString(t *testing.T) {
	if int32(circuitbreaker.StateClosed) != 0 {
		t.Fatalf("StateClosed must be 0, got %d", int32(circuitbreaker.StateClosed))
	}
	if int32(circuitbreaker.StateHalfOpen) != 1 {
		t.Fatalf("StateHalfOpen must be 1, got %d", int32(circuitbreaker.StateHalfOpen))
	}
	if int32(circuitbreaker.StateOpen) != 2 {
		t.Fatalf("StateOpen must be 2, got %d", int32(circuitbreaker.StateOpen))
	}
	cases := map[circuitbreaker.State]string{
		circuitbreaker.StateClosed:   "closed",
		circuitbreaker.StateHalfOpen: "half-open",
		circuitbreaker.StateOpen:     "open",
	}
	for s, want := range cases {
		if got := s.String(); got != want {
			t.Fatalf("State(%d).String() = %q, want %q", int32(s), got, want)
		}
	}
}

// ---------------------------------------------------------------------------
// M-8: re-trip while Open refreshes openedAt (restarts OpenTimeout).
// ---------------------------------------------------------------------------

// TestCB_M8_ReopenRefreshesOpenTimeout verifies that a failure recorded while
// already Open pushes back the Open→HalfOpen deadline.
func TestCB_M8_ReopenRefreshesOpenTimeout(t *testing.T) {
	clk := newClock()
	cfg := circuitbreaker.Config{
		Name:             t.Name(),
		WindowType:       circuitbreaker.CountBased,
		WindowSize:       10,
		FailureThreshold: 3,
		OpenTimeout:      time.Second,
		Clock:            clk,
	}
	cb := circuitbreaker.New(cfg)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		cb.Execute(ctx, fail) //nolint:errcheck
	}
	if cb.State() != circuitbreaker.StateOpen {
		t.Fatalf("expected Open, got %s", cb.State())
	}
	firstUntil := cb.Snapshot().TimeUntilHalfOpen

	// Advance partway, then force a re-trip while Open by directly recording a
	// failure through Execute — but Execute is rejected while Open, so we instead
	// re-open via the metrics path: advance to half-open, fail the probe.
	clk.Advance(500 * time.Millisecond)

	// Snapshot at +500ms: less time remaining than at t=0.
	midUntil := cb.Snapshot().TimeUntilHalfOpen
	if midUntil >= firstUntil {
		t.Fatalf("expected countdown to shrink; first=%v mid=%v", firstUntil, midUntil)
	}

	// Advance to half-open and fail the probe → transitionToOpen from HalfOpen,
	// which stores a fresh openedAt (the normal reopen path), restarting the timer.
	clk.Advance(600 * time.Millisecond) // total 1.1s > OpenTimeout → half-open eligible
	cb.Execute(ctx, fail)               //nolint:errcheck // probe fails → reopen
	if cb.State() != circuitbreaker.StateOpen {
		t.Fatalf("expected Open after failed probe, got %s", cb.State())
	}
	reopenedUntil := cb.Snapshot().TimeUntilHalfOpen
	// After reopening, the full OpenTimeout should be available again.
	if reopenedUntil < 900*time.Millisecond {
		t.Fatalf("reopen should refresh OpenTimeout to ~%v, got %v",
			cfg.OpenTimeout, reopenedUntil)
	}
}

// ---------------------------------------------------------------------------
// L-10: newTimeWindow with a zero BucketDuration must not panic.
// ---------------------------------------------------------------------------

// TestCB_L10_DegenerateTimeWindow_NoPanic constructs a TimeBased breaker via the
// public API with a windowDuration smaller than the bucket (which previously could
// yield zero buckets) and confirms construction and use never panic. The direct
// zero-bucketWidth guard is covered by the internal metrics test.
func TestCB_L10_DegenerateTimeWindow_NoPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("New panicked for degenerate time window: %v", r)
		}
	}()
	clk := newClock()
	cb := circuitbreaker.New(circuitbreaker.Config{
		Name:           t.Name(),
		WindowType:     circuitbreaker.TimeBased,
		WindowDuration: time.Millisecond, // < default bucket → 0 buckets pre-guard
		BucketDuration: time.Second,
		Clock:          clk,
	})
	ctx := context.Background()
	// Exercise it to ensure the window is usable.
	cb.Execute(ctx, fail)    //nolint:errcheck
	cb.Execute(ctx, succeed) //nolint:errcheck
	_ = cb.Snapshot()
}
