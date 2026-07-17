package circuitbreaker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/store"
)

// errBoom is a stand-in failure returned by fns under test.
var errBoom = errors.New("boom")

func failFn(context.Context) error { return errBoom }
func okFn(context.Context) error   { return nil }

// newTestBreaker builds a DistributedCircuitBreaker over a memory store with all
// script emulations registered, driven by a shared ManualClock so the
// Open→HalfOpen timeout can be advanced deterministically.
func newTestBreaker(t *testing.T, cfg Config) (*DistributedCircuitBreaker, *clock.ManualClock, store.Store) {
	t.Helper()
	mc := clock.NewManualClock(time.Unix(1_700_000_000, 0))
	cfg.Clock = mc
	s := store.NewMemoryWithScripts()
	t.Cleanup(func() { _ = s.Close() })
	d := NewDistributed("test", s, cfg)
	return d, mc, s
}

func TestDistributed_ClosedAllowsAndStaysClosed(t *testing.T) {
	d, _, _ := newTestBreaker(t, Config{FailureThreshold: 3})
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if err := d.Execute(ctx, okFn); err != nil {
			t.Fatalf("call %d: unexpected error %v", i, err)
		}
	}
	if got := d.State(ctx); got != StateClosed {
		t.Fatalf("state = %v, want closed", got)
	}
}

func TestDistributed_OpensAfterFailureThreshold(t *testing.T) {
	d, _, _ := newTestBreaker(t, Config{FailureThreshold: 3})
	ctx := context.Background()

	// 2 failures should NOT open (threshold is 3).
	for i := 0; i < 2; i++ {
		if err := d.Execute(ctx, failFn); !errors.Is(err, errBoom) {
			t.Fatalf("failure %d: got %v, want errBoom", i, err)
		}
	}
	if got := d.State(ctx); got != StateClosed {
		t.Fatalf("after 2 failures state = %v, want closed", got)
	}

	// 3rd failure trips it.
	if err := d.Execute(ctx, failFn); !errors.Is(err, errBoom) {
		t.Fatalf("3rd failure: got %v", err)
	}
	if got := d.State(ctx); got != StateOpen {
		t.Fatalf("after 3 failures state = %v, want open", got)
	}
}

func TestDistributed_OpenRejectsWithCircuitOpen(t *testing.T) {
	d, _, _ := newTestBreaker(t, Config{FailureThreshold: 1})
	ctx := context.Background()

	_ = d.Execute(ctx, failFn) // trips (threshold 1)
	if got := d.State(ctx); got != StateOpen {
		t.Fatalf("state = %v, want open", got)
	}

	ran := false
	err := d.Execute(ctx, func(context.Context) error { ran = true; return nil })
	if ran {
		t.Fatal("fn ran while circuit open; expected rejection")
	}
	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("err = %v, want ErrCircuitOpen", err)
	}
	var ce *CircuitError
	if !errors.As(err, &ce) || ce.State != StateOpen {
		t.Fatalf("expected CircuitError with State=open, got %v", err)
	}
}

func TestDistributed_OpenTransitionsToHalfOpenAfterTimeout(t *testing.T) {
	d, mc, _ := newTestBreaker(t, Config{FailureThreshold: 1, OpenTimeout: 30 * time.Second})
	ctx := context.Background()

	_ = d.Execute(ctx, failFn)
	if got := d.State(ctx); got != StateOpen {
		t.Fatalf("state = %v, want open", got)
	}

	// Not enough time elapsed yet.
	mc.Advance(29 * time.Second)
	if got := d.State(ctx); got != StateOpen {
		t.Fatalf("before timeout state = %v, want open", got)
	}

	// Past OpenTimeout: the next read/acquire lazily promotes to half-open.
	mc.Advance(2 * time.Second)
	if got := d.State(ctx); got != StateHalfOpen {
		t.Fatalf("after timeout state = %v, want half-open", got)
	}
}

func TestDistributed_HalfOpenSuccessCloses(t *testing.T) {
	d, mc, _ := newTestBreaker(t, Config{
		FailureThreshold: 1,
		OpenTimeout:      30 * time.Second,
		SuccessThreshold: 2,
	})
	ctx := context.Background()

	_ = d.Execute(ctx, failFn) // open
	mc.Advance(31 * time.Second)

	// First probe success — still half-open (SuccessThreshold 2).
	if err := d.Execute(ctx, okFn); err != nil {
		t.Fatalf("probe 1: %v", err)
	}
	if got := d.State(ctx); got != StateHalfOpen {
		t.Fatalf("after 1 success state = %v, want half-open", got)
	}

	// Second probe success — closes.
	if err := d.Execute(ctx, okFn); err != nil {
		t.Fatalf("probe 2: %v", err)
	}
	if got := d.State(ctx); got != StateClosed {
		t.Fatalf("after 2 successes state = %v, want closed", got)
	}
}

func TestDistributed_HalfOpenFailureReopens(t *testing.T) {
	d, mc, _ := newTestBreaker(t, Config{FailureThreshold: 1, OpenTimeout: 30 * time.Second})
	ctx := context.Background()

	_ = d.Execute(ctx, failFn) // open
	mc.Advance(31 * time.Second)
	if got := d.State(ctx); got != StateHalfOpen {
		t.Fatalf("state = %v, want half-open", got)
	}

	// A probe failure reopens the circuit.
	if err := d.Execute(ctx, failFn); !errors.Is(err, errBoom) {
		t.Fatalf("probe failure: got %v", err)
	}
	if got := d.State(ctx); got != StateOpen {
		t.Fatalf("after probe failure state = %v, want open", got)
	}
}

func TestDistributed_HalfOpenProbeLimitRejectsExcess(t *testing.T) {
	d, mc, _ := newTestBreaker(t, Config{
		FailureThreshold:    1,
		OpenTimeout:         30 * time.Second,
		HalfOpenMaxRequests: 1,
		SuccessThreshold:    5, // keep it in half-open across probes
	})
	ctx := context.Background()

	_ = d.Execute(ctx, failFn) // open
	mc.Advance(31 * time.Second)

	// Hold the single probe slot open with a blocking fn, then attempt a second
	// concurrent probe which must be rejected with ErrTooManyRequests.
	release := make(chan struct{})
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- d.Execute(ctx, func(context.Context) error {
			close(started)
			<-release
			return nil
		})
	}()
	<-started

	err := d.Execute(ctx, okFn)
	if !errors.Is(err, ErrTooManyRequests) {
		t.Fatalf("second concurrent probe: got %v, want ErrTooManyRequests", err)
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("first probe: %v", err)
	}
}

func TestDistributed_SnapshotReportsCounts(t *testing.T) {
	d, _, _ := newTestBreaker(t, Config{FailureThreshold: 5})
	ctx := context.Background()

	_ = d.Execute(ctx, failFn)
	_ = d.Execute(ctx, failFn)

	snap := d.Snapshot(ctx)
	if snap.State != StateClosed {
		t.Fatalf("snapshot state = %v, want closed", snap.State)
	}
	if snap.Failures != 2 {
		t.Fatalf("snapshot failures = %d, want 2", snap.Failures)
	}
	if snap.Name != "test" {
		t.Fatalf("snapshot name = %q", snap.Name)
	}
}

// TestDistributed_SharedStateAcrossInstances verifies the core property with the
// MEMORY store: two independent DistributedCircuitBreaker values over the SAME
// store + name share one state machine. Instance A trips the breaker; instance B
// (which never saw a failure) immediately observes Open. (The integration test
// does the same over real Redis.)
func TestDistributed_SharedStateAcrossInstances(t *testing.T) {
	mc := clock.NewManualClock(time.Unix(1_700_000_000, 0))
	s := store.NewMemoryWithScripts()
	t.Cleanup(func() { _ = s.Close() })

	cfg := Config{FailureThreshold: 3, Clock: mc}
	a := NewDistributed("shared", s, cfg)
	b := NewDistributed("shared", s, cfg)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		_ = a.Execute(ctx, failFn)
	}
	if got := a.State(ctx); got != StateOpen {
		t.Fatalf("instance A state = %v, want open", got)
	}
	// Instance B shares the state and must also see Open.
	if got := b.State(ctx); got != StateOpen {
		t.Fatalf("instance B state = %v, want open (shared)", got)
	}
	ran := false
	err := b.Execute(ctx, func(context.Context) error { ran = true; return nil })
	if ran || !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("instance B: ran=%v err=%v, want rejected with ErrCircuitOpen", ran, err)
	}
}

func TestDistributed_ContextCancellationNotCountedAsFailure(t *testing.T) {
	d, _, _ := newTestBreaker(t, Config{FailureThreshold: 1})
	ctx := context.Background()

	// A caller-cancelled call returns context.Canceled but must NOT trip.
	err := d.Execute(ctx, func(context.Context) error { return context.Canceled })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
	if got := d.State(ctx); got != StateClosed {
		t.Fatalf("state = %v, want closed (cancellation not a failure)", got)
	}
}
