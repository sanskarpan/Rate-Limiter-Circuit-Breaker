package simulation_test

// Bulkhead scenario. The bulkhead's admission is synchronization-based (a
// counting semaphore), not virtual-clock-based, so determinism here comes from
// a release gate rather than the Sim clock: we hold exactly N slots, prove the
// (N+1)-th caller is rejected in non-blocking mode, then release and prove
// capacity is restored. The fault injector supplies the held operation body and
// records how many operations actually ran vs were rejected — giving a
// reproducible saturation timeline with no time.Sleep.

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/bulkhead"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/simulation"
)

func TestScenario_Bulkhead_SaturationAndRecovery(t *testing.T) {
	t.Parallel()
	const capacity = 3
	bh := bulkhead.New(capacity, 0 /* non-blocking */, bulkhead.WithName("sim-bh"))

	// A gate that keeps admitted operations parked until the test releases them,
	// so the bulkhead is deterministically saturated.
	release := make(chan struct{})
	var entered sync.WaitGroup
	entered.Add(capacity)

	// The injector body: signal entry, then block on the gate. Latency is 0 so
	// the injector doesn't itself sleep; blocking is via the release channel,
	// keeping the slot held. No failures scheduled — the op returns nil once
	// released.
	fi := simulation.NewFaultInjector(nil, simulation.Schedule{})
	body := fi.Fn()
	held := func(ctx context.Context) error {
		entered.Done()
		<-release
		return body(ctx)
	}

	// Fill all capacity slots.
	var wg sync.WaitGroup
	for i := 0; i < capacity; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = bh.Execute(context.Background(), held)
		}()
	}
	// Wait until all capacity slots are actually held (deterministic barrier,
	// not a timed sleep).
	entered.Wait()

	if got := bh.Inflight(); got != capacity {
		t.Fatalf("inflight = %d, want %d (saturated)", got, capacity)
	}

	// The bulkhead is full: a further non-blocking call must be rejected
	// immediately with ErrBulkheadFull, and its body must never run.
	extraRan := false
	err := bh.Execute(context.Background(), func(context.Context) error {
		extraRan = true
		return nil
	})
	if !errors.Is(err, bulkhead.ErrBulkheadFull) {
		t.Fatalf("saturated call err = %v, want ErrBulkheadFull", err)
	}
	if extraRan {
		t.Fatal("rejected call's body ran; bulkhead did not isolate")
	}
	if got := bh.Rejected(); got != 1 {
		t.Fatalf("rejected count = %d, want 1", got)
	}

	// Release the held operations; capacity is restored.
	close(release)
	wg.Wait()

	if got := bh.Inflight(); got != 0 {
		t.Fatalf("inflight after release = %d, want 0", got)
	}
	// After recovery a new call succeeds.
	if err := bh.Execute(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatalf("post-recovery call err = %v, want nil", err)
	}
	// The injector observed exactly the capacity operations that were admitted
	// and released (the rejected one never reached the body).
	if st := fi.Stats(); st.Total != capacity {
		t.Fatalf("injector saw %d ops, want %d (rejected op excluded)", st.Total, capacity)
	}
}

// TestScenario_Bulkhead_QueueTimeout exercises the blocking (maxWait>0) queue
// path: a caller that cannot get a slot within maxWait is rejected. maxWait uses
// real time in the bulkhead implementation, so we use a short real duration for
// the timeout while keeping the saturation itself deterministic via the gate.
func TestScenario_Bulkhead_QueueTimeout(t *testing.T) {
	t.Parallel()
	bh := bulkhead.New(1, 20*time.Millisecond, bulkhead.WithName("sim-bh-q"))

	release := make(chan struct{})
	var entered sync.WaitGroup
	entered.Add(1)
	go func() {
		_ = bh.Execute(context.Background(), func(ctx context.Context) error {
			entered.Done()
			<-release
			return nil
		})
	}()
	entered.Wait() // the single slot is now held (deterministic)

	// This caller must queue (maxWait>0) and then time out, since the slot is
	// held for the whole duration.
	start := time.Now()
	err := bh.Execute(context.Background(), func(context.Context) error { return nil })
	waited := time.Since(start)
	if !errors.Is(err, bulkhead.ErrBulkheadFull) {
		t.Fatalf("queued call err = %v, want ErrBulkheadFull (timeout)", err)
	}
	if waited < 20*time.Millisecond {
		t.Fatalf("returned after %v, expected to wait ~maxWait (20ms)", waited)
	}
	close(release)
}
