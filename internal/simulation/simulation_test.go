package simulation_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/simulation"
)

// TestSim_VirtualTimeOnlyMovesOnAdvance verifies the core determinism property:
// virtual time is frozen until the test advances it.
func TestSim_VirtualTimeOnlyMovesOnAdvance(t *testing.T) {
	t.Parallel()
	sim := simulation.NewSim()
	if got := sim.Now(); !got.Equal(simulation.Epoch) {
		t.Fatalf("Now = %v, want epoch %v", got, simulation.Epoch)
	}
	sim.Advance(5 * time.Second)
	if got := sim.Now().Sub(simulation.Epoch); got != 5*time.Second {
		t.Fatalf("after advance elapsed = %v, want 5s", got)
	}
}

// TestFaultInjector_DeterministicLatency proves injected latency resolves
// against the virtual clock, not wall time.
func TestFaultInjector_DeterministicLatency(t *testing.T) {
	t.Parallel()
	sim := simulation.NewSim()
	fi := simulation.NewFaultInjector(sim.Clock(), simulation.Schedule{
		Latency: 100 * time.Millisecond,
	})

	h := sim.RunOp(context.Background(), fi.Fn())
	// The op is blocked in its injected-latency sleep; it has not returned.
	if res := h.Poll(); res.Done {
		t.Fatal("op finished before latency elapsed; latency not virtual-clock driven")
	}
	// Not enough time yet.
	sim.Advance(50 * time.Millisecond)
	if res := h.Poll(); res.Done {
		t.Fatal("op finished after only 50ms of a 100ms latency")
	}
	// Cross the latency threshold.
	sim.Advance(50 * time.Millisecond)
	res := h.Wait()
	if res.Err != nil {
		t.Fatalf("unexpected err: %v", res.Err)
	}
	if st := fi.Stats(); st.InjectedLatency != 100*time.Millisecond {
		t.Fatalf("injected latency = %v, want 100ms", st.InjectedLatency)
	}
}

// TestFaultInjector_Reproducible proves the same seed + schedule yields an
// identical failure sequence across independent runs.
func TestFaultInjector_Reproducible(t *testing.T) {
	t.Parallel()
	run := func() []bool {
		fi := simulation.NewFaultInjector(nil, simulation.Schedule{
			FailureRate: 0.5,
		}, simulation.WithSeed(42))
		fn := fi.Fn()
		out := make([]bool, 20)
		for i := range out {
			out[i] = fn(context.Background()) != nil
		}
		return out
	}
	a, b := run(), run()
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("non-reproducible at call %d: %v vs %v", i, a[i], b[i])
		}
	}
}

// TestFaultInjector_DeterministicRules verifies FailAt/SucceedAt/FailFirstN.
func TestFaultInjector_DeterministicRules(t *testing.T) {
	t.Parallel()
	fi := simulation.NewFaultInjector(nil, simulation.Schedule{
		FailFirstN: 3,
		SucceedAt:  []int{1}, // overrides FailFirstN for index 1
		FailAt:     []int{5},
		Err:        errors.New("custom"),
	})
	fn := fi.Fn()
	want := map[int]bool{0: true, 1: false, 2: true, 3: false, 4: false, 5: true, 6: false}
	for i := 0; i <= 6; i++ {
		got := fn(context.Background()) != nil
		if got != want[i] {
			t.Errorf("call %d failed=%v, want %v", i, got, want[i])
		}
	}
	if st := fi.Stats(); st.Total != 7 {
		t.Fatalf("total calls = %d, want 7", st.Total)
	}
}

// TestFaultInjector_ContextCancelDuringLatency ensures ctx cancellation during
// an injected latency returns ctx.Err() rather than the injected error.
func TestFaultInjector_ContextCancelDuringLatency(t *testing.T) {
	t.Parallel()
	sim := simulation.NewSim()
	fi := simulation.NewFaultInjector(sim.Clock(), simulation.Schedule{
		Latency:    time.Second,
		FailFirstN: 1,
	})
	ctx, cancel := context.WithCancel(context.Background())
	h := sim.RunOp(ctx, fi.Fn())
	cancel()
	res := h.RunFor(time.Millisecond, 10*time.Millisecond)
	if !errors.Is(res.Err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", res.Err)
	}
}

// TestSim_RunOpPanicsOnConcurrentOp documents the single-op-in-flight contract.
func TestSim_RunOpPanicsOnConcurrentOp(t *testing.T) {
	t.Parallel()
	sim := simulation.NewSim()
	fi := simulation.NewFaultInjector(sim.Clock(), simulation.Schedule{Latency: time.Hour})
	_ = sim.RunOp(context.Background(), fi.Fn())
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on second concurrent RunOp")
		}
	}()
	_ = sim.RunOp(context.Background(), fi.Fn())
}
