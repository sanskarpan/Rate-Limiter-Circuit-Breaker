package circuitbreaker_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
)

type cbSpy struct {
	mu          sync.Mutex
	states      []string       // sequence of RecordCBState values
	results     map[string]int // result -> count
	transitions map[string]int // "from->to" -> count
	execObs     int
}

func newCBSpy() *cbSpy {
	return &cbSpy{results: map[string]int{}, transitions: map[string]int{}}
}

func (s *cbSpy) IncAllowed(string)                     {}
func (s *cbSpy) IncDenied(string)                      {}
func (s *cbSpy) ObserveDecision(string, time.Duration) {}

func (s *cbSpy) RecordCBState(_, state string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.states = append(s.states, state)
}

func (s *cbSpy) IncCBResult(_, result string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.results[result]++
}

func (s *cbSpy) ObserveCBExecution(string, time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.execObs++
}

func (s *cbSpy) IncCBTransition(_, from, to string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.transitions[from+"->"+to]++
}

func (s *cbSpy) SetBulkheadInflight(string, int) {}
func (s *cbSpy) IncBulkheadRejected(string)      {}

func TestCircuitBreakerFiresRecorder(t *testing.T) {
	spy := newCBSpy()
	clk := clock.NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	cb := circuitbreaker.New(circuitbreaker.Config{
		Name:             "primary",
		WindowSize:       10,
		FailureThreshold: 3,
		OpenTimeout:      30 * time.Second,
		Clock:            clk,
		Recorder:         spy,
	})

	ctx := context.Background()
	okFn := func(context.Context) error { return nil }
	failErr := errors.New("boom")
	failFn := func(context.Context) error { return failErr }

	// One success.
	if err := cb.Execute(ctx, okFn); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Three failures → opens (FailureThreshold=3).
	for i := 0; i < 3; i++ {
		if err := cb.Execute(ctx, failFn); err == nil {
			t.Fatal("expected failure error")
		}
	}
	if cb.State() != circuitbreaker.StateOpen {
		t.Fatalf("expected open state, got %s", cb.State())
	}
	// Circuit open → next Execute is rejected.
	if err := cb.Execute(ctx, okFn); err == nil {
		t.Fatal("expected rejection while open")
	}

	spy.mu.Lock()
	defer spy.mu.Unlock()

	if spy.results["success"] != 1 {
		t.Errorf("success results = %d, want 1", spy.results["success"])
	}
	if spy.results["failure"] != 3 {
		t.Errorf("failure results = %d, want 3", spy.results["failure"])
	}
	if spy.results["rejected"] != 1 {
		t.Errorf("rejected results = %d, want 1", spy.results["rejected"])
	}
	// 4 executed calls (1 ok + 3 fail) observed latency; the rejected call did
	// not run so it is not observed.
	if spy.execObs != 4 {
		t.Errorf("ObserveCBExecution calls = %d, want 4", spy.execObs)
	}
	if spy.transitions["closed->open"] != 1 {
		t.Errorf("closed->open transitions = %d, want 1", spy.transitions["closed->open"])
	}
	// New() seeds "closed"; opening records "open".
	if len(spy.states) < 2 {
		t.Fatalf("expected at least 2 state records, got %v", spy.states)
	}
	if spy.states[0] != "closed" {
		t.Errorf("first state = %q, want closed (seeded at New)", spy.states[0])
	}
	last := spy.states[len(spy.states)-1]
	if last != "open" {
		t.Errorf("last state = %q, want open", last)
	}
}
