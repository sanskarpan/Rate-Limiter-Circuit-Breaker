package bulkhead_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/bulkhead"
)

type bhSpy struct {
	mu            sync.Mutex
	inflightSets  []int
	maxInflight   int
	rejectedCount int
	lastName      string
}

func (s *bhSpy) IncAllowed(string)                        {}
func (s *bhSpy) IncDenied(string)                         {}
func (s *bhSpy) ObserveDecision(string, time.Duration)    {}
func (s *bhSpy) RecordCBState(string, string)             {}
func (s *bhSpy) IncCBResult(string, string)               {}
func (s *bhSpy) ObserveCBExecution(string, time.Duration) {}
func (s *bhSpy) IncCBTransition(string, string, string)   {}

func (s *bhSpy) SetBulkheadInflight(name string, n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastName = name
	s.inflightSets = append(s.inflightSets, n)
	if n > s.maxInflight {
		s.maxInflight = n
	}
}

func (s *bhSpy) IncBulkheadRejected(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastName = name
	s.rejectedCount++
}

func TestBulkheadFiresRecorder(t *testing.T) {
	spy := &bhSpy{}
	// maxConcurrency 1, non-blocking (maxWait=0).
	bh := bulkhead.New(1, 0, bulkhead.WithName("pool"), bulkhead.WithRecorder(spy))

	ctx := context.Background()

	// Hold the single slot while a second Execute is rejected.
	release := make(chan struct{})
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- bh.Execute(ctx, func(context.Context) error {
			close(started)
			<-release
			return nil
		})
	}()
	<-started

	// Second call must be rejected (no slot, non-blocking).
	if err := bh.Execute(ctx, func(context.Context) error { return nil }); err != bulkhead.ErrBulkheadFull {
		t.Fatalf("expected ErrBulkheadFull, got %v", err)
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("first Execute failed: %v", err)
	}

	spy.mu.Lock()
	defer spy.mu.Unlock()
	if spy.rejectedCount != 1 {
		t.Errorf("IncBulkheadRejected calls = %d, want 1", spy.rejectedCount)
	}
	if spy.maxInflight != 1 {
		t.Errorf("max observed inflight = %d, want 1", spy.maxInflight)
	}
	// After the first Execute returns, inflight is set back to 0.
	if last := spy.inflightSets[len(spy.inflightSets)-1]; last != 0 {
		t.Errorf("final inflight set = %d, want 0", last)
	}
	if spy.lastName != "pool" {
		t.Errorf("name label = %q, want pool", spy.lastName)
	}
}

func TestBulkheadDefaultRecorderNoPanic(t *testing.T) {
	bh := bulkhead.New(2, 0)
	if err := bh.Execute(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatalf("unexpected error with default recorder: %v", err)
	}
}
