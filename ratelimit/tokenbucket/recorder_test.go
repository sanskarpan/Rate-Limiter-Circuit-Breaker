package tokenbucket_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
)

// spyRecorder is a metric.Recorder test double that counts allow/deny calls and
// records the algorithm label + whether ObserveDecision fired.
type spyRecorder struct {
	mu             sync.Mutex
	allowed        int
	denied         int
	observed       int
	lastAlgorithm  string
	lastObservedD  time.Duration
	observedNonNeg bool
}

func (s *spyRecorder) IncAllowed(algorithm string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.allowed++
	s.lastAlgorithm = algorithm
}

func (s *spyRecorder) IncDenied(algorithm string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.denied++
	s.lastAlgorithm = algorithm
}

func (s *spyRecorder) ObserveDecision(algorithm string, d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.observed++
	s.lastObservedD = d
	s.observedNonNeg = d >= 0
}

// Unused CB / bulkhead methods.
func (s *spyRecorder) RecordCBState(string, string)             {}
func (s *spyRecorder) IncCBResult(string, string)               {}
func (s *spyRecorder) ObserveCBExecution(string, time.Duration) {}
func (s *spyRecorder) IncCBTransition(string, string, string)   {}
func (s *spyRecorder) SetBulkheadInflight(string, int)          {}
func (s *spyRecorder) IncBulkheadRejected(string)               {}

func TestTokenBucketFiresRecorder(t *testing.T) {
	spy := &spyRecorder{}
	// capacity 1, refill 1/s: first Allow succeeds, second is denied.
	tb := tokenbucket.New(1, 1, tokenbucket.WithRecorder(spy))
	defer tb.Close()

	ctx := context.Background()
	if r := tb.Allow(ctx, "k"); !r.Allowed {
		t.Fatal("first Allow should be allowed")
	}
	if r := tb.Allow(ctx, "k"); r.Allowed {
		t.Fatal("second Allow should be denied")
	}

	spy.mu.Lock()
	defer spy.mu.Unlock()
	if spy.allowed != 1 {
		t.Errorf("IncAllowed calls = %d, want 1", spy.allowed)
	}
	if spy.denied != 1 {
		t.Errorf("IncDenied calls = %d, want 1", spy.denied)
	}
	if spy.observed != 2 {
		t.Errorf("ObserveDecision calls = %d, want 2", spy.observed)
	}
	if spy.lastAlgorithm != "token_bucket" {
		t.Errorf("algorithm label = %q, want token_bucket", spy.lastAlgorithm)
	}
	if !spy.observedNonNeg {
		t.Errorf("ObserveDecision duration was negative: %v", spy.lastObservedD)
	}
}

// TestTokenBucketDefaultRecorderNoPanic ensures the default (Nop) path works
// without a recorder wired.
func TestTokenBucketDefaultRecorderNoPanic(t *testing.T) {
	tb := tokenbucket.New(5, 5)
	defer tb.Close()
	if r := tb.Allow(context.Background(), "k"); !r.Allowed {
		t.Fatal("expected allowed with default Nop recorder")
	}
}
