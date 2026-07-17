package pipeline

import (
	"context"
	"errors"
	"testing"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/concurrency"
)

// TestBuild_ConcurrencyStageOrderAndShedding verifies the concurrency stage sorts
// between bulkhead and timeout, and sheds with ErrConcurrencyLimited at the limit.
func TestBuild_ConcurrencyStageOrder(t *testing.T) {
	lim := concurrency.NewGradient2(concurrency.Config{InitialLimit: 5, MinLimit: 1, MaxLimit: 10})
	p := New().Timeout(0).Bulkhead(10, 0).Concurrency(lim).Build()
	want := []stageKind{kindBulkhead, kindConcurrency}
	if len(p.kinds) != 2 || p.kinds[0] != want[0] || p.kinds[1] != want[1] {
		t.Fatalf("canonical order: got %v want %v", p.kinds, want)
	}
}

func TestConcurrencyStage_ShedsAtLimit(t *testing.T) {
	lim := concurrency.NewGradient2(concurrency.Config{InitialLimit: 1, MinLimit: 1, MaxLimit: 1})
	p := New().Concurrency(lim).Build()
	// Occupy the only slot inside a long-running fn, then a concurrent Execute sheds.
	block := make(chan struct{})
	started := make(chan struct{})
	go func() {
		_ = p.Execute(context.Background(), func(ctx context.Context) error {
			close(started)
			<-block
			return nil
		})
	}()
	<-started
	err := p.Execute(context.Background(), func(ctx context.Context) error { return nil })
	if !errors.Is(err, ErrConcurrencyLimited) {
		t.Fatalf("expected ErrConcurrencyLimited when at limit, got %v", err)
	}
	close(block)
}
