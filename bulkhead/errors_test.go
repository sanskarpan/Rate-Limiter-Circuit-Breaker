package bulkhead_test

import (
	"context"
	"errors"
	"testing"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/bulkhead"
)

// TestBulkheadError_WrapsSentinel verifies the structured error returned on
// rejection still satisfies errors.Is(err, ErrBulkheadFull) and can be
// extracted with the taxonomy helpers, carrying an accurate saturation snapshot.
func TestBulkheadError_WrapsSentinel(t *testing.T) {
	// Capacity 1, non-blocking. Hold the only slot, then a second call rejects.
	b := bulkhead.New(1, 0, bulkhead.WithName("db"))

	release := make(chan struct{})
	entered := make(chan struct{})
	go func() {
		_ = b.Execute(context.Background(), func(context.Context) error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered // ensure the slot is held

	err := b.Execute(context.Background(), func(context.Context) error { return nil })
	if err == nil {
		t.Fatal("expected rejection, got nil")
	}
	if !errors.Is(err, bulkhead.ErrBulkheadFull) {
		t.Fatalf("errors.Is(err, ErrBulkheadFull) = false; err = %v", err)
	}
	if !bulkhead.IsBulkheadFull(err) {
		t.Fatal("IsBulkheadFull(err) = false")
	}
	be, ok := bulkhead.AsBulkheadError(err)
	if !ok {
		t.Fatalf("AsBulkheadError failed; err type = %T", err)
	}
	if be.Name != "db" {
		t.Errorf("Name = %q, want db", be.Name)
	}
	if be.Capacity != 1 {
		t.Errorf("Capacity = %d, want 1", be.Capacity)
	}
	if be.Inflight != 1 {
		t.Errorf("Inflight = %d, want 1", be.Inflight)
	}
	if be.Error() != bulkhead.ErrBulkheadFull.Error() {
		t.Errorf("Error() = %q, want sentinel message", be.Error())
	}

	close(release)
}

// TestAsBulkheadError_NonBulkheadError confirms the extractor rejects unrelated
// errors.
func TestAsBulkheadError_NonBulkheadError(t *testing.T) {
	if _, ok := bulkhead.AsBulkheadError(errors.New("other")); ok {
		t.Fatal("AsBulkheadError matched an unrelated error")
	}
	if bulkhead.IsBulkheadFull(context.Canceled) {
		t.Fatal("IsBulkheadFull matched context.Canceled")
	}
}
