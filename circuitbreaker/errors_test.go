package circuitbreaker_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	cb "github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
)

// TestCircuitError_IsSentinels verifies that errors.Is still matches the
// original sentinels through the enriched CircuitError, for both the open and
// too-many-requests cases and for wrapped copies.
func TestCircuitError_IsSentinels(t *testing.T) {
	tests := []struct {
		name           string
		err            error
		wantOpen       bool
		wantTooMany    bool
		wantNotSentinl error
	}{
		{
			name:           "open error matches ErrCircuitOpen only",
			err:            cb.NewCircuitError("svc", cb.StateOpen, 0, cb.ErrCircuitOpen),
			wantOpen:       true,
			wantTooMany:    false,
			wantNotSentinl: cb.ErrTooManyRequests,
		},
		{
			name:           "too-many error matches ErrTooManyRequests only",
			err:            cb.NewCircuitError("svc", cb.StateHalfOpen, 0, cb.ErrTooManyRequests),
			wantOpen:       false,
			wantTooMany:    true,
			wantNotSentinl: cb.ErrCircuitOpen,
		},
		{
			name:        "wrapped open error still matches",
			err:         fmt.Errorf("boom: %w", cb.NewCircuitError("svc", cb.StateOpen, 0, cb.ErrCircuitOpen)),
			wantOpen:    true,
			wantTooMany: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := errors.Is(tt.err, cb.ErrCircuitOpen); got != tt.wantOpen {
				t.Errorf("errors.Is(ErrCircuitOpen) = %v, want %v", got, tt.wantOpen)
			}
			if got := errors.Is(tt.err, cb.ErrTooManyRequests); got != tt.wantTooMany {
				t.Errorf("errors.Is(ErrTooManyRequests) = %v, want %v", got, tt.wantTooMany)
			}
			// Package predicates must agree with errors.Is.
			if got := cb.IsOpen(tt.err); got != tt.wantOpen {
				t.Errorf("IsOpen = %v, want %v", got, tt.wantOpen)
			}
			if got := cb.IsTooManyRequests(tt.err); got != tt.wantTooMany {
				t.Errorf("IsTooManyRequests = %v, want %v", got, tt.wantTooMany)
			}
		})
	}
}

// TestCircuitError_Accessors verifies the accessor methods return the stored
// fields.
func TestCircuitError_Accessors(t *testing.T) {
	e := cb.NewCircuitError("payments", cb.StateOpen, 250*time.Millisecond, cb.ErrCircuitOpen)
	if e.Name != "payments" {
		t.Errorf("Name = %q, want payments", e.Name)
	}
	if e.CircuitState() != cb.StateOpen {
		t.Errorf("CircuitState = %v, want StateOpen", e.CircuitState())
	}
	if e.RetryAfter() != 250*time.Millisecond {
		t.Errorf("RetryAfter = %v, want 250ms", e.RetryAfter())
	}
	// Negative TimeUntilHalfOpen clamps to 0.
	e.TimeUntilHalfOpen = -5 * time.Second
	if e.RetryAfter() != 0 {
		t.Errorf("RetryAfter with negative value = %v, want 0", e.RetryAfter())
	}
}

// TestAsCircuitError verifies extraction via errors.As, including through a
// wrapping layer, and the no-match case.
func TestAsCircuitError(t *testing.T) {
	orig := cb.NewCircuitError("svc", cb.StateOpen, 0, cb.ErrCircuitOpen)

	if got, ok := cb.AsCircuitError(orig); !ok || got != orig {
		t.Fatalf("AsCircuitError(direct) = %v, %v; want %v, true", got, ok, orig)
	}

	wrapped := fmt.Errorf("layer: %w", orig)
	got, ok := cb.AsCircuitError(wrapped)
	if !ok || got != orig {
		t.Fatalf("AsCircuitError(wrapped) = %v, %v; want %v, true", got, ok, orig)
	}

	if got, ok := cb.AsCircuitError(errors.New("unrelated")); ok || got != nil {
		t.Fatalf("AsCircuitError(unrelated) = %v, %v; want nil, false", got, ok)
	}
	if got, ok := cb.AsCircuitError(nil); ok || got != nil {
		t.Fatalf("AsCircuitError(nil) = %v, %v; want nil, false", got, ok)
	}
}

// TestCircuitError_EndToEnd_OpenCarriesContext drives a real breaker to Open and
// asserts the rejection error carries name, state, and a non-zero RetryAfter
// that shrinks as the clock advances toward the open timeout.
func TestCircuitError_EndToEnd_OpenCarriesContext(t *testing.T) {
	clk := clock.NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	b := cb.New(cb.Config{
		Name:             "svc-e2e",
		WindowType:       cb.CountBased,
		WindowSize:       5,
		FailureThreshold: 3,
		OpenTimeout:      10 * time.Second,
		Clock:            clk,
	})
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		_ = b.Execute(ctx, fail)
	}
	if b.State() != cb.StateOpen {
		t.Fatalf("expected Open, got %s", b.State())
	}

	// Reject immediately after opening: RetryAfter ~ full OpenTimeout.
	err := b.Execute(ctx, succeed)
	ce, ok := cb.AsCircuitError(err)
	if !ok {
		t.Fatalf("expected CircuitError, got %v", err)
	}
	if ce.Name != "svc-e2e" {
		t.Errorf("Name = %q, want svc-e2e", ce.Name)
	}
	if ce.CircuitState() != cb.StateOpen {
		t.Errorf("State = %v, want StateOpen", ce.CircuitState())
	}
	if ce.RetryAfter() != 10*time.Second {
		t.Errorf("RetryAfter = %v, want 10s", ce.RetryAfter())
	}
	if !cb.IsOpen(err) {
		t.Error("IsOpen should be true")
	}

	// Advance partway: RetryAfter shrinks.
	clk.Advance(4 * time.Second)
	err = b.Execute(ctx, succeed)
	ce, _ = cb.AsCircuitError(err)
	if ce.RetryAfter() != 6*time.Second {
		t.Errorf("RetryAfter after 4s = %v, want 6s", ce.RetryAfter())
	}
}
