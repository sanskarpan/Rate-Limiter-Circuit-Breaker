package circuitbreaker

import (
	"testing"
	"time"

	"github.com/sanskarpan/resilience/internal/clock"
)

// TestM8_RetripWhileOpen_RefreshesOpenedAt verifies the chosen M-8 semantics:
// calling transitionToOpen while already Open refreshes openedAt (restarting the
// OpenTimeout), rather than being a no-op or hitting the old dead code branch.
func TestM8_RetripWhileOpen_RefreshesOpenedAt(t *testing.T) {
	clk := clock.NewManualClock(time.Unix(0, 0))
	cb := New(Config{
		Name:             "m8",
		WindowType:       CountBased,
		WindowSize:       5,
		FailureThreshold: 1,
		OpenTimeout:      time.Second,
		Clock:            clk,
	})

	cb.transitionToOpen()
	first := cb.openedAt.Load()
	if cb.State() != StateOpen {
		t.Fatalf("expected Open, got %s", cb.State())
	}

	// Advance time and re-trip while already Open.
	clk.Advance(500 * time.Millisecond)
	cb.transitionToOpen()
	second := cb.openedAt.Load()

	if second <= first {
		t.Fatalf("re-trip while Open should refresh openedAt: first=%d second=%d", first, second)
	}
	if want := clk.Now().UnixNano(); second != want {
		t.Fatalf("openedAt should equal now after re-trip: got %d want %d", second, want)
	}
	if cb.State() != StateOpen {
		t.Fatalf("state must remain Open after re-trip, got %s", cb.State())
	}
}
