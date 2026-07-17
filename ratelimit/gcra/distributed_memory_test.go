package gcra_test

import (
	"context"
	"testing"

	"github.com/sanskarpan/resilience/ratelimit/gcra"
	"github.com/sanskarpan/resilience/ratelimit/store"
)

// Exercises the Distributed GCRA against the in-memory script emulation
// (H-21/TQ-1) and verifies the H-7 input validation.

func TestDistributedGCRA_H7_ValidatesInput(t *testing.T) {
	s := store.NewMemoryWithScripts()
	defer s.Close()
	d := gcra.NewDistributed(10, 5, s, "gcra") // 10/s, burst 5
	ctx := context.Background()

	if d.AllowN(ctx, "k", 0).Allowed {
		t.Fatal("AllowN(n=0) must be denied, not a silent no-op allow")
	}
	if d.AllowN(ctx, "", 1).Allowed {
		t.Fatal("AllowN with empty key must be denied")
	}
	// n exceeding burst can never be admitted.
	if d.AllowN(ctx, "k", 6).Allowed {
		t.Fatal("AllowN(n > burst) must be denied")
	}
	// A normal request still works after the invalid ones (no state corruption).
	if !d.Allow(ctx, "k").Allowed {
		t.Fatal("first valid request should be allowed")
	}
}
