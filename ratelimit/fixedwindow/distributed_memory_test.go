package fixedwindow_test

import (
	"context"
	"testing"
	"time"

	"github.com/sanskarpan/resilience/ratelimit/fixedwindow"
	"github.com/sanskarpan/resilience/ratelimit/store"
)

// Distributed fixed-window tests run against the in-memory script emulation
// (H-21/TQ-1) and verify H-4: a rejected over-limit AllowN must NOT poison the
// window (previously IncrBy(n) with no rollback permanently wedged it).

func TestDistributedFixedWindow_H4_RejectedBatchDoesNotPoison(t *testing.T) {
	s := store.NewMemoryWithScripts()
	defer s.Close()
	d := fixedwindow.NewDistributed(5, time.Minute, s, "fw")
	ctx := context.Background()

	if d.AllowN(ctx, "k", 10).Allowed {
		t.Fatal("AllowN(10) over limit 5 must be denied")
	}
	// The window must not be poisoned by the rejected batch: 5 single requests
	// still fit.
	for i := 0; i < 5; i++ {
		if !d.Allow(ctx, "k").Allowed {
			t.Fatalf("request %d must succeed after a rejected over-limit batch (no poison)", i)
		}
	}
	if d.Allow(ctx, "k").Allowed {
		t.Fatal("6th request should be denied")
	}
}

func TestDistributedFixedWindow_H7_ValidatesInput(t *testing.T) {
	s := store.NewMemoryWithScripts()
	defer s.Close()
	d := fixedwindow.NewDistributed(5, time.Minute, s, "fw2")
	ctx := context.Background()
	if d.AllowN(ctx, "k", 0).Allowed {
		t.Fatal("AllowN(n=0) must be denied")
	}
	if d.AllowN(ctx, "", 1).Allowed {
		t.Fatal("empty key must be denied")
	}
}
