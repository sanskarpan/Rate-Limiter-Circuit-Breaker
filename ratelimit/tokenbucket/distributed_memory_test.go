package tokenbucket_test

import (
	"context"
	"testing"

	"github.com/sanskarpan/resilience/ratelimit/store"
	"github.com/sanskarpan/resilience/ratelimit/tokenbucket"
)

// These tests exercise the Distributed token bucket against an in-memory store
// with the default script emulations (H-21/TQ-1) — no live Redis required —
// and verify the H-7 input-validation fix.

func TestDistributedTokenBucket_H7_ValidatesInput(t *testing.T) {
	s := store.NewMemoryWithScripts()
	defer s.Close()
	d := tokenbucket.NewDistributed(1, 5, s, "tb") // refillRate 1/s, capacity 5
	ctx := context.Background()

	if d.AllowN(ctx, "k", 0).Allowed {
		t.Fatal("AllowN(n=0) must be denied, not a silent no-op allow")
	}
	if d.AllowN(ctx, "", 1).Allowed {
		t.Fatal("AllowN with empty key must be denied")
	}
	if d.AllowN(ctx, "bad\r\nkey", 1).Allowed {
		t.Fatal("AllowN with injection key must be denied")
	}

	// The rejected n=0 / invalid calls must not have consumed capacity: 5 real
	// requests still succeed, the 6th is denied.
	for i := 0; i < 5; i++ {
		if !d.AllowN(ctx, "k", 1).Allowed {
			t.Fatalf("request %d within capacity should be allowed", i)
		}
	}
	if d.AllowN(ctx, "k", 1).Allowed {
		t.Fatal("request beyond capacity should be denied")
	}
}
