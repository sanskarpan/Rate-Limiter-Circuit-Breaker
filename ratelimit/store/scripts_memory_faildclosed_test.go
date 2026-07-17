package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/fixedwindow"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/store"
)

// TestScriptEmulation_MaxKeys_FailsClosed is the regression test for F-4: when
// the script-backed memory store is at maxKeys, a brand-new key must be DENIED
// (fail closed), not silently admitted forever against a throwaway entry.
func TestScriptEmulation_MaxKeys_FailsClosed(t *testing.T) {
	s := store.NewMemoryWithScripts(store.WithMaxKeys(1))
	defer s.Close()
	d := fixedwindow.NewDistributed(5, time.Minute, s, "fw")
	ctx := context.Background()

	// Occupy the single key slot.
	if !d.Allow(ctx, "keyA").Allowed {
		t.Fatal("first key should be admitted")
	}
	// A second, brand-new key is over capacity: it must be denied, and must NOT
	// become an always-allow (previously it admitted every request).
	for i := 0; i < 3; i++ {
		if d.Allow(ctx, "keyB").Allowed {
			t.Fatalf("over-capacity new key must be denied (fail closed), got allowed on attempt %d", i)
		}
	}
}
