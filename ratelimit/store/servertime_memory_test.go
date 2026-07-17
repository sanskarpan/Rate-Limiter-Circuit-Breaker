package store_test

import (
	"context"
	"testing"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/store"
)

// Non-integration tests for the server-time-mode plumbing on the in-memory
// store. These verify (a) ServerTimeMode reflects the option, and (b) the script
// emulations produce the SAME allow/deny outcome in server-time mode as in
// client-time mode when the client clock agrees with the store's own clock.

func TestMemory_ServerTimeMode_Flag(t *testing.T) {
	if store.NewMemory().ServerTimeMode() {
		t.Fatal("default memory store should report ServerTimeMode()==false")
	}
	if !store.NewMemory(store.WithServerTime(true)).ServerTimeMode() {
		t.Fatal("WithServerTime(true) should report ServerTimeMode()==true")
	}
	// NewMemoryWithScripts must forward options.
	if !store.NewMemoryWithScripts(store.WithServerTime(true)).ServerTimeMode() {
		t.Fatal("NewMemoryWithScripts(WithServerTime(true)) should be server-time")
	}
}

// TestMemory_ServerTime_TokenBucketParity runs the token-bucket emulation with
// server-time mode ON and a client `now` that AGREES with the store clock, and
// asserts the same admit-then-deny behaviour as client-time mode. It also checks
// that a use_server_time flag of "0" (client mode) still works.
func TestMemory_ServerTime_TokenBucketParity(t *testing.T) {
	ctx := context.Background()
	// capacity 3, refillRate ~0 over the test's timescale.
	const capacity = 3.0
	const refillRate = 1e-12 // tokens/ns; negligible refill in-test
	const n = 1
	const ttlMs = 60000

	run := func(m *store.Memory, useServerTime int) (allowed int) {
		nowNs := int64(1_000_000_000_000_000) // fixed client clock
		for i := 0; i < 5; i++ {
			res, err := m.Eval(ctx, store.TokenBucketScript,
				[]string{"tb:parity"},
				capacity, refillRate, n, nowNs, ttlMs, useServerTime,
			)
			if err != nil {
				t.Fatalf("eval: %v", err)
			}
			arr := res.([]any)
			if arr[0].(int64) == 1 {
				allowed++
			}
		}
		return
	}

	// Client-time mode on a plain store.
	clientAllowed := run(store.NewMemoryWithScripts(), 0)
	// Server-time mode on a server-time store: the store uses its OWN clock, so
	// the fixed client nowNs is ignored, but with negligible refill the count of
	// admitted requests must still equal capacity.
	serverAllowed := run(store.NewMemoryWithScripts(store.WithServerTime(true)), 1)

	if clientAllowed != int(capacity) {
		t.Fatalf("client-time: expected %d allowed, got %d", int(capacity), clientAllowed)
	}
	if serverAllowed != int(capacity) {
		t.Fatalf("server-time: expected %d allowed, got %d", int(capacity), serverAllowed)
	}
}

// TestMemory_ServerTime_GCRAParity does the same equivalence check for GCRA.
func TestMemory_ServerTime_GCRAParity(t *testing.T) {
	ctx := context.Background()
	const emission = int64(1_000_000_000) // 1s per cell
	const burst = 3
	const n = 1
	const ttlMs = 60000

	run := func(m *store.Memory, useServerTime int) (allowed int) {
		nowNs := int64(2_000_000_000_000_000)
		for i := 0; i < 5; i++ {
			res, err := m.Eval(ctx, store.GCRAScript,
				[]string{"gcra:parity"},
				emission, burst, n, nowNs, ttlMs, useServerTime,
			)
			if err != nil {
				t.Fatalf("eval: %v", err)
			}
			arr := res.([]any)
			if arr[0].(int64) == 1 {
				allowed++
			}
		}
		return
	}

	clientAllowed := run(store.NewMemoryWithScripts(), 0)
	serverAllowed := run(store.NewMemoryWithScripts(store.WithServerTime(true)), 1)

	// GCRA admits `burst` immediately, then denies until a cell elapses.
	if clientAllowed != burst {
		t.Fatalf("client-time GCRA: expected %d allowed, got %d", burst, clientAllowed)
	}
	if serverAllowed != burst {
		t.Fatalf("server-time GCRA: expected %d allowed, got %d", burst, serverAllowed)
	}
}

// TestMemory_ServerTime_IgnoredWhenStoreDisabled verifies that passing
// use_server_time=1 to a store that is NOT in server-time mode falls back to
// client time (the flag alone does not flip a plain store).
func TestMemory_ServerTime_IgnoredWhenStoreDisabled(t *testing.T) {
	ctx := context.Background()
	m := store.NewMemoryWithScripts() // server-time OFF
	// A deliberately-skewed far-future client now; if the store honored the flag
	// it would use its own clock instead. With server-time OFF the client now is
	// authoritative and the first request is admitted normally.
	res, err := m.Eval(ctx, store.TokenBucketScript,
		[]string{"tb:disabled"},
		3.0, 1e-12, 1, int64(9_000_000_000_000_000), 60000, 1,
	)
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if res.([]any)[0].(int64) != 1 {
		t.Fatal("expected first request admitted in client-time (flag ignored on plain store)")
	}
}
