package store_test

import (
	"context"
	"testing"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/store"
)

// Non-integration tests for the LeakyBucketScript in-memory emulation
// (ENHANCEMENTS §1.8). These validate deterministic admit/deny behaviour with a
// fixed client clock and the server-time flag plumbing, mirroring the GCRA/token
// bucket emulation tests.

// TestMemory_LeakyBucket_AdmitThenDeny drives the leaky bucket script with a
// fixed client `now` and a slow leak, asserting exactly `capacity` admits then a
// deny with a positive retry_after and a bounded queue depth.
func TestMemory_LeakyBucket_AdmitThenDeny(t *testing.T) {
	ctx := context.Background()
	m := store.NewMemoryWithScripts()
	defer m.Close()

	const emission = int64(1_000_000_000_000) // 1000s per slot: negligible leak over the burst
	const capacity = 3
	const ttlMs = 60000
	nowNs := int64(5_000_000_000_000_000) // fixed client clock

	allowed := 0
	var lastDepth int64
	for i := 0; i < 6; i++ {
		res, err := m.Eval(ctx, store.LeakyBucketScriptID,
			[]string{"lb:k"},
			emission, capacity, int64(1), nowNs, ttlMs, 0,
		)
		if err != nil {
			t.Fatalf("eval: %v", err)
		}
		arr := res.([]any)
		if arr[0].(int64) == 1 {
			allowed++
			lastDepth = arr[1].(int64)
			if lastDepth < 0 || lastDepth > capacity {
				t.Fatalf("queue_depth out of range on admit %d: %d", i, lastDepth)
			}
		} else {
			// Denied: retry_after must be positive.
			if arr[2].(int64) <= 0 {
				t.Fatalf("denied request should have positive retry_after, got %d", arr[2].(int64))
			}
		}
	}
	if allowed != capacity {
		t.Fatalf("expected exactly %d admits, got %d", capacity, allowed)
	}
}

// TestMemory_LeakyBucket_ServerTimeParity runs the leaky bucket emulation with
// server-time mode ON and a client `now` that AGREES with the store clock, and
// asserts the same admit count (== capacity) as client-time mode, matching the
// discipline of the token-bucket / GCRA server-time parity tests.
func TestMemory_LeakyBucket_ServerTimeParity(t *testing.T) {
	ctx := context.Background()
	const emission = int64(1_000_000_000_000) // 1000s per slot
	const capacity = 3
	const ttlMs = 60000

	run := func(m *store.Memory, useServerTime int) (allowed int) {
		nowNs := int64(6_000_000_000_000_000)
		for i := 0; i < 6; i++ {
			res, err := m.Eval(ctx, store.LeakyBucketScriptID,
				[]string{"lb:parity"},
				emission, capacity, int64(1), nowNs, ttlMs, useServerTime,
			)
			if err != nil {
				t.Fatalf("eval: %v", err)
			}
			if res.([]any)[0].(int64) == 1 {
				allowed++
			}
		}
		return
	}

	clientAllowed := run(store.NewMemoryWithScripts(), 0)
	serverAllowed := run(store.NewMemoryWithScripts(store.WithServerTime(true)), 1)

	if clientAllowed != capacity {
		t.Fatalf("client-time: expected %d allowed, got %d", capacity, clientAllowed)
	}
	if serverAllowed != capacity {
		t.Fatalf("server-time: expected %d allowed, got %d", capacity, serverAllowed)
	}
}

// TestMemory_LeakyBucket_DenyDoesNotPersist verifies a denied request does not
// advance the stored TAT (only SET on allow), so a later request can still be
// admitted once a slot conceptually frees — matching the Lua script.
func TestMemory_LeakyBucket_DenyDoesNotPersist(t *testing.T) {
	ctx := context.Background()
	m := store.NewMemoryWithScripts()
	defer m.Close()

	const emission = int64(1_000_000_000) // 1s per slot
	const capacity = 1
	const ttlMs = 60000
	base := int64(7_000_000_000_000_000)

	// Fill the single slot at t=base.
	res, _ := m.Eval(ctx, store.LeakyBucketScriptID, []string{"lb:np"}, emission, capacity, int64(1), base, ttlMs, 0)
	if res.([]any)[0].(int64) != 1 {
		t.Fatal("first request should be admitted")
	}
	// Immediately deny (queue full).
	res, _ = m.Eval(ctx, store.LeakyBucketScriptID, []string{"lb:np"}, emission, capacity, int64(1), base, ttlMs, 0)
	if res.([]any)[0].(int64) != 0 {
		t.Fatal("second immediate request should be denied")
	}
	// One emission interval later, exactly one slot has drained → admit again.
	res, _ = m.Eval(ctx, store.LeakyBucketScriptID, []string{"lb:np"}, emission, capacity, int64(1), base+emission, ttlMs, 0)
	if res.([]any)[0].(int64) != 1 {
		t.Fatal("request one interval later should be admitted (a slot drained)")
	}
}
