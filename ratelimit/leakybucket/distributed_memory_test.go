package leakybucket_test

import (
	"context"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/leakybucket"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/store"
)

// These tests exercise the DistributedLeakyBucket against an in-memory store with
// the default script emulations (mirroring the token-bucket / GCRA pattern) — no
// live Redis required — so admit/deny behaviour is deterministic and validated.

func TestDistributedLeakyBucket_ValidatesInput(t *testing.T) {
	s := store.NewMemoryWithScripts()
	defer s.Close()
	d := leakybucket.NewDistributed(5, 1, s, "lb") // capacity 5, leak 1/s
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
	// n exceeding capacity can never be admitted.
	if d.AllowN(ctx, "k", 6).Allowed {
		t.Fatal("AllowN(n > capacity) must be denied")
	}
	// A normal request still works after the invalid ones (no state corruption).
	if !d.Allow(ctx, "k").Allowed {
		t.Fatal("first valid request should be allowed")
	}
}

// TestDistributedLeakyBucket_AdmitThenDeny verifies the GCRA-dual leaky bucket
// admits up to `capacity` requests in a burst (when the leak over the burst is
// negligible) and then denies further ones — deterministically, with no Redis.
func TestDistributedLeakyBucket_AdmitThenDeny(t *testing.T) {
	s := store.NewMemoryWithScripts()
	defer s.Close()
	// capacity 3, very slow leak so no slots drain during the burst.
	d := leakybucket.NewDistributed(3, 0.001, s, "lb")
	ctx := context.Background()

	allowed := 0
	for i := 0; i < 6; i++ {
		if d.Allow(ctx, "burst").Allowed {
			allowed++
		}
	}
	if allowed != 3 {
		t.Fatalf("expected exactly capacity=3 admitted in a burst, got %d", allowed)
	}
	// The next request must be denied and report a positive RetryAfter.
	r := d.Allow(ctx, "burst")
	if r.Allowed {
		t.Fatal("request beyond capacity must be denied")
	}
	if r.RetryAfter <= 0 {
		t.Fatalf("denied request should report a positive RetryAfter, got %v", r.RetryAfter)
	}
}

// TestDistributedLeakyBucket_Reset verifies Reset clears the stored TAT so a key
// can admit again after being exhausted.
func TestDistributedLeakyBucket_Reset(t *testing.T) {
	s := store.NewMemoryWithScripts()
	defer s.Close()
	d := leakybucket.NewDistributed(2, 0.001, s, "lb")
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if !d.Allow(ctx, "k").Allowed {
			t.Fatalf("request %d within capacity should be allowed", i)
		}
	}
	if d.Allow(ctx, "k").Allowed {
		t.Fatal("third request should be denied")
	}
	if err := d.Reset(ctx, "k"); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if !d.Allow(ctx, "k").Allowed {
		t.Fatal("expected allowed after reset")
	}
}

// TestDistributedLeakyBucket_LeaksOverTime verifies that after enough real time a
// drained slot frees capacity again (uses a fast leak rate and a short sleep).
func TestDistributedLeakyBucket_LeaksOverTime(t *testing.T) {
	s := store.NewMemoryWithScripts()
	defer s.Close()
	// leak 100/s → one slot every 10ms; capacity 1.
	d := leakybucket.NewDistributed(1, 100, s, "k")
	ctx := context.Background()

	if !d.Allow(ctx, "x").Allowed {
		t.Fatal("first request should be allowed")
	}
	if d.Allow(ctx, "x").Allowed {
		t.Fatal("immediate second request should be denied (queue full)")
	}
	time.Sleep(30 * time.Millisecond) // let >=1 slot drain
	if !d.Allow(ctx, "x").Allowed {
		t.Fatal("request after a slot drained should be allowed")
	}
}

// TestDistributedLeakyBucket_Remaining verifies Remaining is derived from the
// reported queue depth and is bounded by the capacity. The GCRA-dual depth is an
// advisory estimate (floor of the TAT lead in slots), so the exact value can
// vary by one with real-time leak between calls; the invariant tested here is
// that it stays within [0, capacity) for an admitted request and never exceeds
// the limit.
func TestDistributedLeakyBucket_Remaining(t *testing.T) {
	s := store.NewMemoryWithScripts()
	defer s.Close()
	const capacity = 4
	d := leakybucket.NewDistributed(capacity, 0.001, s, "lb")
	ctx := context.Background()

	r1 := d.Allow(ctx, "k")
	if !r1.Allowed {
		t.Fatal("first request should be allowed")
	}
	if r1.Remaining < 0 || r1.Remaining >= capacity {
		t.Fatalf("Remaining out of range after 1 admit: got %d (capacity %d)", r1.Remaining, capacity)
	}
	if r1.Limit != capacity {
		t.Fatalf("expected Limit=%d, got %d", capacity, r1.Limit)
	}
	r2 := d.Allow(ctx, "k")
	if !r2.Allowed {
		t.Fatal("second request should be allowed")
	}
	if r2.Remaining < 0 || r2.Remaining >= capacity {
		t.Fatalf("Remaining out of range after 2 admits: got %d (capacity %d)", r2.Remaining, capacity)
	}
	// The backlog can only have grown (or held) between the two admits, so the
	// second call's Remaining must not exceed the first's.
	if r2.Remaining > r1.Remaining {
		t.Fatalf("Remaining increased across admits: r1=%d r2=%d", r1.Remaining, r2.Remaining)
	}
}
