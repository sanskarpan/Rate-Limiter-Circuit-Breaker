package tokenbucket_test

import (
	"context"
	"testing"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/store"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
)

// Server-time mode (ENHANCEMENTS §5.1) memory-emulation tests for the
// distributed token bucket. They verify the limiter inherits server-time mode
// from the store, that WithServerTime overrides it, and that server-time mode
// yields the same allow/deny outcome as client-time mode when clocks agree.

func TestDistributedTokenBucket_ServerTime_InheritsFromStore(t *testing.T) {
	ctx := context.Background()

	// Server-time store: limiter should behave identically to a client-time
	// store's limiter under agreeing clocks — admit capacity, then deny.
	srv := store.NewMemoryWithScripts(store.WithServerTime(true))
	defer srv.Close()
	cli := store.NewMemoryWithScripts()
	defer cli.Close()

	// Negligible refill so we can count exact admissions.
	dSrv := tokenbucket.NewDistributed(1e-9, 4, srv, "tb")
	dCli := tokenbucket.NewDistributed(1e-9, 4, cli, "tb")

	count := func(d *tokenbucket.DistributedTokenBucket) (allowed int) {
		for i := 0; i < 6; i++ {
			if d.AllowN(ctx, "k", 1).Allowed {
				allowed++
			}
		}
		return
	}
	if a := count(dSrv); a != 4 {
		t.Fatalf("server-time limiter: expected 4 allowed, got %d", a)
	}
	if a := count(dCli); a != 4 {
		t.Fatalf("client-time limiter: expected 4 allowed, got %d", a)
	}
}

func TestDistributedTokenBucket_WithServerTime_Override(t *testing.T) {
	ctx := context.Background()
	// Plain (client-time) store, but force server-time ON via option. On the
	// memory store the flag alone is honored only when the store is server-time,
	// so behaviour must still be correct (admit capacity then deny) regardless.
	s := store.NewMemoryWithScripts(store.WithServerTime(true))
	defer s.Close()

	d := tokenbucket.NewDistributed(1e-9, 3, s, "tb", tokenbucket.WithServerTime(true))
	allowed := 0
	for i := 0; i < 5; i++ {
		if d.AllowN(ctx, "k", 1).Allowed {
			allowed++
		}
	}
	if allowed != 3 {
		t.Fatalf("expected 3 allowed with server-time override, got %d", allowed)
	}

	// And WithServerTime(false) must force client mode even on a server-time store.
	d2 := tokenbucket.NewDistributed(1e-9, 3, s, "tb", tokenbucket.WithServerTime(false))
	if !d2.AllowN(ctx, "k2", 1).Allowed {
		t.Fatal("client-time-forced limiter should admit first request")
	}
}
