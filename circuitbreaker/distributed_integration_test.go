//go:build integration

// Integration tests for the distributed circuit breaker (ENHANCEMENTS §1.4).
// These exercise the REAL Redis Lua scripts (not the in-memory emulation), so
// they validate the actual shared-state path used in production.
//
// Run with: go test ./circuitbreaker/ -tags=integration
// Requires a Redis instance at localhost:6379 or REDIS_ADDR env var.
package circuitbreaker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/store"
)

func newIntegrationStore(t *testing.T) *store.Redis {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	// Unique key prefix per run so parallel/repeated runs never collide on the
	// shared cb:<name> key.
	r := store.NewRedis(store.RedisOptions{
		Addr:      addr,
		KeyPrefix: fmt.Sprintf("cbtest:%d:", time.Now().UnixNano()),
	})
	if err := r.Ping(context.Background()); err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

func intFail(context.Context) error { return errors.New("boom") }
func intOK(context.Context) error   { return nil }

// TestIntegration_SharedStateAcrossInstances is the headline test: two
// INDEPENDENT DistributedCircuitBreaker instances share one Redis-backed state.
// Instance A's failures trip the breaker; instance B — which never issued a
// failing call — immediately observes Open and rejects.
func TestIntegration_SharedStateAcrossInstances(t *testing.T) {
	s := newIntegrationStore(t)
	ctx := context.Background()
	name := fmt.Sprintf("shared-%d", time.Now().UnixNano())
	cfg := Config{FailureThreshold: 3, OpenTimeout: 30 * time.Second}

	a := NewDistributed(name, s, cfg)
	b := NewDistributed(name, s, cfg)

	// A trips the breaker.
	for i := 0; i < 3; i++ {
		_ = a.Execute(ctx, intFail)
	}
	if got := a.State(ctx); got != StateOpen {
		t.Fatalf("instance A state = %v, want open", got)
	}

	// B shares the Redis state and must see Open without ever having failed.
	if got := b.State(ctx); got != StateOpen {
		t.Fatalf("instance B state = %v, want open (shared across instances)", got)
	}
	ran := false
	err := b.Execute(ctx, func(context.Context) error { ran = true; return nil })
	if ran {
		t.Fatal("instance B ran fn while shared circuit is open")
	}
	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("instance B err = %v, want ErrCircuitOpen", err)
	}
}

// TestIntegration_FullLifecycle drives closed→open→half-open→closed over real
// Redis, using a short OpenTimeout and a real sleep for the lazy promotion (the
// server clock is authoritative here, so we cannot inject a manual clock).
func TestIntegration_FullLifecycle(t *testing.T) {
	s := newIntegrationStore(t)
	ctx := context.Background()
	name := fmt.Sprintf("lifecycle-%d", time.Now().UnixNano())
	cfg := Config{
		FailureThreshold: 2,
		OpenTimeout:      1 * time.Second,
		SuccessThreshold: 1,
	}
	d := NewDistributed(name, s, cfg)

	// Trip it.
	_ = d.Execute(ctx, intFail)
	_ = d.Execute(ctx, intFail)
	if got := d.State(ctx); got != StateOpen {
		t.Fatalf("state = %v, want open", got)
	}

	// Wait out OpenTimeout on the server clock, then a probe success closes it.
	time.Sleep(1200 * time.Millisecond)
	if got := d.State(ctx); got != StateHalfOpen {
		t.Fatalf("after timeout state = %v, want half-open", got)
	}
	if err := d.Execute(ctx, intOK); err != nil {
		t.Fatalf("probe: %v", err)
	}
	if got := d.State(ctx); got != StateClosed {
		t.Fatalf("after probe success state = %v, want closed", got)
	}
}

// TestIntegration_FailOpenOnStoreError points a breaker at a bad Redis address
// (nothing listening) and asserts Execute STILL runs fn — the fail-open
// contract: a store outage must not wedge traffic.
func TestIntegration_FailOpenOnStoreError(t *testing.T) {
	ctx := context.Background()
	// 127.0.0.1:1 has nothing listening → connection refused. The Redis store's
	// default fallback is a per-process memory store WITHOUT circuit-breaker
	// scripts registered, so Eval routes to the fallback and returns an
	// "unregistered script" error → the breaker fails open.
	bad := store.NewRedis(store.RedisOptions{
		Addr:        "127.0.0.1:1",
		DialTimeout: 200 * time.Millisecond,
		MaxRetries:  -1,
		Fallback:    store.NewMemory(), // no CB scripts → Eval errors → fail open
	})
	t.Cleanup(func() { _ = bad.Close() })

	d := NewDistributed("failopen", bad, Config{FailureThreshold: 1})

	ran := false
	err := d.Execute(ctx, func(context.Context) error { ran = true; return nil })
	if !ran {
		t.Fatal("fn did NOT run despite store error; fail-open violated")
	}
	if err != nil {
		t.Fatalf("Execute returned %v; want nil (fn succeeded, store errors swallowed)", err)
	}

	// Even a failing fn must be allowed to run (not rejected) under a store outage.
	ranFail := false
	_ = d.Execute(ctx, func(context.Context) error { ranFail = true; return errors.New("x") })
	if !ranFail {
		t.Fatal("failing fn did not run under store outage; fail-open violated")
	}
}
