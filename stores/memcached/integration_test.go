//go:build integration

package memcached_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/store"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/stores/memcached"
)

// These tests run only under `-tags=integration` and require a live memcached.
// Set MEMCACHED_ADDR (default "localhost:11211").
//
//	docker run -p 11211:11211 memcached:1.6-alpine
//	cd stores && go test -tags=integration ./memcached/...
func mcAddr() string {
	if a := os.Getenv("MEMCACHED_ADDR"); a != "" {
		return a
	}
	return "localhost:11211"
}

func newLiveStore(t *testing.T) *memcached.Memcached {
	t.Helper()
	s := memcached.New(memcached.Options{
		Servers:   []string{mcAddr()},
		KeyPrefix: "itest:",
	})
	if err := s.Ping(context.Background()); err != nil {
		t.Skipf("memcached not reachable at %s: %v", mcAddr(), err)
	}
	return s
}

func TestIntegrationGetSetIncr(t *testing.T) {
	ctx := context.Background()
	s := newLiveStore(t)
	defer s.Close()

	key := "int-basic"
	_ = s.Del(ctx, key)

	if err := s.Set(ctx, key, "hello", time.Minute); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(ctx, key)
	if err != nil || got != "hello" {
		t.Fatalf("Get: %q %v", got, err)
	}

	ctr := "int-ctr"
	_ = s.Del(ctx, ctr)
	v, err := s.IncrBy(ctx, ctr, 4, time.Minute)
	if err != nil || v != 4 {
		t.Fatalf("IncrBy create: %d %v", v, err)
	}
	v, err = s.IncrBy(ctx, ctr, 3, time.Minute)
	if err != nil || v != 7 {
		t.Fatalf("IncrBy add: %d %v", v, err)
	}
}

func TestIntegrationTokenBucket(t *testing.T) {
	ctx := context.Background()
	s := newLiveStore(t)
	defer s.Close()

	key := "int-tb"
	_ = s.Del(ctx, key)

	capacity := 3.0
	refillRate := 1e-18
	now := time.Now().UnixNano()
	ttlMs := int64(60_000)

	allowed := 0
	for i := 0; i < 5; i++ {
		res, err := s.Eval(ctx, store.TokenBucketScriptID, []string{key},
			capacity, refillRate, int64(1), now, ttlMs, "0")
		if err != nil {
			t.Fatalf("Eval: %v", err)
		}
		out := res.([]any)
		if out[0].(int64) == 1 {
			allowed++
		}
	}
	if allowed != 3 {
		t.Fatalf("live token bucket admitted %d, want 3", allowed)
	}
}
