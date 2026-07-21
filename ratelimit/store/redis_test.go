//go:build integration

// Integration tests for the Redis store.
// Run with: go test ./ratelimit/store/ -tags=integration -run TestRedis
// Requires a Redis instance at localhost:6379 or REDIS_ADDR env var.
package store_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/store"
)

func redisAddr() string {
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		return addr
	}
	return "localhost:6379"
}

func newTestRedis(t *testing.T) *store.Redis {
	t.Helper()
	s := store.NewRedis(store.RedisOptions{
		Addr:      redisAddr(),
		KeyPrefix: fmt.Sprintf("test:%d:", time.Now().UnixNano()),
	})
	if err := s.Ping(context.Background()); err != nil {
		t.Skipf("Redis not available at %s: %v", redisAddr(), err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestRedis_Ping(t *testing.T) {
	s := newTestRedis(t)
	if err := s.Ping(context.Background()); err != nil {
		t.Fatalf("ping failed: %v", err)
	}
}

func TestRedis_GetSet(t *testing.T) {
	s := newTestRedis(t)
	ctx := context.Background()

	// Get non-existent key
	_, err := s.Get(ctx, "nokey")
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	// Set + Get
	err = s.Set(ctx, "key1", "hello", time.Minute)
	if err != nil {
		t.Fatalf("set: %v", err)
	}

	val, err := s.Get(ctx, "key1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if val != "hello" {
		t.Fatalf("expected 'hello', got %q", val)
	}
}

func TestRedis_SetNX(t *testing.T) {
	s := newTestRedis(t)
	ctx := context.Background()

	ok, err := s.SetNX(ctx, "nx1", "v1", time.Minute)
	if err != nil {
		t.Fatalf("setnx 1: %v", err)
	}
	if !ok {
		t.Fatal("expected set to succeed on new key")
	}

	ok, err = s.SetNX(ctx, "nx1", "v2", time.Minute)
	if err != nil {
		t.Fatalf("setnx 2: %v", err)
	}
	if ok {
		t.Fatal("expected set to fail on existing key")
	}

	// Value should still be v1
	val, _ := s.Get(ctx, "nx1")
	if val != "v1" {
		t.Fatalf("expected v1, got %q", val)
	}
}

func TestRedis_IncrBy(t *testing.T) {
	s := newTestRedis(t)
	ctx := context.Background()

	v1, err := s.IncrBy(ctx, "counter", 5, time.Minute)
	if err != nil {
		t.Fatalf("incrby 1: %v", err)
	}
	if v1 != 5 {
		t.Fatalf("expected 5, got %d", v1)
	}

	v2, err := s.IncrBy(ctx, "counter", 3, time.Minute)
	if err != nil {
		t.Fatalf("incrby 2: %v", err)
	}
	if v2 != 8 {
		t.Fatalf("expected 8, got %d", v2)
	}
}

func TestRedis_Del(t *testing.T) {
	s := newTestRedis(t)
	ctx := context.Background()

	s.Set(ctx, "del1", "a", time.Minute)
	s.Set(ctx, "del2", "b", time.Minute)

	if err := s.Del(ctx, "del1", "del2"); err != nil {
		t.Fatalf("del: %v", err)
	}

	for _, k := range []string{"del1", "del2"} {
		_, err := s.Get(ctx, k)
		if !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("key %q should be deleted, got: %v", k, err)
		}
	}
}

func TestRedis_TTL_Expiry(t *testing.T) {
	s := newTestRedis(t)
	ctx := context.Background()

	err := s.Set(ctx, "ttlkey", "expiring", 100*time.Millisecond)
	if err != nil {
		t.Fatalf("set: %v", err)
	}

	// Should exist immediately
	val, err := s.Get(ctx, "ttlkey")
	if err != nil {
		t.Fatalf("get before expiry: %v", err)
	}
	if val != "expiring" {
		t.Fatalf("expected 'expiring', got %q", val)
	}

	// Wait for expiry
	time.Sleep(200 * time.Millisecond)
	_, err = s.Get(ctx, "ttlkey")
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected ErrNotFound after TTL, got %v", err)
	}
}

func TestRedis_Concurrent_IncrBy(t *testing.T) {
	s := newTestRedis(t)
	ctx := context.Background()

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			s.IncrBy(ctx, "concurrent_counter", 1, time.Minute)
		}()
	}
	wg.Wait()

	val, err := s.Get(ctx, "concurrent_counter")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	n, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if n != goroutines {
		t.Fatalf("expected %d, got %d (lost some increments)", goroutines, n)
	}
}

func TestRedis_GetSet_AtomicSwap(t *testing.T) {
	s := newTestRedis(t)
	ctx := context.Background()

	// GetSet on new key returns ""
	old, err := s.GetSet(ctx, "swap1", "new_value", time.Minute)
	if err != nil {
		t.Fatalf("getset 1: %v", err)
	}
	if old != "" {
		t.Fatalf("expected empty old value, got %q", old)
	}

	// GetSet on existing key returns old value
	old, err = s.GetSet(ctx, "swap1", "newer_value", time.Minute)
	if err != nil {
		t.Fatalf("getset 2: %v", err)
	}
	if old != "new_value" {
		t.Fatalf("expected 'new_value', got %q", old)
	}
}

func TestRedis_Eval_GCRAScript(t *testing.T) {
	s := newTestRedis(t)
	ctx := context.Background()

	// Simulate a GCRA check
	nowNs := time.Now().UnixNano()
	emissionIntervalNs := int64(time.Second / 10) // 10 req/s
	burst := 5
	ttlMs := 60000

	result, err := s.Eval(ctx, store.GCRAScriptID,
		[]string{"gcra_test"},
		emissionIntervalNs, burst, 1, nowNs, ttlMs,
	)
	if err != nil {
		t.Fatalf("eval: %v", err)
	}

	arr, ok := result.([]any)
	if !ok {
		t.Fatalf("expected []any, got %T: %v", result, result)
	}
	if len(arr) < 2 {
		t.Fatalf("expected at least 2 elements, got %d", len(arr))
	}

	// First call should be allowed
	allowed, _ := arr[0].(int64)
	if allowed != 1 {
		t.Fatalf("expected allowed=1, got %d", allowed)
	}
}
