package dynamodb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/store"
)

func TestStoreInterface(t *testing.T) {
	var _ store.Store = (*DynamoDB)(nil)
}

func TestNewRequiresTable(t *testing.T) {
	_, err := NewFromAPI(newFakeDDB("pk"), Options{})
	if err == nil {
		t.Fatal("expected error when TableName is empty")
	}
}

func TestGetSetDel(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(newFakeDDB("pk"))

	if _, err := s.Get(ctx, "missing"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Get missing: want ErrNotFound, got %v", err)
	}
	if err := s.Set(ctx, "k", "v", time.Minute); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, err := s.Get(ctx, "k")
	if err != nil || got != "v" {
		t.Fatalf("Get: %q %v", got, err)
	}
	if err := s.Del(ctx, "k"); err != nil {
		t.Fatalf("Del: %v", err)
	}
	if _, err := s.Get(ctx, "k"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Get after Del: want ErrNotFound, got %v", err)
	}
}

func TestSetNX(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(newFakeDDB("pk"))

	ok, err := s.SetNX(ctx, "k", "a", time.Minute)
	if err != nil || !ok {
		t.Fatalf("first SetNX: ok=%v err=%v", ok, err)
	}
	ok, err = s.SetNX(ctx, "k", "b", time.Minute)
	if err != nil || ok {
		t.Fatalf("second SetNX: want ok=false, got ok=%v err=%v", ok, err)
	}
	got, _ := s.Get(ctx, "k")
	if got != "a" {
		t.Fatalf("value overwritten: %q", got)
	}
}

func TestSetNXExpiredAllowsReset(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(newFakeDDB("pk"))

	// TTL already in the past → logically expired → SetNX must succeed to reset.
	if ok, err := s.SetNX(ctx, "k", "a", -time.Second); err != nil || !ok {
		t.Fatalf("SetNX with no TTL: ok=%v err=%v", ok, err)
	}
	// "a" has no exp attribute (ttl<=0), so it never expires; a second SetNX fails.
	if ok, _ := s.SetNX(ctx, "k", "b", time.Minute); ok {
		t.Fatal("expected second SetNX to fail against a live key")
	}
}

func TestGetSet(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(newFakeDDB("pk"))

	old, err := s.GetSet(ctx, "k", "v1", time.Minute)
	if err != nil || old != "" {
		t.Fatalf("GetSet new: old=%q err=%v", old, err)
	}
	old, err = s.GetSet(ctx, "k", "v2", time.Minute)
	if err != nil || old != "v1" {
		t.Fatalf("GetSet existing: old=%q err=%v", old, err)
	}
}

func TestIncrBy(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(newFakeDDB("pk"))

	v, err := s.IncrBy(ctx, "c", 5, time.Minute)
	if err != nil || v != 5 {
		t.Fatalf("IncrBy create: %d %v", v, err)
	}
	v, err = s.IncrBy(ctx, "c", 3, time.Minute)
	if err != nil || v != 8 {
		t.Fatalf("IncrBy add: %d %v", v, err)
	}
	v, err = s.IncrBy(ctx, "c", -2, time.Minute)
	if err != nil || v != 6 {
		t.Fatalf("IncrBy negative: %d %v", v, err)
	}
	// Get on a counter-only item returns the numeric value.
	got, err := s.Get(ctx, "c")
	if err != nil || got != "6" {
		t.Fatalf("Get counter: %q %v", got, err)
	}
}

func TestPing(t *testing.T) {
	if err := newTestStore(newFakeDDB("pk")).Ping(context.Background()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

func TestConnErrorFallback(t *testing.T) {
	ctx := context.Background()
	f := newFakeDDB("pk")
	f.failConn = true
	fallback := store.NewMemory()
	s, err := NewFromAPI(f, Options{TableName: "rl", KeyPrefix: "t:", Fallback: fallback})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Set(ctx, "k", "v", time.Minute); err != nil {
		t.Fatalf("Set via fallback: %v", err)
	}
	got, err := s.Get(ctx, "k")
	if err != nil || got != "v" {
		t.Fatalf("Get via fallback: %q %v", got, err)
	}
}
