package memcached

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/store"
)

func TestStoreInterface(t *testing.T) {
	// Compile-time assertion is in the package; this exercises it at runtime too.
	var _ store.Store = (*Memcached)(nil)
}

func TestGetSetDel(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(newFakeMemcache())

	if _, err := s.Get(ctx, "missing"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Get missing: want ErrNotFound, got %v", err)
	}
	if err := s.Set(ctx, "k", "v", time.Minute); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, err := s.Get(ctx, "k")
	if err != nil || got != "v" {
		t.Fatalf("Get: got %q err %v", got, err)
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
	s := newTestStore(newFakeMemcache())

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
		t.Fatalf("value overwritten: got %q", got)
	}
}

func TestGetSet(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(newFakeMemcache())

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
	s := newTestStore(newFakeMemcache())

	v, err := s.IncrBy(ctx, "c", 5, time.Minute)
	if err != nil || v != 5 {
		t.Fatalf("IncrBy create: v=%d err=%v", v, err)
	}
	v, err = s.IncrBy(ctx, "c", 3, time.Minute)
	if err != nil || v != 8 {
		t.Fatalf("IncrBy add: v=%d err=%v", v, err)
	}
	// Negative delta goes through the CAS loop.
	v, err = s.IncrBy(ctx, "c", -2, time.Minute)
	if err != nil || v != 6 {
		t.Fatalf("IncrBy negative: v=%d err=%v", v, err)
	}
}

func TestPing(t *testing.T) {
	if err := newTestStore(newFakeMemcache()).Ping(context.Background()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

// TestCASRetryOnConflict verifies GetSet retries and succeeds when a concurrent
// writer bumps the CAS version once.
func TestCASRetryOnConflict(t *testing.T) {
	ctx := context.Background()
	f := newFakeMemcache()
	s := newTestStore(f)
	if err := s.Set(ctx, "k", "seed", time.Minute); err != nil {
		t.Fatal(err)
	}

	conflicts := 1
	f.casHook = func(key string) {
		if conflicts > 0 {
			conflicts--
			// Mutate the stored version out from under the CAS by writing directly.
			f.mu.Lock()
			if it, ok := f.data[key]; ok {
				f.nextID++
				it.casID = f.nextID
			}
			f.mu.Unlock()
		}
	}
	old, err := s.GetSet(ctx, "k", "new", time.Minute)
	if err != nil {
		t.Fatalf("GetSet under conflict: %v", err)
	}
	if old != "seed" {
		t.Fatalf("GetSet old value: got %q", old)
	}
	if conflicts != 0 {
		t.Fatalf("expected exactly one injected conflict, %d left", conflicts)
	}
}

// TestConnErrorFallback verifies a connection error routes to the fallback store.
func TestConnErrorFallback(t *testing.T) {
	ctx := context.Background()
	f := newFakeMemcache()
	f.failConn = true
	fallback := store.NewMemory()
	s := newFromRawClient(f, Options{KeyPrefix: "t:", Fallback: fallback})

	if err := s.Set(ctx, "k", "v", time.Minute); err != nil {
		t.Fatalf("Set via fallback: %v", err)
	}
	got, err := s.Get(ctx, "k")
	if err != nil || got != "v" {
		t.Fatalf("Get via fallback: got %q err %v", got, err)
	}
}
