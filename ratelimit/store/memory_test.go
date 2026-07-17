package store_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/store"
)

func TestMemory_GetSet_Basic(t *testing.T) {
	s := store.NewMemory()
	defer s.Close()
	ctx := context.Background()

	if err := s.Set(ctx, "key", "value", 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, err := s.Get(ctx, "key")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != "value" {
		t.Fatalf("expected 'value', got %q", got)
	}
}

func TestMemory_Get_NotFound(t *testing.T) {
	s := store.NewMemory()
	defer s.Close()
	_, err := s.Get(context.Background(), "missing")
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestMemory_TTL_Expiry(t *testing.T) {
	s := store.NewMemory(store.WithCleanupInterval(time.Hour)) // disable background cleanup
	defer s.Close()
	ctx := context.Background()

	if err := s.Set(ctx, "expiring", "val", 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	// Should exist
	if _, err := s.Get(ctx, "expiring"); err != nil {
		t.Fatalf("key should exist before expiry: %v", err)
	}
	// Wait for expiry
	time.Sleep(60 * time.Millisecond)
	if _, err := s.Get(ctx, "expiring"); !errors.Is(err, store.ErrNotFound) {
		t.Fatal("key should have expired")
	}
}

func TestMemory_SetNX_OnlyFirst(t *testing.T) {
	s := store.NewMemory()
	defer s.Close()
	ctx := context.Background()

	ok, err := s.SetNX(ctx, "nx-key", "first", 0)
	if err != nil || !ok {
		t.Fatalf("first SetNX should succeed: ok=%v err=%v", ok, err)
	}
	ok, err = s.SetNX(ctx, "nx-key", "second", 0)
	if err != nil || ok {
		t.Fatalf("second SetNX should fail: ok=%v err=%v", ok, err)
	}
	val, _ := s.Get(ctx, "nx-key")
	if val != "first" {
		t.Fatalf("value should remain 'first', got %q", val)
	}
}

func TestMemory_SetNX_Concurrent(t *testing.T) {
	s := store.NewMemory()
	defer s.Close()
	ctx := context.Background()
	var wins int32
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, _ := s.SetNX(ctx, "race-key", "value", 0)
			if ok {
				atomic.AddInt32(&wins, 1)
			}
		}()
	}
	wg.Wait()
	if wins != 1 {
		t.Fatalf("exactly one SetNX should win, got %d", wins)
	}
}

func TestMemory_IncrBy_Basic(t *testing.T) {
	s := store.NewMemory()
	defer s.Close()
	ctx := context.Background()

	v, err := s.IncrBy(ctx, "counter", 5, 0)
	if err != nil || v != 5 {
		t.Fatalf("first IncrBy: v=%d err=%v", v, err)
	}
	v, err = s.IncrBy(ctx, "counter", 3, 0)
	if err != nil || v != 8 {
		t.Fatalf("second IncrBy: v=%d err=%v", v, err)
	}
}

func TestMemory_IncrBy_Concurrent(t *testing.T) {
	s := store.NewMemory()
	defer s.Close()
	ctx := context.Background()

	var wg sync.WaitGroup
	n := 1000
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			s.IncrBy(ctx, "shared", 1, 0) //nolint:errcheck
		}()
	}
	wg.Wait()

	got, err := s.Get(ctx, "shared")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != fmt.Sprintf("%d", n) {
		t.Fatalf("expected %d, got %s", n, got)
	}
}

func TestMemory_GetSet_Atomic(t *testing.T) {
	s := store.NewMemory()
	defer s.Close()
	ctx := context.Background()

	s.Set(ctx, "k", "old", 0) //nolint:errcheck
	old, err := s.GetSet(ctx, "k", "new", 0)
	if err != nil || old != "old" {
		t.Fatalf("expected old='old': got %q, %v", old, err)
	}
	val, _ := s.Get(ctx, "k")
	if val != "new" {
		t.Fatalf("expected 'new', got %q", val)
	}
}

func TestMemory_Del(t *testing.T) {
	s := store.NewMemory()
	defer s.Close()
	ctx := context.Background()

	s.Set(ctx, "a", "1", 0) //nolint:errcheck
	s.Set(ctx, "b", "2", 0) //nolint:errcheck
	if err := s.Del(ctx, "a", "b"); err != nil {
		t.Fatalf("Del: %v", err)
	}
	if _, err := s.Get(ctx, "a"); !errors.Is(err, store.ErrNotFound) {
		t.Fatal("key 'a' should be deleted")
	}
	if _, err := s.Get(ctx, "b"); !errors.Is(err, store.ErrNotFound) {
		t.Fatal("key 'b' should be deleted")
	}
}

func TestMemory_Eval_RegisteredScript(t *testing.T) {
	s := store.NewMemory()
	defer s.Close()
	ctx := context.Background()

	s.RegisterScript("sum", func(keys []string, args []any) (any, error) {
		return int64(42), nil
	})

	result, err := s.Eval(ctx, "sum", []string{"k"})
	if err != nil {
		t.Fatalf("Eval: %v", err)
	}
	if result.(int64) != 42 {
		t.Fatalf("expected 42, got %v", result)
	}
}

func TestMemory_Eval_UnregisteredScript(t *testing.T) {
	s := store.NewMemory()
	defer s.Close()
	_, err := s.Eval(context.Background(), "unknown-script", nil)
	if err == nil {
		t.Fatal("eval of unknown script should return error")
	}
}

func TestMemory_Ping(t *testing.T) {
	s := store.NewMemory()
	defer s.Close()
	if err := s.Ping(context.Background()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

func TestMemory_Close_Idempotent(t *testing.T) {
	s := store.NewMemory()
	if err := s.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestMemory_SetNX_ExpiredKey(t *testing.T) {
	s := store.NewMemory(store.WithCleanupInterval(time.Hour))
	defer s.Close()
	ctx := context.Background()

	s.Set(ctx, "key", "value", 50*time.Millisecond)
	time.Sleep(60 * time.Millisecond)

	ok, err := s.SetNX(ctx, "key", "newvalue", 0)
	if err != nil {
		t.Fatalf("SetNX: %v", err)
	}
	if !ok {
		t.Fatal("SetNX should succeed for expired key")
	}
}

func TestMemory_GetSet_Expired(t *testing.T) {
	s := store.NewMemory(store.WithCleanupInterval(time.Hour))
	defer s.Close()
	ctx := context.Background()

	s.Set(ctx, "key", "old", 50*time.Millisecond)
	time.Sleep(60 * time.Millisecond)

	old, err := s.GetSet(ctx, "key", "new", 0)
	if err != nil {
		t.Fatalf("GetSet: %v", err)
	}
	if old != "" {
		t.Fatalf("expected empty old value for expired key, got %q", old)
	}
}

func TestMemory_IncrBy_Expired(t *testing.T) {
	s := store.NewMemory(store.WithCleanupInterval(time.Hour))
	defer s.Close()
	ctx := context.Background()

	s.IncrBy(ctx, "counter", 5, 50*time.Millisecond)
	time.Sleep(60 * time.Millisecond)

	v, err := s.IncrBy(ctx, "counter", 3, 0)
	if err != nil {
		t.Fatalf("IncrBy: %v", err)
	}
	if v != 3 {
		t.Fatalf("expected 3 for expired key, got %d", v)
	}
}

func TestMemory_IncrBy_InvalidValue(t *testing.T) {
	s := store.NewMemory()
	defer s.Close()
	ctx := context.Background()

	s.Set(ctx, "notanumber", "invalid", 0)

	_, err := s.IncrBy(ctx, "notanumber", 1, 0)
	if err == nil {
		t.Fatal("expected error for non-integer value")
	}
}

func TestMemory_TtlToTime(t *testing.T) {
	s := store.NewMemory()
	defer s.Close()
	ctx := context.Background()

	if err := s.Set(ctx, "a", "1", -1); err != nil {
		t.Fatalf("Set with negative TTL: %v", err)
	}
	val, _ := s.Get(ctx, "a")
	if val != "1" {
		t.Fatalf("negative TTL should mean no expiry, got %q", val)
	}
}

func TestMemory_Concurrent_SetGet(t *testing.T) {
	s := store.NewMemory()
	defer s.Close()
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", i%10)
			s.Set(ctx, key, fmt.Sprintf("value-%d", i), 0)
		}(i)
	}
	wg.Wait()

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key-%d", i)
		val, err := s.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get %s: %v", key, err)
		}
		if val == "" {
			t.Fatalf("key %s should have value", key)
		}
	}
}

func TestMemory_GetSet_NilKey(t *testing.T) {
	s := store.NewMemory()
	defer s.Close()
	ctx := context.Background()

	err := s.Set(ctx, "", "value", 0)
	if err != nil {
		t.Fatalf("Set with empty key: %v", err)
	}
}

func TestMemory_IncrBy_ConcurrentDifferentKeys(t *testing.T) {
	s := store.NewMemory()
	defer s.Close()
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		for j := 0; j < 10; j++ {
			wg.Add(1)
			go func(key string) {
				defer wg.Done()
				s.IncrBy(ctx, key, 1, 0)
			}(fmt.Sprintf("key-%d", j))
		}
	}
	wg.Wait()

	for j := 0; j < 10; j++ {
		val, err := s.Get(ctx, fmt.Sprintf("key-%d", j))
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if val != "50" {
			t.Fatalf("key-%d: expected 50, got %s", j, val)
		}
	}
}

func TestMemory_MaxKeys_Set(t *testing.T) {
	s := store.NewMemory(store.WithMaxKeys(3))
	defer s.Close()
	ctx := context.Background()

	if err := s.Set(ctx, "key1", "val1", 0); err != nil {
		t.Fatalf("Set key1: %v", err)
	}
	if err := s.Set(ctx, "key2", "val2", 0); err != nil {
		t.Fatalf("Set key2: %v", err)
	}
	if err := s.Set(ctx, "key3", "val3", 0); err != nil {
		t.Fatalf("Set key3: %v", err)
	}

	// Fourth key should fail
	err := s.Set(ctx, "key4", "val4", 0)
	if err == nil {
		t.Fatal("expected error for exceeding max keys")
	}
}

func TestMemory_MaxKeys_SetNX(t *testing.T) {
	s := store.NewMemory(store.WithMaxKeys(2))
	defer s.Close()
	ctx := context.Background()

	ok, err := s.SetNX(ctx, "key1", "val1", 0)
	if err != nil || !ok {
		t.Fatalf("SetNX key1: ok=%v err=%v", ok, err)
	}
	ok, err = s.SetNX(ctx, "key2", "val2", 0)
	if err != nil || !ok {
		t.Fatalf("SetNX key2: ok=%v err=%v", ok, err)
	}

	// Third key should fail
	ok, err = s.SetNX(ctx, "key3", "val3", 0)
	if err == nil {
		t.Fatal("expected error for exceeding max keys")
	}
}

func TestMemory_MaxKeys_IncrBy(t *testing.T) {
	s := store.NewMemory(store.WithMaxKeys(2))
	defer s.Close()
	ctx := context.Background()

	_, err := s.IncrBy(ctx, "key1", 1, 0)
	if err != nil {
		t.Fatalf("IncrBy key1: %v", err)
	}
	_, err = s.IncrBy(ctx, "key2", 1, 0)
	if err != nil {
		t.Fatalf("IncrBy key2: %v", err)
	}

	// Third key should fail
	_, err = s.IncrBy(ctx, "key3", 1, 0)
	if err == nil {
		t.Fatal("expected error for exceeding max keys")
	}
}
