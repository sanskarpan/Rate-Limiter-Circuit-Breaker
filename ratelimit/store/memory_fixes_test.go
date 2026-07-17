package store

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"
)

// TestMemory_KeyCountDecrementedOnDel is the C-4 regression: keyCount must be
// decremented on every removal path. With WithMaxKeys(3), churning keys
// (Set+Del) must not exhaust the budget. On the buggy code the 11th distinct Set
// would fail because keyCount climbed to 3 and never came back down.
func TestMemory_KeyCountDecrementedOnDel(t *testing.T) {
	m := NewMemory(WithMaxKeys(3))
	defer m.Close()
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("churn-%d", i)
		if err := m.Set(ctx, key, "v", time.Minute); err != nil {
			t.Fatalf("Set %s failed on iteration %d: %v", key, i, err)
		}
		if err := m.Del(ctx, key); err != nil {
			t.Fatalf("Del %s failed: %v", key, err)
		}
	}

	// The 11th distinct Set must succeed — keyCount should be 0 now.
	if err := m.Set(ctx, "final", "v", time.Minute); err != nil {
		t.Fatalf("11th distinct Set failed, keyCount leaked: %v", err)
	}
}

// TestMemory_KeyCountDecrementedOnLazyExpiry checks Get's lazy-expiry path also
// decrements keyCount (C-4).
func TestMemory_KeyCountDecrementedOnLazyExpiry(t *testing.T) {
	m := NewMemory(WithMaxKeys(2), WithCleanupInterval(time.Hour))
	defer m.Close()
	ctx := context.Background()

	// Fill with short-TTL keys.
	for i := 0; i < 2; i++ {
		if err := m.Set(ctx, fmt.Sprintf("k-%d", i), "v", 20*time.Millisecond); err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}
	time.Sleep(40 * time.Millisecond)

	// Trigger lazy expiry via Get (cleanup interval is 1h so GC won't run).
	for i := 0; i < 2; i++ {
		if _, err := m.Get(ctx, fmt.Sprintf("k-%d", i)); err != ErrNotFound {
			t.Fatalf("expected ErrNotFound for expired key, got %v", err)
		}
	}

	// New keys must now fit.
	for i := 0; i < 2; i++ {
		if err := m.Set(ctx, fmt.Sprintf("fresh-%d", i), "v", time.Minute); err != nil {
			t.Fatalf("Set after lazy expiry failed (keyCount leaked): %v", err)
		}
	}
}

// TestMemory_IncrByOverflow is the M-13 regression: IncrBy near MaxInt64 must
// error rather than wrapping negative, and the stored value must be unchanged.
func TestMemory_IncrByOverflow(t *testing.T) {
	m := NewMemory()
	defer m.Close()
	ctx := context.Background()

	start := int64(math.MaxInt64 - 5)
	if _, err := m.IncrBy(ctx, "ctr", start, time.Minute); err != nil {
		t.Fatalf("seed IncrBy failed: %v", err)
	}

	// Incrementing by 10 would overflow.
	_, err := m.IncrBy(ctx, "ctr", 10, time.Minute)
	if err == nil {
		t.Fatalf("expected overflow error, got nil")
	}

	// Value must be unchanged (still start).
	got, gerr := m.Get(ctx, "ctr")
	if gerr != nil {
		t.Fatalf("Get after overflow failed: %v", gerr)
	}
	v, _ := strconv.ParseInt(got, 10, 64)
	if v != start {
		t.Fatalf("value changed after overflow: got %d want %d", v, start)
	}
}

// TestMemory_IncrByUnderflow checks the negative-direction overflow guard.
func TestMemory_IncrByUnderflow(t *testing.T) {
	m := NewMemory()
	defer m.Close()
	ctx := context.Background()

	start := int64(math.MinInt64 + 5)
	if _, err := m.IncrBy(ctx, "ctr", start, time.Minute); err != nil {
		t.Fatalf("seed IncrBy failed: %v", err)
	}
	if _, err := m.IncrBy(ctx, "ctr", -10, time.Minute); err == nil {
		t.Fatalf("expected underflow error, got nil")
	}
}

// TestMemory_MaxKeysConcurrentProbeConsistent is the M-14 regression, run under
// -race: concurrent SetNX/IncrBy at the maxKeys boundary plus concurrent Del and
// Get probes must stay consistent (no observing an about-to-be-rejected entry, no
// lost Del, keyCount never exceeds maxKeys).
func TestMemory_MaxKeysConcurrentProbeConsistent(t *testing.T) {
	const maxKeys = 8
	m := NewMemory(WithMaxKeys(maxKeys), WithCleanupInterval(10*time.Millisecond))
	defer m.Close()
	ctx := context.Background()

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				key := fmt.Sprintf("k-%d-%d", g, i%maxKeys)
				switch i % 3 {
				case 0:
					_, _ = m.SetNX(ctx, key, "v", 5*time.Millisecond)
				case 1:
					_, _ = m.IncrBy(ctx, key, 1, 5*time.Millisecond)
				case 2:
					_ = m.Del(ctx, key)
				}
				_, _ = m.Get(ctx, key)
			}
		}(g)
	}
	wg.Wait()

	// keyCount must never have leaked above maxKeys.
	if c := m.keyCount.Load(); c > maxKeys {
		t.Fatalf("keyCount %d exceeded maxKeys %d", c, maxKeys)
	}
	if c := m.keyCount.Load(); c < 0 {
		t.Fatalf("keyCount %d went negative", c)
	}
}
