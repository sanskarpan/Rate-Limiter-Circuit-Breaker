package fallback_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/fallback"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
)

var errBoom = errors.New("boom")

func newTestClock() *clock.ManualClock {
	return clock.NewManualClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
}

// success caches; a subsequent failure serves the stale value within
// StaleWhileError, and returns the error once beyond it.
func TestCached_StaleWhileError(t *testing.T) {
	clk := newTestClock()
	c := fallback.NewCached[string](fallback.CacheConfig{
		TTL:             30 * time.Second,
		StaleWhileError: 5 * time.Minute,
	}).WithClock(clk)

	// Prime the cache with a success.
	v, err := c.Do(context.Background(), "k", func(context.Context) (string, error) {
		return "good", nil
	})
	if err != nil {
		t.Fatalf("initial Do: unexpected error %v", err)
	}
	if v != "good" {
		t.Fatalf("initial Do: got %q want %q", v, "good")
	}

	// Advance within StaleWhileError; primary now fails -> serve stale.
	clk.Advance(2 * time.Minute)
	v, err = c.Do(context.Background(), "k", func(context.Context) (string, error) {
		return "", errBoom
	})
	if !errors.Is(err, fallback.ErrStale) {
		t.Fatalf("within stale window: got err %v want ErrStale", err)
	}
	if v != "good" {
		t.Fatalf("within stale window: got value %q want %q", v, "good")
	}

	// Advance beyond StaleWhileError (total 6m > 5m); primary fails -> error.
	clk.Advance(4 * time.Minute)
	v, err = c.Do(context.Background(), "k", func(context.Context) (string, error) {
		return "", errBoom
	})
	if !errors.Is(err, errBoom) {
		t.Fatalf("beyond stale window: got err %v want errBoom", err)
	}
	if v != "" {
		t.Fatalf("beyond stale window: got value %q want empty", v)
	}
}

// With no cached value at all, a primary failure surfaces the error.
func TestCached_NoCacheReturnsError(t *testing.T) {
	clk := newTestClock()
	c := fallback.NewCached[int](fallback.CacheConfig{
		StaleWhileError: time.Minute,
	}).WithClock(clk)

	_, err := c.Do(context.Background(), "missing", func(context.Context) (int, error) {
		return 0, errBoom
	})
	if !errors.Is(err, errBoom) {
		t.Fatalf("got %v want errBoom", err)
	}
}

// In ModeAlwaysCallPrimary, the primary is invoked on every call even while a
// fresh value exists (fresh data on the happy path).
func TestCached_AlwaysCallPrimary_CallsEveryTime(t *testing.T) {
	clk := newTestClock()
	c := fallback.NewCached[int](fallback.CacheConfig{
		TTL:             time.Minute,
		StaleWhileError: time.Hour,
		Mode:            fallback.ModeAlwaysCallPrimary,
	}).WithClock(clk)

	var calls int64
	do := func() (int, error) {
		return c.Do(context.Background(), "k", func(context.Context) (int, error) {
			atomic.AddInt64(&calls, 1)
			return 7, nil
		})
	}

	for i := 0; i < 3; i++ {
		if v, err := do(); err != nil || v != 7 {
			t.Fatalf("call %d: got (%d,%v)", i, v, err)
		}
	}
	if got := atomic.LoadInt64(&calls); got != 3 {
		t.Fatalf("primary call count = %d want 3", got)
	}
}

// In ModeServeFreshFromCache, the primary is NOT called again while the value
// is within TTL, then is called once past TTL.
func TestCached_ServeFreshFromCache_SkipsPrimary(t *testing.T) {
	clk := newTestClock()
	c := fallback.NewCached[int](fallback.CacheConfig{
		TTL:             time.Minute,
		StaleWhileError: time.Hour,
		Mode:            fallback.ModeServeFreshFromCache,
	}).WithClock(clk)

	var calls int64
	do := func() (int, error) {
		return c.Do(context.Background(), "k", func(context.Context) (int, error) {
			atomic.AddInt64(&calls, 1)
			return 42, nil
		})
	}

	// First call populates the cache (1 primary call).
	if v, err := do(); err != nil || v != 42 {
		t.Fatalf("first call: got (%d,%v)", v, err)
	}
	// Within TTL: no additional primary calls.
	clk.Advance(30 * time.Second)
	for i := 0; i < 5; i++ {
		if v, err := do(); err != nil || v != 42 {
			t.Fatalf("fresh call %d: got (%d,%v)", i, v, err)
		}
	}
	if got := atomic.LoadInt64(&calls); got != 1 {
		t.Fatalf("primary calls within TTL = %d want 1", got)
	}

	// Past TTL: one more primary call.
	clk.Advance(2 * time.Minute)
	if v, err := do(); err != nil || v != 42 {
		t.Fatalf("post-TTL call: got (%d,%v)", v, err)
	}
	if got := atomic.LoadInt64(&calls); got != 2 {
		t.Fatalf("primary calls after TTL = %d want 2", got)
	}
}

// N concurrent Do(sameKey) with a slow primary trigger exactly one primary call
// and all callers receive the value.
func TestCached_SingleFlight(t *testing.T) {
	c := fallback.NewCached[int](fallback.CacheConfig{
		TTL:             time.Minute,
		StaleWhileError: time.Hour,
	})

	const n = 50
	var calls int64
	release := make(chan struct{})
	var started sync.WaitGroup
	started.Add(1)
	var once sync.Once

	primary := func(context.Context) (int, error) {
		once.Do(started.Done)
		atomic.AddInt64(&calls, 1)
		<-release // block until all goroutines have joined
		return 99, nil
	}

	var wg sync.WaitGroup
	results := make([]int, n)
	errs := make([]error, n)
	// Launch one caller first and wait until its primary is actually running,
	// guaranteeing the rest join the same in-flight call.
	wg.Add(1)
	go func() {
		defer wg.Done()
		results[0], errs[0] = c.Do(context.Background(), "k", primary)
	}()
	started.Wait()

	for i := 1; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = c.Do(context.Background(), "k", primary)
		}(i)
	}
	// Give joiners a moment to register on the single-flight call.
	time.Sleep(20 * time.Millisecond)
	close(release)
	wg.Wait()

	if got := atomic.LoadInt64(&calls); got != 1 {
		t.Fatalf("primary call count = %d want 1", got)
	}
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("caller %d: unexpected error %v", i, errs[i])
		}
		if results[i] != 99 {
			t.Fatalf("caller %d: got %d want 99", i, results[i])
		}
	}
}

// Concurrent access to distinct keys and repeated refreshes under -race.
func TestCached_ConcurrentRace(t *testing.T) {
	c := fallback.NewCached[int](fallback.CacheConfig{
		TTL:             time.Millisecond,
		StaleWhileError: time.Second,
		MaxEntries:      8,
	})
	keys := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}

	var wg sync.WaitGroup
	for g := 0; g < 20; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				k := keys[(g+i)%len(keys)]
				_, _ = c.Do(context.Background(), k, func(context.Context) (int, error) {
					if i%3 == 0 {
						return 0, errBoom
					}
					return i, nil
				})
			}
		}(g)
	}
	wg.Wait()
}

// MaxEntries bounds the number of retained keys.
func TestCached_MaxEntriesEviction(t *testing.T) {
	clk := newTestClock()
	c := fallback.NewCached[int](fallback.CacheConfig{
		TTL:             time.Hour,
		StaleWhileError: time.Hour,
		Mode:            fallback.ModeServeFreshFromCache,
		MaxEntries:      2,
	}).WithClock(clk)

	store := func(key string, val int) {
		v, err := c.Do(context.Background(), key, func(context.Context) (int, error) {
			return val, nil
		})
		if err != nil || v != val {
			t.Fatalf("store %s: got (%d,%v)", key, v, err)
		}
		clk.Advance(time.Second) // distinct timestamps for oldest-eviction
	}

	store("a", 1)
	store("b", 2)
	store("c", 3) // evicts "a" (oldest)

	// "a" should be gone: a failing primary now surfaces the error (no stale).
	_, err := c.Do(context.Background(), "a", func(context.Context) (int, error) {
		return 0, errBoom
	})
	if !errors.Is(err, errBoom) {
		t.Fatalf("evicted key a: got %v want errBoom", err)
	}
	// "c" should still be fresh and served without calling primary.
	v, err := c.Do(context.Background(), "c", func(context.Context) (int, error) {
		t.Fatal("primary should not be called for fresh key c")
		return 0, nil
	})
	if err != nil || v != 3 {
		t.Fatalf("fresh key c: got (%d,%v)", v, err)
	}
}

// Static returns the fixed value on primary failure via DoWithResult.
func TestStatic(t *testing.T) {
	v, err := fallback.DoWithResult(context.Background(),
		func(context.Context) (string, error) { return "", errBoom },
		fallback.Static("default"),
	)
	if err != nil {
		t.Fatalf("Static: unexpected error %v", err)
	}
	if v != "default" {
		t.Fatalf("Static: got %q want %q", v, "default")
	}

	// On success the fallback is never invoked.
	v, err = fallback.DoWithResult(context.Background(),
		func(context.Context) (string, error) { return "live", nil },
		fallback.Static("default"),
	)
	if err != nil || v != "live" {
		t.Fatalf("Static success path: got (%q,%v)", v, err)
	}
}

// StaticErr preserves the original error alongside the default value.
func TestStaticErr(t *testing.T) {
	v, err := fallback.DoWithResult(context.Background(),
		func(context.Context) (int, error) { return 0, errBoom },
		fallback.StaticErr(-1),
	)
	if !errors.Is(err, errBoom) {
		t.Fatalf("StaticErr: got err %v want errBoom", err)
	}
	if v != -1 {
		t.Fatalf("StaticErr: got %d want -1", v)
	}
}
