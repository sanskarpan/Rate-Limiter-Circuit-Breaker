package fallback_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/fallback"
)

// ExampleCached demonstrates serving a last-known-good value when the primary
// lookup fails within the stale-while-error window.
func ExampleCached() {
	c := fallback.NewCached[string](fallback.CacheConfig{
		TTL:             30 * time.Second,
		StaleWhileError: 5 * time.Minute,
	})

	// First call succeeds and caches the value.
	v, err := c.Do(context.Background(), "user:42", func(context.Context) (string, error) {
		return "Ada Lovelace", nil
	})
	fmt.Printf("fresh:  value=%q stale=%v\n", v, errors.Is(err, fallback.ErrStale))

	// Later, the primary fails but the cached value is still within
	// StaleWhileError, so the stale value is served with ErrStale.
	v, err = c.Do(context.Background(), "user:42", func(context.Context) (string, error) {
		return "", errors.New("backend down")
	})
	fmt.Printf("failed: value=%q stale=%v\n", v, errors.Is(err, fallback.ErrStale))

	// Output:
	// fresh:  value="Ada Lovelace" stale=false
	// failed: value="Ada Lovelace" stale=true
}

// ExampleStatic pairs a static default value with DoWithResult.
func ExampleStatic() {
	limit, _ := fallback.DoWithResult(context.Background(),
		func(context.Context) (int, error) { return 0, errors.New("config service unavailable") },
		fallback.Static(100), // default rate limit
	)
	fmt.Println("rate limit:", limit)
	// Output:
	// rate limit: 100
}
