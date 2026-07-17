package fallback

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
)

// ErrStale is returned (wrapped) by Cached.Do alongside a stale cached value
// when the primary function fails but a within-StaleWhileError value exists.
//
// Callers that want to treat a stale hit as a success should check
// errors.Is(err, ErrStale) and use the returned value anyway. The Do method
// also returns a distinct *bool*-free signal via the returned error: a nil
// error means a fresh/served value, ErrStale means a served-but-stale value,
// and any other error means total failure with no usable cached value.
var ErrStale = errors.New("fallback: serving stale cached value")

// CacheMode selects how Cached decides whether to invoke the primary function.
type CacheMode int

const (
	// ModeAlwaysCallPrimary always invokes the primary function and only falls
	// back to the cached value when the primary fails. This is the core
	// "stale-while-error" behavior: fresh data on the happy path, last-known-good
	// data on failure. It is the zero value / default.
	ModeAlwaysCallPrimary CacheMode = iota

	// ModeServeFreshFromCache serves the cached value without calling the primary
	// while the value is still within TTL ("fresh"). Once the value is older than
	// TTL, the primary is invoked to refresh it; on refresh failure the stale
	// value is served if still within StaleWhileError. This is a read-through
	// cache with a stale-while-error safety net.
	ModeServeFreshFromCache
)

// CacheConfig configures a Cached instance.
type CacheConfig struct {
	// TTL is how long a stored value is considered "fresh".
	//
	//   - In ModeServeFreshFromCache, a value younger than TTL is served without
	//     calling the primary.
	//   - In ModeAlwaysCallPrimary, TTL is ignored for the happy path (the primary
	//     is always called) but a non-positive TTL is still valid.
	//
	// A non-positive TTL means "never fresh" (every call reaches the primary).
	TTL time.Duration

	// StaleWhileError is the additional window, measured from the time a value was
	// stored, during which a stale value may be served when the primary fails.
	// A value stored at T may be served as stale until T+StaleWhileError.
	//
	// A non-positive StaleWhileError disables stale serving entirely: primary
	// failures always surface as errors.
	StaleWhileError time.Duration

	// Mode selects the refresh strategy. Defaults to ModeAlwaysCallPrimary.
	Mode CacheMode

	// MaxEntries bounds the number of keys retained. When exceeded, the oldest
	// (least-recently-stored) entry is evicted. Zero or negative means unbounded.
	MaxEntries int
}

// entry is a single cached value with the wall-clock time it was stored.
type entry[T any] struct {
	value    T
	storedAt time.Time
}

// callGroup single-flights concurrent refreshes of the same key. It is a
// minimal stdlib implementation of the golang.org/x/sync/singleflight pattern
// (which is not a dependency of this module).
type callGroup[T any] struct {
	mu    sync.Mutex
	calls map[string]*call[T]
}

// call is one in-flight primary invocation whose result is shared by all
// goroutines that joined while it was running.
type call[T any] struct {
	wg  sync.WaitGroup
	val T
	err error
}

func newCallGroup[T any]() *callGroup[T] {
	return &callGroup[T]{calls: make(map[string]*call[T])}
}

// do runs fn for key, ensuring that concurrent calls for the same key result in
// at most one execution of fn. Callers that join an in-flight call block until
// it finishes and then receive its shared result. The bool reports whether this
// caller was the one that actually executed fn (the leader).
func (g *callGroup[T]) do(key string, fn func() (T, error)) (T, bool, error) {
	g.mu.Lock()
	if c, ok := g.calls[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, false, c.err
	}
	c := new(call[T])
	c.wg.Add(1)
	g.calls[key] = c
	g.mu.Unlock()

	c.val, c.err = fn()
	c.wg.Done()

	g.mu.Lock()
	// Only delete if still the current call for this key (it always is, since a
	// new call cannot be inserted while this one occupies the slot).
	if g.calls[key] == c {
		delete(g.calls, key)
	}
	g.mu.Unlock()

	return c.val, true, c.err
}

// Cached wraps a primary lookup and serves last-known-good values on failure.
//
// It records successful results per key with a timestamp and, depending on the
// configured CacheMode, either always calls the primary (falling back to the
// cached value on error) or serves fresh values directly from the cache. It
// single-flights concurrent refreshes of the same key so a burst of callers
// triggers at most one primary invocation.
//
// Cached is safe for concurrent use by multiple goroutines.
type Cached[T any] struct {
	cfg   CacheConfig
	clk   clock.Clock
	group *callGroup[T]

	mu    sync.Mutex
	store map[string]entry[T]
}

// NewCached creates a Cached with the given configuration. It uses a real clock
// by default; pass WithClock to override for deterministic tests.
func NewCached[T any](cfg CacheConfig) *Cached[T] {
	return &Cached[T]{
		cfg:   cfg,
		clk:   clock.RealClock{},
		group: newCallGroup[T](),
		store: make(map[string]entry[T]),
	}
}

// WithClock overrides the clock used for freshness/staleness accounting. It
// returns the receiver for chaining and is intended for tests.
func (c *Cached[T]) WithClock(clk clock.Clock) *Cached[T] {
	c.clk = clk
	return c
}

// Do resolves key using primary, applying the configured caching semantics.
//
// Return semantics:
//
//   - (value, nil):        a fresh value (from primary success, or served from
//     cache while fresh in ModeServeFreshFromCache).
//   - (value, ErrStale):   the primary failed but a stale cached value within
//     StaleWhileError was served. Check errors.Is(err, ErrStale).
//   - (zero, primaryErr):  the primary failed and no servable cached value
//     existed. The original primary error is returned.
//
// Concurrent Do calls for the same key are single-flighted: at most one primary
// invocation runs; the others share its result.
func (c *Cached[T]) Do(ctx context.Context, key string, primary func(context.Context) (T, error)) (T, error) {
	// Fast path for ModeServeFreshFromCache: serve without single-flight if the
	// cached value is still fresh.
	if c.cfg.Mode == ModeServeFreshFromCache {
		if v, ok := c.freshValue(key); ok {
			return v, nil
		}
	}

	val, _, err := c.group.do(key, func() (T, error) {
		// Re-check freshness inside the single-flight leader: a value may have
		// been refreshed by a call that completed while we were queuing.
		if c.cfg.Mode == ModeServeFreshFromCache {
			if v, ok := c.freshValue(key); ok {
				return v, nil
			}
		}

		v, perr := primary(ctx)
		if perr == nil {
			c.storeValue(key, v)
			return v, nil
		}

		// Primary failed: try to serve a stale value.
		if sv, ok := c.staleValue(key); ok {
			return sv, ErrStale
		}
		var zero T
		return zero, perr
	})

	return val, err
}

// freshValue returns the cached value for key if it exists and is within TTL.
func (c *Cached[T]) freshValue(key string) (T, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.store[key]
	if !ok {
		var zero T
		return zero, false
	}
	if c.cfg.TTL <= 0 {
		var zero T
		return zero, false
	}
	if c.clk.Since(e.storedAt) < c.cfg.TTL {
		return e.value, true
	}
	var zero T
	return zero, false
}

// staleValue returns the cached value for key if it exists and is within the
// StaleWhileError window (measured from when it was stored).
func (c *Cached[T]) staleValue(key string) (T, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.store[key]
	if !ok {
		var zero T
		return zero, false
	}
	if c.cfg.StaleWhileError <= 0 {
		var zero T
		return zero, false
	}
	if c.clk.Since(e.storedAt) <= c.cfg.StaleWhileError {
		return e.value, true
	}
	var zero T
	return zero, false
}

// storeValue records value for key with a fresh timestamp, evicting the oldest
// entry if MaxEntries would be exceeded.
func (c *Cached[T]) storeValue(key string, value T) {
	c.mu.Lock()
	defer c.mu.Unlock()

	_, existing := c.store[key]
	c.store[key] = entry[T]{value: value, storedAt: c.clk.Now()}

	if !existing && c.cfg.MaxEntries > 0 && len(c.store) > c.cfg.MaxEntries {
		c.evictOldestLocked(key)
	}
}

// evictOldestLocked removes the least-recently-stored entry other than keep.
// Must hold c.mu.
func (c *Cached[T]) evictOldestLocked(keep string) {
	var oldestKey string
	var oldestAt time.Time
	first := true
	for k, e := range c.store {
		if k == keep {
			continue
		}
		if first || e.storedAt.Before(oldestAt) {
			oldestKey = k
			oldestAt = e.storedAt
			first = false
		}
	}
	if !first {
		delete(c.store, oldestKey)
	}
}
