package store

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// entry holds a stored value with optional expiration.
type entry struct {
	value     string
	expiresAt time.Time // zero = never expires
	mu        sync.Mutex
}

func (e *entry) isExpired(now time.Time) bool {
	return !e.expiresAt.IsZero() && now.After(e.expiresAt)
}

// ScriptHandler is a registered handler for a named in-memory script.
type ScriptHandler func(keys []string, args []any) (any, error)

// Memory is a thread-safe in-memory Store with TTL support.
// All methods are safe for concurrent use.
type Memory struct {
	entries sync.Map // string -> *entry
	scripts sync.Map // string -> ScriptHandler

	cleanupInterval time.Duration
	done            chan struct{}
	wg              sync.WaitGroup
	closed          atomic.Bool

	// maxKeys limits the number of keys to prevent memory exhaustion
	maxKeys  int
	keyCount atomic.Int64

	// useServerTime mirrors RedisOptions.UseServerTime for the in-memory store so
	// the memory-backed distributed tests can exercise the same server-time code
	// path. When true, ServerTimeMode() reports true and the script emulations,
	// on receiving use_server_time=1, substitute their OWN local clock as the
	// authoritative "server" clock (the memory store IS the server).
	useServerTime bool
}

// MemoryOption configures a Memory store.
type MemoryOption func(*Memory)

// WithCleanupInterval sets how often expired keys are evicted.
// Default is 30 seconds.
func WithCleanupInterval(d time.Duration) MemoryOption {
	return func(m *Memory) { m.cleanupInterval = d }
}

// WithMaxKeys sets the maximum number of keys allowed in the store.
// This prevents memory exhaustion under attack.
// Default is 0 (unlimited).
func WithMaxKeys(max int) MemoryOption {
	return func(m *Memory) { m.maxKeys = max }
}

// WithServerTime enables server-time mode on the in-memory store, mirroring
// RedisOptions.UseServerTime. It only affects ServerTimeMode() and the behaviour
// of the script emulations when they receive use_server_time=1 (they then use
// the store's own local clock as the authoritative server clock). Default off.
func WithServerTime(on bool) MemoryOption {
	return func(m *Memory) { m.useServerTime = on }
}

// ServerTimeMode reports whether server-time mode is enabled on this store. It
// exists so callers can treat a *Memory and a *Redis uniformly via the interface
// { ServerTimeMode() bool }.
func (m *Memory) ServerTimeMode() bool { return m.useServerTime }

// NewMemory creates a new in-memory Store.
// Background goroutine evicts expired keys at cleanupInterval.
func NewMemory(opts ...MemoryOption) *Memory {
	m := &Memory{
		cleanupInterval: 30 * time.Second,
		done:            make(chan struct{}),
	}
	for _, opt := range opts {
		opt(m)
	}
	m.wg.Add(1)
	go m.cleanup()
	return m
}

// RegisterScript registers a named script handler for use with Eval.
func (m *Memory) RegisterScript(name string, h ScriptHandler) {
	m.scripts.Store(name, h)
}

// removeEntry deletes key from the map and decrements the tracked key count if
// the deletion actually removed a present entry. This is the single removal path
// so that keyCount stays in sync across Del, lazy expiry, and GC (C-4). Passing
// the *entry we already loaded lets sync.Map's CompareAndDelete avoid racing
// with a concurrent writer that replaced the entry between our load and delete:
// we only decrement when we removed the exact entry we accounted for.
func (m *Memory) removeEntry(key string, e *entry) {
	var removed bool
	if e != nil {
		removed = m.entries.CompareAndDelete(key, e)
	} else {
		_, removed = m.entries.LoadAndDelete(key)
	}
	if removed && m.maxKeys > 0 {
		m.keyCount.Add(-1)
	}
}

// reserveSlot attempts to claim one key-count slot for a brand-new key. It
// returns nil on success, or an error if the store is already at maxKeys. When
// maxKeys is unlimited (0) it is a no-op. Callers must publish the entry only
// after reserving so a concurrent reader never observes an entry that is about
// to be rejected (M-14).
func (m *Memory) reserveSlot() error {
	if m.maxKeys <= 0 {
		return nil
	}
	if int(m.keyCount.Add(1)) > m.maxKeys {
		m.keyCount.Add(-1)
		return fmt.Errorf("store: max keys limit (%d) exceeded", m.maxKeys)
	}
	return nil
}

// Get returns the value for key, or ErrNotFound if absent or expired.
func (m *Memory) Get(_ context.Context, key string) (string, error) {
	v, ok := m.entries.Load(key)
	if !ok {
		return "", ErrNotFound
	}
	e := v.(*entry)
	e.mu.Lock()
	expired := e.isExpired(time.Now())
	value := e.value
	e.mu.Unlock()
	if expired {
		m.removeEntry(key, e)
		return "", ErrNotFound
	}
	return value, nil
}

// Set stores value for key with the given TTL. TTL=0 means no expiry.
func (m *Memory) Set(_ context.Context, key, value string, ttl time.Duration) error {
	e := &entry{value: value}
	if ttl > 0 {
		e.expiresAt = time.Now().Add(ttl)
	}

	if m.maxKeys <= 0 {
		m.entries.Store(key, e)
		return nil
	}

	// Reserve a slot before publishing so a rejected Set is never observable and
	// the count can never exceed maxKeys (M-14). If the key already exists we are
	// overwriting an already-counted slot, so release the reservation we took.
	if err := m.reserveSlot(); err != nil {
		return err
	}
	if _, loaded := m.entries.Swap(key, e); loaded {
		// Overwrote an existing entry — it was already counted, so give back the
		// slot we just reserved.
		m.keyCount.Add(-1)
	}
	return nil
}

// SetNX stores value only if key does not exist or has expired. Returns true if set.
func (m *Memory) SetNX(_ context.Context, key, value string, ttl time.Duration) (bool, error) {
	now := time.Now()
	newEntry := &entry{value: value}
	if ttl > 0 {
		newEntry.expiresAt = now.Add(ttl)
	}

	// Reserve the count slot BEFORE publishing the entry so a concurrent reader
	// can never observe an entry that is about to be rejected, and a racing Del
	// can never be "lost" against a delete-on-reject (M-14).
	reserved := false
	if m.maxKeys > 0 {
		if _, ok := m.entries.Load(key); !ok {
			if err := m.reserveSlot(); err != nil {
				return false, err
			}
			reserved = true
		}
	}

	// Use LoadOrStore for optimistic lock-free path
	actual, loaded := m.entries.LoadOrStore(key, newEntry)
	if !loaded {
		// Newly created. If we didn't reserve above (rare: key appeared absent to
		// Load but our reservation belonged to it), we already hold the slot.
		if m.maxKeys > 0 && !reserved {
			if err := m.reserveSlot(); err != nil {
				m.entries.Delete(key)
				return false, err
			}
		}
		return true, nil
	}
	// Key already existed, so our reservation (if any) was surplus — release it.
	if reserved {
		m.keyCount.Add(-1)
	}
	// Key exists — check if expired
	existing := actual.(*entry)
	existing.mu.Lock()
	defer existing.mu.Unlock()
	if existing.isExpired(now) {
		existing.value = value
		if ttl > 0 {
			existing.expiresAt = now.Add(ttl)
		} else {
			existing.expiresAt = time.Time{}
		}
		return true, nil
	}
	return false, nil
}

// GetSet atomically gets the current value and sets the new one.
func (m *Memory) GetSet(_ context.Context, key, value string, ttl time.Duration) (string, error) {
	now := time.Now()
	newEntry := &entry{value: value}
	if ttl > 0 {
		newEntry.expiresAt = now.Add(ttl)
	}
	// Reserve before publishing (M-14) — same rationale as SetNX.
	reserved := false
	if m.maxKeys > 0 {
		if _, ok := m.entries.Load(key); !ok {
			if err := m.reserveSlot(); err != nil {
				return "", err
			}
			reserved = true
		}
	}

	actual, loaded := m.entries.LoadOrStore(key, newEntry)
	if !loaded {
		if m.maxKeys > 0 && !reserved {
			if err := m.reserveSlot(); err != nil {
				m.entries.Delete(key)
				return "", err
			}
		}
		return "", nil
	}
	if reserved {
		m.keyCount.Add(-1)
	}
	existing := actual.(*entry)
	existing.mu.Lock()
	defer existing.mu.Unlock()
	old := existing.value
	if existing.isExpired(now) {
		old = ""
	}
	existing.value = value
	if ttl > 0 {
		existing.expiresAt = now.Add(ttl)
	} else {
		existing.expiresAt = time.Time{}
	}
	return old, nil
}

// IncrBy atomically increments the stored integer value by delta.
// If the key doesn't exist or has expired, it's created with value delta and the TTL is set.
func (m *Memory) IncrBy(_ context.Context, key string, delta int64, ttl time.Duration) (int64, error) {
	now := time.Now()

	// Reserve the count slot before publishing so a rejected create is never
	// observable and cannot be lost against a concurrent Del (M-14).
	reserved := false
	if m.maxKeys > 0 {
		if _, ok := m.entries.Load(key); !ok {
			if err := m.reserveSlot(); err != nil {
				return 0, err
			}
			reserved = true
		}
	}

	actual, loaded := m.entries.LoadOrStore(key, &entry{
		value:     strconv.FormatInt(delta, 10),
		expiresAt: ttlToTime(now, ttl),
	})
	if !loaded {
		if m.maxKeys > 0 && !reserved {
			if err := m.reserveSlot(); err != nil {
				m.entries.Delete(key)
				return 0, err
			}
		}
		return delta, nil
	}
	if reserved {
		m.keyCount.Add(-1)
	}
	e := actual.(*entry)
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.isExpired(now) {
		// Expired: reset in place (entry stays counted, so keyCount unchanged).
		e.value = strconv.FormatInt(delta, 10)
		e.expiresAt = ttlToTime(now, ttl)
		return delta, nil
	}
	current, err := strconv.ParseInt(e.value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("store: key %q value %q is not an integer", key, e.value)
	}
	// Detect signed 64-bit overflow before it wraps. Redis INCRBY errors on
	// overflow rather than wrapping a counter negative (which would let a rate
	// limiter suddenly admit everything) (M-13).
	if (delta > 0 && current > math.MaxInt64-delta) ||
		(delta < 0 && current < math.MinInt64-delta) {
		return 0, fmt.Errorf("store: key %q increment by %d would overflow int64", key, delta)
	}
	newVal := current + delta
	e.value = strconv.FormatInt(newVal, 10)
	// Only set TTL on creation; don't update it on subsequent increments.
	return newVal, nil
}

// Eval dispatches to a registered script handler by name.
func (m *Memory) Eval(_ context.Context, script string, keys []string, args ...any) (any, error) {
	h, ok := m.scripts.Load(script)
	if !ok {
		return nil, fmt.Errorf("store: no script handler registered for %q", script)
	}
	return h.(ScriptHandler)(keys, args)
}

// Del deletes one or more keys.
func (m *Memory) Del(_ context.Context, keys ...string) error {
	for _, k := range keys {
		// removeEntry(k, nil) uses LoadAndDelete so keyCount is only decremented
		// when a present entry was actually removed (C-4).
		m.removeEntry(k, nil)
	}
	return nil
}

// Ping always succeeds for the in-memory store.
func (m *Memory) Ping(_ context.Context) error { return nil }

// Close stops the cleanup goroutine and clears all stored data.
func (m *Memory) Close() error {
	if m.closed.Swap(true) {
		return nil
	}
	close(m.done)
	m.wg.Wait()
	return nil
}

// cleanup runs in the background, evicting expired entries.
func (m *Memory) cleanup() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.cleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.done:
			return
		case now := <-ticker.C:
			m.entries.Range(func(k, v any) bool {
				e := v.(*entry)
				e.mu.Lock()
				expired := e.isExpired(now)
				e.mu.Unlock()
				if expired {
					// Decrement keyCount on GC eviction (C-4). CompareAndDelete
					// ensures we only decrement if this exact entry is removed.
					m.removeEntry(k.(string), e)
				}
				return true
			})
		}
	}
}

func ttlToTime(now time.Time, ttl time.Duration) time.Time {
	if ttl <= 0 {
		return time.Time{}
	}
	return now.Add(ttl)
}
