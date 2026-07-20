// Package memcached provides a Memcached-backed implementation of the core
// store.Store interface (github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/store).
//
// Memcached is an OPTIONAL, heavy backend and therefore lives in this separate
// nested Go module (stores/) so the CORE library stays zero-dependency, exactly
// like the framework middleware in contrib/. Import it only when you need
// distributed rate limiting backed by a Memcached cluster rather than Redis.
//
// # Atomicity model
//
// Memcached has NO server-side scripting (no Lua, no MULTI/EXEC transactions).
// The only primitives available for atomic read-modify-write are:
//
//   - Incr/Decr         — atomic integer counters (used for IncrBy)
//   - Add               — set-if-absent (used to seed counters/CAS keys)
//   - Gets/CompareAndSwap — optimistic concurrency via a per-item CAS token
//
// Every "script" that Redis runs as a single atomic Lua body is re-expressed
// here as a Gets → compute → CompareAndSwap retry loop (see eval.go). The loop
// re-reads and recomputes on a CAS mismatch, so each individual key mutation is
// linearizable: two concurrent callers cannot both commit against the same base
// version — the loser retries against the winner's value.
//
// # HONEST comparison vs Redis Lua
//
// This is a genuinely WEAKER guarantee than Redis Lua for two reasons:
//
//  1. Single-key only. A CAS loop is atomic per key. Any algorithm that must
//     read/mutate MULTIPLE keys atomically (e.g. the sliding-window-counter,
//     which reads current+previous windows, or the distributed circuit breaker,
//     which the Redis backend keeps in one hash) CANNOT be made safe on
//     Memcached — there is no cross-key transaction. Those scripts are
//     deliberately unsupported here (Eval returns ErrScriptUnsupported); use
//     Redis for them. Token bucket, GCRA, leaky bucket, fixed window, and
//     sliding-window-log all keep their entire state in ONE key, so they DO port.
//
//  2. No blocking / fairness. Under heavy contention on a single hot key the CAS
//     loop can retry many times; it is bounded by maxCASRetries and fails closed
//     (deny) if it cannot commit, whereas Redis serializes on the server with no
//     client retry. This trades a small amount of goodput under extreme
//     single-key contention for availability, and can very rarely deny a request
//     that Redis would have admitted.
//
// Memcached also silently EVICTS keys under memory pressure (LRU) independent of
// TTL. A rate-limit counter can therefore vanish early, momentarily resetting a
// limiter to "empty" (fail-open for that key). Size the slab/limit for your key
// working set, and prefer Redis when strict enforcement across an eviction event
// matters.
package memcached

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/bradfitz/gomemcache/memcache"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/store"
)

// Compile-time assertion that *Memcached satisfies the core Store interface.
var _ store.Store = (*Memcached)(nil)

// maxCASRetries bounds the optimistic Gets/CompareAndSwap retry loop used by the
// client-side "scripts". On exhaustion the operation fails (fail-closed / deny)
// rather than committing a stale computation.
const maxCASRetries = 32

// Options configures the Memcached store.
type Options struct {
	// Servers is the list of "host:port" memcached addresses (required unless a
	// pre-built client is supplied via NewFromClient).
	Servers []string

	// Timeout is the socket read/write timeout for each operation
	// (default: 100ms, matching gomemcache's default).
	Timeout time.Duration

	// MaxIdleConns is the maximum number of idle connections per address
	// (default: gomemcache's default of 2).
	MaxIdleConns int

	// KeyPrefix is prepended to every key (default: "rl:"). Memcached keys are
	// limited to 250 bytes and may not contain whitespace or control chars; the
	// caller is responsible for keeping (prefix + key) within that bound.
	KeyPrefix string

	// Fallback is the store used when memcached is unreachable. If nil, a fresh
	// per-process in-memory store is installed.
	//
	// FAIL-OPEN WARNING: with the default per-process fallback, a memcached
	// outage makes each application instance rate-limit against its own local
	// counters, multiplying the effective global limit by the instance count.
	// Supply an explicit fail-closed store if that trade-off is unacceptable.
	Fallback store.Store
}

func (o *Options) defaults() {
	if o.Timeout == 0 {
		o.Timeout = 100 * time.Millisecond
	}
	if o.MaxIdleConns == 0 {
		o.MaxIdleConns = 2
	}
	if o.KeyPrefix == "" {
		o.KeyPrefix = "rl:"
	}
	if o.Fallback == nil {
		o.Fallback = store.NewMemoryWithScripts()
	}
}

// client is the minimal subset of *memcache.Client this store depends on. It is
// an interface so tests can inject an in-process fake without a live server.
type client interface {
	Get(key string) (*memcache.Item, error)
	Set(item *memcache.Item) error
	Add(item *memcache.Item) error
	CompareAndSwap(item *memcache.Item) error
	Increment(key string, delta uint64) (uint64, error)
	Delete(key string) error
	Ping() error
}

// Memcached is a Store implementation backed by a Memcached cluster.
//
// When memcached is unavailable, every method transparently routes to the
// configured fallback store (see Options.Fallback). All methods are safe for
// concurrent use.
type Memcached struct {
	mc   client
	opts Options
}

// New creates a Memcached store connecting to opts.Servers.
func New(opts Options) *Memcached {
	opts.defaults()
	mc := memcache.New(opts.Servers...)
	mc.Timeout = opts.Timeout
	mc.MaxIdleConns = opts.MaxIdleConns
	return &Memcached{mc: mc, opts: opts}
}

// NewFromClient creates a Memcached store from a pre-configured *memcache.Client,
// letting the caller own connection tuning.
func NewFromClient(mc *memcache.Client, opts Options) *Memcached {
	opts.defaults()
	return &Memcached{mc: mc, opts: opts}
}

// newFromRawClient is used by tests to inject a fake client.
func newFromRawClient(c client, opts Options) *Memcached {
	opts.defaults()
	return &Memcached{mc: c, opts: opts}
}

func (m *Memcached) prefixed(key string) string {
	return m.opts.KeyPrefix + key
}

// ttlToSeconds converts a Go TTL to the memcached expiration field. Memcached
// treats an exptime of 0 as "never expire", and a value greater than 30 days as
// a Unix timestamp; we clamp sub-second positive TTLs up to 1s so a short window
// is never accidentally interpreted as "no expiry".
func ttlToSeconds(ttl time.Duration) int32 {
	if ttl <= 0 {
		return 0
	}
	secs := int64(ttl / time.Second)
	if ttl%time.Second != 0 {
		secs++
	}
	if secs < 1 {
		secs = 1
	}
	// Memcached's 30-day boundary: anything larger is read as an absolute Unix
	// time. Convert to an absolute timestamp to preserve the intended duration.
	const thirtyDays = int64(60 * 60 * 24 * 30)
	if secs > thirtyDays {
		return int32(time.Now().Unix() + secs)
	}
	return int32(secs)
}

// isConnError reports whether err indicates memcached is unreachable (as opposed
// to an application-level miss). gomemcache surfaces connectivity failures as
// memcache.ErrServerError, memcache.ErrNoServers, or wrapped net errors; a cache
// MISS (ErrCacheMiss) and a CAS conflict (ErrCASConflict / ErrNotStored) are NOT
// connection errors.
func isConnError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, memcache.ErrCacheMiss) ||
		errors.Is(err, memcache.ErrCASConflict) ||
		errors.Is(err, memcache.ErrNotStored) {
		return false
	}
	if errors.Is(err, memcache.ErrNoServers) || errors.Is(err, memcache.ErrServerError) {
		return true
	}
	// Timeouts and dial errors implement net.Error / Timeout().
	var timeout interface{ Timeout() bool }
	if errors.As(err, &timeout) {
		return true
	}
	// Any other non-protocol error (e.g. "connect: connection refused") is
	// treated as a connectivity failure so the fallback engages.
	return !errors.Is(err, memcache.ErrMalformedKey)
}

// Get returns the value for key, or store.ErrNotFound if absent or expired.
func (m *Memcached) Get(ctx context.Context, key string) (string, error) {
	item, err := m.mc.Get(m.prefixed(key))
	if err == nil {
		return string(item.Value), nil
	}
	if errors.Is(err, memcache.ErrCacheMiss) {
		return "", store.ErrNotFound
	}
	if isConnError(err) {
		return m.opts.Fallback.Get(ctx, key)
	}
	return "", fmt.Errorf("memcached get %q: %w", key, err)
}

// Set stores value for key with the given TTL. TTL <= 0 means no expiry.
func (m *Memcached) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	err := m.mc.Set(&memcache.Item{
		Key:        m.prefixed(key),
		Value:      []byte(value),
		Expiration: ttlToSeconds(ttl),
	})
	if err == nil {
		return nil
	}
	if isConnError(err) {
		return m.opts.Fallback.Set(ctx, key, value, ttl)
	}
	return fmt.Errorf("memcached set %q: %w", key, err)
}

// SetNX stores value only if key does not exist. Returns true if the key was set.
// Implemented with memcached's Add (store-if-absent), which is atomic.
func (m *Memcached) SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	err := m.mc.Add(&memcache.Item{
		Key:        m.prefixed(key),
		Value:      []byte(value),
		Expiration: ttlToSeconds(ttl),
	})
	if err == nil {
		return true, nil
	}
	if errors.Is(err, memcache.ErrNotStored) {
		// Key already exists.
		return false, nil
	}
	if isConnError(err) {
		return m.opts.Fallback.SetNX(ctx, key, value, ttl)
	}
	return false, fmt.Errorf("memcached setnx %q: %w", key, err)
}

// GetSet atomically reads the current value and writes the new one, returning the
// old value ("" if the key was absent). Atomicity is provided by a Gets/CAS loop.
func (m *Memcached) GetSet(ctx context.Context, key, value string, ttl time.Duration) (string, error) {
	pk := m.prefixed(key)
	exp := ttlToSeconds(ttl)
	for i := 0; i < maxCASRetries; i++ {
		item, err := m.mc.Get(pk)
		if errors.Is(err, memcache.ErrCacheMiss) {
			// Absent: Add atomically; if someone raced us, retry the loop.
			addErr := m.mc.Add(&memcache.Item{Key: pk, Value: []byte(value), Expiration: exp})
			if addErr == nil {
				return "", nil
			}
			if errors.Is(addErr, memcache.ErrNotStored) {
				continue // lost the race; a value now exists, retry as CAS
			}
			if isConnError(addErr) {
				return m.opts.Fallback.GetSet(ctx, key, value, ttl)
			}
			return "", fmt.Errorf("memcached getset add %q: %w", key, addErr)
		}
		if err != nil {
			if isConnError(err) {
				return m.opts.Fallback.GetSet(ctx, key, value, ttl)
			}
			return "", fmt.Errorf("memcached getset get %q: %w", key, err)
		}
		old := string(item.Value)
		item.Value = []byte(value)
		item.Expiration = exp
		casErr := m.mc.CompareAndSwap(item)
		if casErr == nil {
			return old, nil
		}
		if errors.Is(casErr, memcache.ErrCASConflict) || errors.Is(casErr, memcache.ErrNotStored) {
			continue // concurrent write; retry
		}
		if isConnError(casErr) {
			return m.opts.Fallback.GetSet(ctx, key, value, ttl)
		}
		return "", fmt.Errorf("memcached getset cas %q: %w", key, casErr)
	}
	return "", fmt.Errorf("memcached getset %q: %w", key, ErrCASExhausted)
}

// IncrBy atomically increments the integer value of key by delta.
//
// Memcached's native Incr/Decr operate on unsigned 64-bit counters and CANNOT
// create a key — so the key is seeded with Add (atomic, store-if-absent) on the
// first increment, applying the TTL only then (honoring the Store contract that
// TTL is set only on creation). Native Decr also CLAMPS at zero rather than going
// negative, so negative deltas are handled via a CAS loop to preserve signed
// arithmetic parity with the Memory/Redis stores.
func (m *Memcached) IncrBy(ctx context.Context, key string, delta int64, ttl time.Duration) (int64, error) {
	pk := m.prefixed(key)
	exp := ttlToSeconds(ttl)

	if delta < 0 {
		// Signed decrement: memcached Decr clamps at 0, so use a CAS loop.
		return m.incrByCAS(ctx, key, pk, delta, exp)
	}

	// Try to seed the key atomically; Add succeeds only if the key is absent.
	addErr := m.mc.Add(&memcache.Item{Key: pk, Value: []byte(strconv.FormatInt(delta, 10)), Expiration: exp})
	if addErr == nil {
		return delta, nil
	}
	if isConnError(addErr) {
		return m.opts.Fallback.IncrBy(ctx, key, delta, ttl)
	}
	if !errors.Is(addErr, memcache.ErrNotStored) {
		return 0, fmt.Errorf("memcached incrby add %q: %w", key, addErr)
	}
	// Key exists — do an atomic native increment. TTL is intentionally NOT
	// refreshed here (set only on creation).
	newVal, err := m.mc.Increment(pk, uint64(delta))
	if err == nil {
		return int64(newVal), nil
	}
	if errors.Is(err, memcache.ErrCacheMiss) {
		// Key expired between Add and Increment; recurse to re-seed.
		return m.IncrBy(ctx, key, delta, ttl)
	}
	if isConnError(err) {
		return m.opts.Fallback.IncrBy(ctx, key, delta, ttl)
	}
	return 0, fmt.Errorf("memcached incrby %q: %w", key, err)
}

// incrByCAS handles signed (including negative) increments via a Gets/CAS loop.
func (m *Memcached) incrByCAS(ctx context.Context, key, pk string, delta int64, exp int32) (int64, error) {
	for i := 0; i < maxCASRetries; i++ {
		item, err := m.mc.Get(pk)
		if errors.Is(err, memcache.ErrCacheMiss) {
			addErr := m.mc.Add(&memcache.Item{Key: pk, Value: []byte(strconv.FormatInt(delta, 10)), Expiration: exp})
			if addErr == nil {
				return delta, nil
			}
			if errors.Is(addErr, memcache.ErrNotStored) {
				continue
			}
			if isConnError(addErr) {
				return m.opts.Fallback.IncrBy(ctx, key, delta, ttlFromExp(exp))
			}
			return 0, fmt.Errorf("memcached incrby add %q: %w", key, addErr)
		}
		if err != nil {
			if isConnError(err) {
				return m.opts.Fallback.IncrBy(ctx, key, delta, ttlFromExp(exp))
			}
			return 0, fmt.Errorf("memcached incrby get %q: %w", key, err)
		}
		current, perr := strconv.ParseInt(string(item.Value), 10, 64)
		if perr != nil {
			return 0, fmt.Errorf("memcached incrby %q: value %q not an integer", key, item.Value)
		}
		newVal := current + delta
		item.Value = []byte(strconv.FormatInt(newVal, 10))
		// Preserve existing expiry on increment (TTL set only on creation).
		item.Expiration = 0
		casErr := m.mc.CompareAndSwap(item)
		if casErr == nil {
			return newVal, nil
		}
		if errors.Is(casErr, memcache.ErrCASConflict) || errors.Is(casErr, memcache.ErrNotStored) {
			continue
		}
		if isConnError(casErr) {
			return m.opts.Fallback.IncrBy(ctx, key, delta, ttlFromExp(exp))
		}
		return 0, fmt.Errorf("memcached incrby cas %q: %w", key, casErr)
	}
	return 0, fmt.Errorf("memcached incrby %q: %w", key, ErrCASExhausted)
}

// ttlFromExp converts a memcached exptime seconds value back to a Duration for
// the fallback path (best-effort; absolute timestamps are treated as 0).
func ttlFromExp(exp int32) time.Duration {
	if exp <= 0 {
		return 0
	}
	const thirtyDays = 60 * 60 * 24 * 30
	if int64(exp) > thirtyDays {
		return 0
	}
	return time.Duration(exp) * time.Second
}

// Del deletes one or more keys.
func (m *Memcached) Del(ctx context.Context, keys ...string) error {
	for _, k := range keys {
		err := m.mc.Delete(m.prefixed(k))
		if err == nil || errors.Is(err, memcache.ErrCacheMiss) {
			continue
		}
		if isConnError(err) {
			return m.opts.Fallback.Del(ctx, keys...)
		}
		return fmt.Errorf("memcached del %q: %w", k, err)
	}
	return nil
}

// Ping checks memcached connectivity.
func (m *Memcached) Ping(_ context.Context) error {
	return m.mc.Ping()
}

// Close releases resources held by the store and its fallback. gomemcache holds
// no explicit close handle, so only the fallback is closed.
func (m *Memcached) Close() error {
	return m.opts.Fallback.Close()
}
