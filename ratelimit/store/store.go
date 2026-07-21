// Package store provides the persistence interface for distributed rate limiters.
// All implementations must be safe for concurrent use.
//
// # Script identifiers
//
// Eval accepts a ScriptID to identify the script to run rather than an opaque
// string body. Use the package-level ScriptID constants (TokenBucketScriptID,
// GCRAScriptID, etc.) — do not construct ScriptID values directly.
//
// Return shapes per ScriptID:
//
//	TokenBucketScriptID           → []any{allowed int64, remaining int64, refilled int64}
//	FixedWindowScriptID           → []any{allowed int64, count int64}
//	GCRAScriptID                  → []any{allowed int64, retry_after_ns int64}
//	LeakyBucketScriptID           → []any{allowed int64, queue_depth int64, retry_after_ns int64}
//	SlidingWindowLogScriptID      → []any{allowed int64, count int64, retry_after_ns int64}
//	SlidingWindowCounterScriptID  → []any{allowed int64, new_current int64, estimated_scaled int64}
//	CircuitBreakerAcquireScriptID → []any{decision int64, state int64} or
//	                                []any{decision int64, state int64, opened_at_ns int64}
//	                                when decision==1 (reject-open), opened_at_ns is the
//	                                nanosecond timestamp when the circuit last opened.
//	CircuitBreakerRecordScriptID  → []any{state int64}
//	CircuitBreakerReadScriptID    → []any{state int64, failures int64, successes int64,
//	                                opened_at_ns int64, half_open_inflight int64}
//
// For all rate-limit scripts: allowed==1 means the request is permitted,
// allowed==0 means denied. retry_after_ns is the wait before retry in
// nanoseconds; remaining/queue_depth are token/slot counts.
package store

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound is returned by Get when a key does not exist or has expired.
var ErrNotFound = errors.New("key not found")

// ScriptID identifies a registered script. Use the package-level constants
// (TokenBucketScriptID, GCRAScriptID, etc.) for the built-in scripts, or
// NewScriptID to create identifiers for custom scripts. The zero value is not
// a valid script identifier.
type ScriptID struct{ name string }

// String returns the internal script name, useful for logging/debugging.
func (s ScriptID) String() string { return s.name }

// NewScriptID creates a ScriptID for a custom script name. Use this when
// registering and calling scripts beyond the built-in set (e.g., in tests or
// for application-specific scripts registered via Memory.RegisterScript).
// For the standard built-in scripts, use the package-level ScriptID vars.
func NewScriptID(name string) ScriptID { return ScriptID{name: name} }

// ServerTimeable is implemented by stores that can report whether server-time
// mode is enabled. Distributed limiters perform a type assertion against this
// interface to inherit the store's server-time setting automatically. Third-party
// Store implementations that want to participate in server-time mode must
// implement this interface.
type ServerTimeable interface {
	ServerTimeMode() bool
}

// Store is the persistence abstraction for distributed rate limiters.
// All methods must be safe for concurrent use.
type Store interface {
	// Get returns the value for key. Returns ("", ErrNotFound) if absent or expired.
	Get(ctx context.Context, key string) (string, error)

	// Set stores value for key with TTL. Overwrites any existing value.
	Set(ctx context.Context, key string, value string, ttl time.Duration) error

	// SetNX stores value only if key does not exist. Returns true if the key was set.
	SetNX(ctx context.Context, key string, value string, ttl time.Duration) (bool, error)

	// GetSet atomically gets the current value and sets the new value.
	// Returns the old value (or "" if key was absent).
	GetSet(ctx context.Context, key string, value string, ttl time.Duration) (string, error)

	// IncrBy atomically increments the integer value of key by delta.
	// Creates the key with value delta if it does not exist.
	// Sets TTL only on creation (when previous value was 0 or key absent).
	IncrBy(ctx context.Context, key string, delta int64, ttl time.Duration) (int64, error)

	// Eval executes a named script atomically.
	//
	// The scriptID parameter must be one of the package-level ScriptID constants.
	// For the in-memory store, handlers are registered by ScriptID via
	// RegisterScript. For Redis, the ScriptID is mapped to a Lua script body
	// internally. See the package doc for the concrete return shape of each
	// ScriptID.
	Eval(ctx context.Context, scriptID ScriptID, keys []string, args ...any) (any, error)

	// Del deletes one or more keys.
	Del(ctx context.Context, keys ...string) error

	// Ping checks store connectivity.
	Ping(ctx context.Context) error

	// Close releases all resources held by the store.
	Close() error
}
