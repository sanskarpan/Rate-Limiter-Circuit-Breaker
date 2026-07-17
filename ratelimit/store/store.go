// Package store provides the persistence interface for distributed rate limiters.
// All implementations must be safe for concurrent use.
package store

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound is returned by Get when a key does not exist or has expired.
var ErrNotFound = errors.New("key not found")

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
	// For the in-memory store, scripts are registered by name.
	// For Redis, this executes a Lua script.
	Eval(ctx context.Context, script string, keys []string, args ...any) (any, error)

	// Del deletes one or more keys.
	Del(ctx context.Context, keys ...string) error

	// Ping checks store connectivity.
	Ping(ctx context.Context) error

	// Close releases all resources held by the store.
	Close() error
}
