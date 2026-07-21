// Package ratelimit provides rate limiting algorithms for Go applications.
// All implementations are safe for concurrent use. All time operations are
// performed through a Clock interface (see internal/clock) enabling
// deterministic testing without time.Sleep.
//
// Quick start:
//
//	limiter := tokenbucket.New(100, 10, tokenbucket.WithIdleCleanup(5*time.Minute))
//	result := limiter.Allow(ctx, "user:123")
//	if !result.Allowed {
//	    http.Error(w, "rate limited", http.StatusTooManyRequests)
//	    return
//	}
package ratelimit

import (
	"context"
	"time"
)

// Limiter is the core interface all rate limiting algorithms implement.
// All methods are safe for concurrent use.
type Limiter interface {
	// Allow checks if a single token is available. Non-blocking.
	Allow(ctx context.Context, key string) Result

	// AllowN checks if n tokens are available. Non-blocking.
	// Either all n tokens are consumed or none. Never partial.
	AllowN(ctx context.Context, key string, n int) Result

	// Wait blocks until a token is available or ctx is cancelled.
	Wait(ctx context.Context, key string) error

	// WaitN blocks until n tokens are available or ctx is cancelled.
	WaitN(ctx context.Context, key string, n int) error

	// Peek returns current state without consuming a token.
	Peek(ctx context.Context, key string) State

	// Reset resets all state for a given key.
	Reset(ctx context.Context, key string) error

	// Close releases all resources.
	Close() error
}

// PriorityLimiter is an optional extension of Limiter for algorithms that
// support per-request priority scheduling. Higher priority values are served
// before lower ones under contention on the same key; within a single priority
// level requests are served in FIFO order.
//
// Callers that need priority-aware methods should accept PriorityLimiter
// rather than type-asserting to a concrete type.
type PriorityLimiter interface {
	Limiter
	// AllowP is the priority-aware form of Allow.
	AllowP(ctx context.Context, key string, priority int) Result
	// WaitP is the priority-aware form of Wait.
	WaitP(ctx context.Context, key string, priority int) error
}

// Result is returned by Allow and AllowN.
type Result struct {
	// Allowed indicates whether the request was permitted.
	Allowed bool

	// Limit is the configured limit for this key.
	Limit int

	// Remaining is the number of tokens/requests remaining after this call.
	// Always >= 0. For algorithms with fractional internal state (e.g. the token
	// bucket), Remaining is floored to a whole number, so it can under-report by
	// up to one relative to the exact internal count; it is a conservative
	// observability value and never the basis of the allow/deny decision.
	Remaining int

	// ResetAfter is the duration until the window/bucket fully resets.
	ResetAfter time.Duration

	// RetryAfter is the minimum wait before this key will be allowed again.
	// Zero when Allowed is true.
	RetryAfter time.Duration

	// Algorithm identifies which algorithm produced this result.
	Algorithm string

	// Metadata contains algorithm-specific observability data.
	Metadata Metadata
}

// Metadata is a map of algorithm-specific observability data.
type Metadata map[string]any

// State is returned by Peek. It represents current limiter state without side effects.
type State struct {
	// Key is the rate limit key.
	Key string

	// Algorithm identifies the algorithm.
	Algorithm string

	// Limit is the configured limit.
	Limit int

	// Remaining is the current number of available tokens/slots.
	Remaining int

	// ResetAt is when the window/bucket fully resets.
	ResetAt time.Time

	// WindowStart is the start of the current window.
	WindowStart time.Time

	// Extra contains algorithm-specific state data.
	Extra map[string]any
}
