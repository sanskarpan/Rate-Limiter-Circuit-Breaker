package ratelimit

import (
	"errors"
	"fmt"
	"time"
)

// Sentinel errors for the ratelimit package.
var (
	// ErrLimitExceeded is returned when a rate limit is exceeded.
	ErrLimitExceeded = errors.New("rate limit exceeded")

	// ErrInvalidKey is returned when a key is empty, too long, or contains illegal characters.
	ErrInvalidKey = errors.New("invalid key: empty or contains illegal characters")

	// ErrInvalidN is returned when n < 1 is passed to AllowN or WaitN.
	ErrInvalidN = errors.New("n must be >= 1")

	// ErrClosed is returned when a method is called on a closed limiter.
	ErrClosed = errors.New("limiter is closed")

	// ErrContextDone is returned when the context is cancelled while waiting for a token.
	ErrContextDone = errors.New("context cancelled while waiting for token")
)

// RateLimitError is a rich error type returned when a rate limit is exceeded.
// Users can inspect it with errors.As to extract RetryAfter, Algorithm, etc.
type RateLimitError struct {
	// Algorithm identifies which limiter produced this error.
	Algorithm string

	// Key is the rate limited key.
	Key string

	// Limit is the configured limit.
	Limit int

	// RetryAfter is how long the caller should wait before retrying.
	RetryAfter time.Duration

	// Err is the underlying error (wraps ErrLimitExceeded).
	Err error
}

// Error implements the error interface.
func (e *RateLimitError) Error() string {
	return fmt.Sprintf("rate limit exceeded for key %q (algorithm=%s, limit=%d, retry_after=%s): %v",
		e.Key, e.Algorithm, e.Limit, e.RetryAfter, e.Err)
}

// Unwrap returns the wrapped error, enabling errors.Is and errors.As chaining.
func (e *RateLimitError) Unwrap() error { return e.Err }

// Is reports whether target matches ErrLimitExceeded, enabling errors.Is to work
// regardless of wrapping depth.
func (e *RateLimitError) Is(target error) bool {
	return target == ErrLimitExceeded
}

// ValidateKey checks that a key is valid for use with a rate limiter.
// Returns ErrInvalidKey if the key is empty, too long (>512 bytes), or contains
// null bytes or CR/LF characters (which could cause HTTP header injection).
func ValidateKey(key string) error {
	if len(key) == 0 {
		return fmt.Errorf("%w: key is empty", ErrInvalidKey)
	}
	if len(key) > 512 {
		return fmt.Errorf("%w: key exceeds 512 bytes (got %d)", ErrInvalidKey, len(key))
	}
	for _, c := range key {
		if c == '\x00' || c == '\r' || c == '\n' {
			return fmt.Errorf("%w: key contains illegal character (0x%02x)", ErrInvalidKey, c)
		}
	}
	return nil
}

// ValidateN checks that n >= 1 for AllowN/WaitN calls.
func ValidateN(n int) error {
	if n < 1 {
		return fmt.Errorf("%w: got %d", ErrInvalidN, n)
	}
	return nil
}
