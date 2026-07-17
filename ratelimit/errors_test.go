package ratelimit_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/sanskarpan/resilience/ratelimit"
)

func TestErrors_Is_DirectMatch(t *testing.T) {
	err := &ratelimit.RateLimitError{
		Algorithm:  "token_bucket",
		Key:        "user:123",
		Limit:      100,
		RetryAfter: time.Second,
		Err:        ratelimit.ErrLimitExceeded,
	}
	if !errors.Is(err, ratelimit.ErrLimitExceeded) {
		t.Fatal("errors.Is should match ErrLimitExceeded directly")
	}
}

func TestErrors_Is_ThreeLevelsWrapped(t *testing.T) {
	base := &ratelimit.RateLimitError{
		Algorithm:  "gcra",
		Key:        "api:key",
		Limit:      10,
		RetryAfter: 500 * time.Millisecond,
		Err:        ratelimit.ErrLimitExceeded,
	}
	// Wrap 3 levels deep
	wrapped := fmt.Errorf("level3: %w", fmt.Errorf("level2: %w", fmt.Errorf("level1: %w", base)))
	if !errors.Is(wrapped, ratelimit.ErrLimitExceeded) {
		t.Fatal("errors.Is should match ErrLimitExceeded through 3 levels of wrapping")
	}
}

func TestErrors_As_ExtractsRetryAfter(t *testing.T) {
	expected := 1500 * time.Millisecond
	err := &ratelimit.RateLimitError{
		Algorithm:  "sliding_window_log",
		Key:        "user:456",
		Limit:      50,
		RetryAfter: expected,
		Err:        ratelimit.ErrLimitExceeded,
	}
	wrapped := fmt.Errorf("outer: %w", err)

	var rateLimitErr *ratelimit.RateLimitError
	if !errors.As(wrapped, &rateLimitErr) {
		t.Fatal("errors.As should extract RateLimitError")
	}
	if rateLimitErr.RetryAfter != expected {
		t.Fatalf("expected RetryAfter %v, got %v", expected, rateLimitErr.RetryAfter)
	}
	if rateLimitErr.Key != "user:456" {
		t.Fatalf("expected key 'user:456', got %q", rateLimitErr.Key)
	}
}

func TestErrors_As_ExtractsAlgorithm(t *testing.T) {
	err := &ratelimit.RateLimitError{
		Algorithm: "fixed_window",
		Key:       "ip:1.2.3.4",
		Limit:     100,
		Err:       ratelimit.ErrLimitExceeded,
	}
	var rlErr *ratelimit.RateLimitError
	if !errors.As(err, &rlErr) {
		t.Fatal("errors.As should work on RateLimitError directly")
	}
	if rlErr.Algorithm != "fixed_window" {
		t.Fatalf("expected algorithm 'fixed_window', got %q", rlErr.Algorithm)
	}
}

func TestErrors_ValidateKey_Empty(t *testing.T) {
	err := ratelimit.ValidateKey("")
	if err == nil {
		t.Fatal("empty key should return error")
	}
	if !errors.Is(err, ratelimit.ErrInvalidKey) {
		t.Fatalf("expected ErrInvalidKey, got: %v", err)
	}
}

func TestErrors_ValidateKey_TooLong(t *testing.T) {
	key := make([]byte, 513)
	for i := range key {
		key[i] = 'a'
	}
	err := ratelimit.ValidateKey(string(key))
	if err == nil {
		t.Fatal("513-byte key should return error")
	}
	if !errors.Is(err, ratelimit.ErrInvalidKey) {
		t.Fatalf("expected ErrInvalidKey, got: %v", err)
	}
}

func TestErrors_ValidateKey_NullByte(t *testing.T) {
	err := ratelimit.ValidateKey("user\x00123")
	if err == nil {
		t.Fatal("key with null byte should return error")
	}
}

func TestErrors_ValidateKey_CRLF(t *testing.T) {
	for _, key := range []string{"user\r123", "user\n123", "user\r\n123"} {
		err := ratelimit.ValidateKey(key)
		if err == nil {
			t.Fatalf("key %q with CR/LF should return error", key)
		}
	}
}

func TestErrors_ValidateKey_Valid(t *testing.T) {
	// Build a 512-byte key with only valid characters
	longKey := make([]byte, 512)
	for i := range longKey {
		longKey[i] = 'a'
	}
	validKeys := []string{
		"user:123",
		"api-key-abc",
		"ip:192.168.1.1",
		"a",
		"user@example.com",
		string(longKey), // exactly 512 bytes, all valid
	}
	for _, key := range validKeys {
		if err := ratelimit.ValidateKey(key); err != nil {
			t.Errorf("valid key %q should not return error: %v", key, err)
		}
	}
}

func TestErrors_ValidateN_Valid(t *testing.T) {
	if err := ratelimit.ValidateN(1); err != nil {
		t.Fatal("n=1 should be valid")
	}
	if err := ratelimit.ValidateN(1000); err != nil {
		t.Fatal("n=1000 should be valid")
	}
}

func TestErrors_ValidateN_Invalid(t *testing.T) {
	for _, n := range []int{0, -1, -100} {
		err := ratelimit.ValidateN(n)
		if err == nil {
			t.Fatalf("n=%d should return error", n)
		}
		if !errors.Is(err, ratelimit.ErrInvalidN) {
			t.Fatalf("expected ErrInvalidN for n=%d, got: %v", n, err)
		}
	}
}
