package store

import (
	"errors"
	"fmt"
	"net"
	"syscall"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

type timeoutErr struct{}

func (timeoutErr) Error() string { return "i/o timeout" }
func (timeoutErr) Timeout() bool  { return true }

// TestIsConnectionError_Classification is the M-16 regression: ECONNREFUSED
// wrapped in a *net.OpError is a connection error (previously missed because it
// is not a Timeout), alongside timeouts and closed pools. Non-connection errors
// (e.g. WRONGTYPE) must NOT be classified as connection errors.
func TestIsConnectionError_Classification(t *testing.T) {
	connRefused := &net.OpError{
		Op:   "dial",
		Net:  "tcp",
		Addr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 6379},
		Err:  syscall.ECONNREFUSED,
	}

	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"redis closed pool", goredis.ErrClosed, true},
		{"timeout", timeoutErr{}, true},
		{"econnrefused net.OpError", connRefused, true},
		{"wrapped econnrefused", fmt.Errorf("redis get: %w", connRefused), true},
		{"bare econnrefused", syscall.ECONNREFUSED, true},
		{"econnreset", syscall.ECONNRESET, true},
		{"application error", errors.New("WRONGTYPE Operation against a key"), false},
		{"redis nil", goredis.Nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isConnectionError(tc.err); got != tc.want {
				t.Fatalf("isConnectionError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestMemory_IncrBy_TTLOnlyOnCreation documents the contract the Redis IncrBy fix
// (H-5) aligns to: the in-memory store already sets TTL only on creation, not on
// every increment. This guards against a regression in the shared semantics.
func TestMemory_IncrBy_TTLOnlyOnCreation(t *testing.T) {
	m := NewMemory(WithCleanupInterval(time.Hour))
	defer m.Close()
	ctx := t.Context()

	const ttl = 50 * time.Millisecond
	if _, err := m.IncrBy(ctx, "win", 1, ttl); err != nil {
		t.Fatalf("IncrBy create failed: %v", err)
	}
	// Increment several more times immediately, all comfortably WITHIN the TTL
	// window. These must not refresh the TTL. We deliberately stop incrementing
	// before the key can expire: an increment on an already-expired key would
	// re-create it with a fresh TTL (correct behaviour), but a cross-boundary
	// loop made the old test nondeterministic (F-6).
	for i := 0; i < 3; i++ {
		if _, err := m.IncrBy(ctx, "win", 1, ttl); err != nil {
			t.Fatalf("IncrBy failed: %v", err)
		}
	}
	if v, err := m.Get(ctx, "win"); err != nil || v != "4" {
		t.Fatalf("expected live counter value 4, got v=%q err=%v", v, err)
	}

	// Wait well past the ORIGINAL TTL (5x margin, robust under load) without
	// incrementing. If the increments had refreshed the TTL the key would still
	// be alive; because TTL is set only on creation, it must have expired.
	time.Sleep(5 * ttl)
	if _, err := m.Get(ctx, "win"); err != ErrNotFound {
		t.Fatalf("expected key to expire (TTL set only on creation), got err=%v", err)
	}
}
