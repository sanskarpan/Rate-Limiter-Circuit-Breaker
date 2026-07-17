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

	if _, err := m.IncrBy(ctx, "win", 1, 60*time.Millisecond); err != nil {
		t.Fatalf("IncrBy create failed: %v", err)
	}
	// Keep incrementing; TTL must NOT be refreshed by later increments.
	deadline := time.Now().Add(120 * time.Millisecond)
	for time.Now().Before(deadline) {
		_, _ = m.IncrBy(ctx, "win", 1, 60*time.Millisecond)
		time.Sleep(15 * time.Millisecond)
	}
	// After > original TTL from creation, the key must have expired despite the
	// repeated increments (proving TTL was set only on creation).
	if _, err := m.Get(ctx, "win"); err != ErrNotFound {
		t.Fatalf("expected key to expire (TTL set only on creation), got err=%v", err)
	}
}
