package backoff

import (
	"math/rand"
	"sync"
	"time"
)

// fullJitterBackoff implements the "Full Jitter" AWS backoff strategy.
// sleep = random(0, min(cap, base * 2^attempt))
type fullJitterBackoff struct {
	base time.Duration
	cap  time.Duration
	mu   sync.Mutex // guards concurrent use of rng
	rng  *rand.Rand
}

// FullJitter returns a BackoffStrategy that applies full random jitter.
// The delay is uniformly distributed in [0, min(cap, base * 2^attempt)).
// rng must not be nil; it is the caller's responsibility to seed it.
func FullJitter(base, cap time.Duration, rng *rand.Rand) BackoffStrategy {
	return &fullJitterBackoff{base: base, cap: cap, rng: rng}
}

// Next returns a random duration in [0, exponentialCap).
func (f *fullJitterBackoff) Next(attempt int) time.Duration {
	exp := exponentialCap(f.base, f.cap, attempt)
	if exp <= 0 {
		return 0
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return time.Duration(f.rng.Int63n(int64(exp)))
}

// equalJitterBackoff implements the "Equal Jitter" AWS backoff strategy.
// sleep = cap/2 + random(0, cap/2)  where cap = min(maxCap, base * 2^attempt)
type equalJitterBackoff struct {
	base   time.Duration
	maxCap time.Duration
	mu     sync.Mutex // guards concurrent use of rng
	rng    *rand.Rand
}

// EqualJitter returns a BackoffStrategy that applies equal jitter.
// Half of the delay is constant (cap/2) and half is random [0, cap/2).
// rng must not be nil.
func EqualJitter(base, cap time.Duration, rng *rand.Rand) BackoffStrategy {
	return &equalJitterBackoff{base: base, maxCap: cap, rng: rng}
}

// Next returns cap/2 + random(0, cap/2) where cap = min(maxCap, base * 2^attempt).
func (e *equalJitterBackoff) Next(attempt int) time.Duration {
	cap := exponentialCap(e.base, e.maxCap, attempt)
	if cap <= 0 {
		return 0
	}
	half := cap / 2
	if half <= 0 {
		return cap
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return half + time.Duration(e.rng.Int63n(int64(half)))
}

// exponentialCap computes min(maxCap, base * 2^attempt) without overflowing.
func exponentialCap(base, maxCap time.Duration, attempt int) time.Duration {
	// A non-positive base yields no exponential growth. Return 0 so it is not
	// conflated with an overflow (which clamps to maxCap below). The jitter
	// Next methods already treat a <=0 cap as a zero delay.
	if base <= 0 {
		return 0
	}
	const maxShift = 62
	shift := attempt
	if shift > maxShift {
		shift = maxShift
	}
	d := base << uint(shift)
	if d <= 0 || d > maxCap {
		return maxCap
	}
	return d
}
