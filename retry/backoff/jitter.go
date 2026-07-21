package backoff

import (
	"math/rand"
	"sync"
	"time"
)

// rngSource wraps an optional *rand.Rand behind a mutex so that concurrent
// calls to Next are safe even when the caller supplies their own source.
// When rng is nil, the package-level rand functions are used instead (those
// are goroutine-safe since Go 1.20).
type rngSource struct {
	mu  sync.Mutex
	rng *rand.Rand // nil means: use package-level rand
}

// int63n returns a non-negative pseudo-random int64 in [0, n).
func (r *rngSource) int63n(n int64) int64 {
	if r.rng == nil {
		return rand.Int63n(n)
	}
	r.mu.Lock()
	v := r.rng.Int63n(n)
	r.mu.Unlock()
	return v
}

// fullJitterBackoff implements the "Full Jitter" AWS backoff strategy.
// sleep = random(0, min(cap, base * 2^attempt))
type fullJitterBackoff struct {
	base time.Duration
	cap  time.Duration
	src  rngSource
}

// FullJitter returns a BackoffStrategy that applies full random jitter.
// The delay is uniformly distributed in [0, min(cap, base * 2^attempt)).
//
// rng is optional: pass a *rand.Rand to use a deterministic or isolated source
// (useful in tests and for reproducible behaviour). Omit it (or pass nil) to
// use the auto-seeded package-level source, which is goroutine-safe since
// Go 1.20. When a non-nil rng is supplied it is protected by an internal mutex
// so the returned strategy is safe for concurrent use.
func FullJitter(base, cap time.Duration, rng ...*rand.Rand) BackoffStrategy {
	b := &fullJitterBackoff{base: base, cap: cap}
	if len(rng) > 0 {
		b.src.rng = rng[0]
	}
	return b
}

// Next returns a random duration in [0, exponentialCap).
func (f *fullJitterBackoff) Next(attempt int) time.Duration {
	exp := exponentialCap(f.base, f.cap, attempt)
	if exp <= 0 {
		return 0
	}
	return time.Duration(f.src.int63n(int64(exp)))
}

// equalJitterBackoff implements the "Equal Jitter" AWS backoff strategy.
// sleep = cap/2 + random(0, cap/2)  where cap = min(maxCap, base * 2^attempt)
type equalJitterBackoff struct {
	base   time.Duration
	maxCap time.Duration
	src    rngSource
}

// EqualJitter returns a BackoffStrategy that applies equal jitter.
// Half of the delay is constant (cap/2) and half is random [0, cap/2).
//
// rng is optional: pass a *rand.Rand to use a deterministic or isolated source
// (useful in tests and for reproducible behaviour). Omit it (or pass nil) to
// use the auto-seeded package-level source, which is goroutine-safe since
// Go 1.20. When a non-nil rng is supplied it is protected by an internal mutex
// so the returned strategy is safe for concurrent use.
func EqualJitter(base, cap time.Duration, rng ...*rand.Rand) BackoffStrategy {
	b := &equalJitterBackoff{base: base, maxCap: cap}
	if len(rng) > 0 {
		b.src.rng = rng[0]
	}
	return b
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
	return half + time.Duration(e.src.int63n(int64(half)))
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
