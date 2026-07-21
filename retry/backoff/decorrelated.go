package backoff

import (
	"math/rand"
	"sync"
	"time"
)

// decorrelatedJitter implements the AWS "Decorrelated Jitter" backoff strategy.
// sleep = min(cap, random(base, prev * 3))
// The previous sleep duration feeds back into the next calculation, breaking
// the correlation between retries in concurrent clients.
type decorrelatedJitter struct {
	base time.Duration
	cap  time.Duration
	mu   sync.Mutex // guards prev and the optional caller-supplied rng
	prev time.Duration
	rng  *rand.Rand // nil means: use package-level rand
}

// Decorrelated returns a BackoffStrategy using the AWS decorrelated jitter formula.
// Each delay is min(cap, random(base, prev*3)) where prev starts at base.
//
// rng is optional: pass a *rand.Rand to use a deterministic or isolated source
// (useful in tests and for reproducible behaviour). Omit it (or pass nil) to
// use the auto-seeded package-level source, which is goroutine-safe since
// Go 1.20. When a non-nil rng is supplied it is accessed under the strategy's
// internal mutex so the returned strategy is safe for concurrent use.
func Decorrelated(base, cap time.Duration, rng ...*rand.Rand) BackoffStrategy {
	d := &decorrelatedJitter{
		base: base,
		cap:  cap,
		prev: base, // initial previous = base per the AWS formula
	}
	if len(rng) > 0 {
		d.rng = rng[0]
	}
	return d
}

// Next computes min(cap, random(base, prev*3)) and stores result as new prev.
func (d *decorrelatedJitter) Next(_ int) time.Duration {
	d.mu.Lock()
	defer d.mu.Unlock()

	// upper bound: prev * 3, clamped to cap to avoid extreme values
	upper := d.prev * 3
	if upper > d.cap || upper <= 0 {
		upper = d.cap
	}

	// lower bound is base
	lower := d.base
	if lower >= upper {
		// base >= upper: just return base (or cap if base > cap)
		sleep := lower
		if sleep > d.cap {
			sleep = d.cap
		}
		d.prev = sleep
		return sleep
	}

	// random in [lower, upper) — d.mu already held, so access d.rng directly.
	spread := int64(upper - lower)
	var jitter int64
	if d.rng != nil {
		jitter = d.rng.Int63n(spread) // d.mu protects d.rng from concurrent access
	} else {
		jitter = rand.Int63n(spread) // package-level source is goroutine-safe
	}
	sleep := lower + time.Duration(jitter)

	// final cap guard
	if sleep > d.cap {
		sleep = d.cap
	}
	d.prev = sleep
	return sleep
}
