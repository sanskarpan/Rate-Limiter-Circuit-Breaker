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
	mu   sync.Mutex // guards rng and prev for concurrent use
	rng  *rand.Rand
	prev time.Duration
}

// Decorrelated returns a BackoffStrategy using the AWS decorrelated jitter formula.
// Each delay is min(cap, random(base, prev*3)) where prev starts at base.
// rng must not be nil.
func Decorrelated(base, cap time.Duration, rng *rand.Rand) BackoffStrategy {
	return &decorrelatedJitter{
		base: base,
		cap:  cap,
		rng:  rng,
		prev: base, // initial previous = base per the AWS formula
	}
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

	// random in [lower, upper)
	spread := int64(upper - lower)
	sleep := lower + time.Duration(d.rng.Int63n(spread))

	// final cap guard
	if sleep > d.cap {
		sleep = d.cap
	}
	d.prev = sleep
	return sleep
}
