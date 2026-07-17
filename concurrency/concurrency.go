package concurrency

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
)

// Outcome describes how a single acquired request finished. It is passed to the
// ReleaseFunc so the strategy can update the limit. RTT is the measured
// round-trip time; if left zero the Limiter measures wall-clock time from
// Acquire to release itself. Dropped marks a timeout/rejection/error, the
// strongest overload signal.
type Outcome struct {
	// RTT is the round-trip time of the request. If zero, the Limiter uses the
	// time elapsed between Acquire and release as the RTT.
	RTT time.Duration
	// Dropped is true when the request failed in a way that indicates overload
	// (timeout, rejection, connection error). Strategies back off hard on drops.
	Dropped bool
}

// ReleaseFunc must be called exactly once for every successful Acquire/Wait. It
// decrements the in-flight counter, feeds the Outcome to the limit strategy and
// wakes one waiter (if any). Calling it more than once is a no-op after the
// first call.
type ReleaseFunc func(Outcome)

// Limiter is an adaptive concurrency limiter. It admits work up to a limit that
// a LimitStrategy continuously adjusts from observed latency. The admission gate
// (Acquire/Inflight/Limit) is lock-free on the hot path using atomics; only the
// strategy update and the Wait queue take a lock.
type Limiter struct {
	strategy LimitStrategy
	clk      clock.Clock

	inflight int64 // atomic
	limit    int64 // atomic; rounded, installed by refreshLimit

	minLimit int64
	maxLimit int64

	// waiters are goroutines blocked in Wait. Signalled on release.
	waitMu   sync.Mutex
	waitCond *sync.Cond
}

// newLimiter wires a strategy and config into a Limiter. Used by the NewXxx
// constructors in strategy.go.
func newLimiter(strategy LimitStrategy, cfg Config, opts ...Option) *Limiter {
	o := newOptions(opts...)
	l := &Limiter{
		strategy: strategy,
		clk:      o.clk,
		inflight: 0,
		limit:    int64(cfg.InitialLimit),
		minLimit: int64(cfg.MinLimit),
		maxLimit: int64(cfg.MaxLimit),
	}
	l.waitCond = sync.NewCond(&l.waitMu)
	return l
}

// Limit returns the current concurrency limit.
func (l *Limiter) Limit() int { return int(atomic.LoadInt64(&l.limit)) }

// Inflight returns the number of requests currently admitted but not yet
// released.
func (l *Limiter) Inflight() int { return int(atomic.LoadInt64(&l.inflight)) }

// Acquire attempts to admit one request without blocking. It returns
// (release, true) if a slot was available, or (nil, false) if inflight already
// equals the current limit (the caller should shed). The returned ReleaseFunc
// must be called exactly once. The ctx is honoured for cancellation: an
// already-cancelled ctx yields (nil, false).
func (l *Limiter) Acquire(ctx context.Context) (ReleaseFunc, bool) {
	if ctx != nil {
		select {
		case <-ctx.Done():
			return nil, false
		default:
		}
	}
	for {
		cur := atomic.LoadInt64(&l.inflight)
		lim := atomic.LoadInt64(&l.limit)
		if cur >= lim {
			return nil, false
		}
		if atomic.CompareAndSwapInt64(&l.inflight, cur, cur+1) {
			return l.newRelease(), true
		}
		// CAS lost a race; retry.
	}
}

// Wait blocks until a slot is available or ctx is done, whichever comes first.
// It returns a ReleaseFunc on success or a non-nil error (ctx.Err()) on
// cancellation/timeout. Wait is bounded solely by ctx: pass a context with a
// deadline to cap the wait.
//
// Implementation: a release Broadcasts on the shared condition variable, and a
// single watcher goroutine converts ctx cancellation into a Broadcast too, so a
// blocked waiter always re-checks after either event. All waits are guarded by a
// predicate loop (inflight < limit || ctx.Err()) making spurious wakeups
// harmless.
func (l *Limiter) Wait(ctx context.Context) (ReleaseFunc, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	// Fast path: try a non-blocking acquire first.
	if rel, ok := l.Acquire(ctx); ok {
		return rel, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Watcher: turn ctx cancellation into a Broadcast so the waiter unblocks.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			l.waitCond.Broadcast()
		case <-stop:
		}
	}()

	l.waitMu.Lock()
	for {
		// Attempt acquire while unlocked? No — keep it simple: check predicate,
		// then try a real CAS acquire. The CAS is the source of truth.
		l.waitMu.Unlock()
		if rel, ok := l.Acquire(ctx); ok {
			return rel, nil
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		l.waitMu.Lock()
		// Re-check under the lock to avoid missing a Broadcast between the failed
		// Acquire above and the Wait below.
		if atomic.LoadInt64(&l.inflight) < atomic.LoadInt64(&l.limit) {
			continue
		}
		if err := ctx.Err(); err != nil {
			l.waitMu.Unlock()
			return nil, err
		}
		l.waitCond.Wait()
	}
}

// release carries the state a single ReleaseFunc needs: the acquire timestamp
// (for RTT fallback) and an atomic done flag for idempotency. It is allocated
// once per Acquire; its finish method is returned as the ReleaseFunc. Using a
// bound method rather than a closure-over-sync.Once keeps the hot path to a
// single allocation.
type release struct {
	lim   *Limiter
	start time.Time
	done  int32 // atomic; 0 = not yet released
}

// finish implements ReleaseFunc. It is idempotent: only the first call has any
// effect, so a double release cannot drive inflight negative.
func (r *release) finish(o Outcome) {
	if !atomic.CompareAndSwapInt32(&r.done, 0, 1) {
		return
	}
	l := r.lim
	rtt := o.RTT
	if rtt <= 0 {
		rtt = l.clk.Since(r.start)
	}
	// Decrement first so a waiter that wakes sees the freed slot.
	atomic.AddInt64(&l.inflight, -1)
	inflightNow := int(atomic.LoadInt64(&l.inflight))

	newLimit := l.strategy.Update(inflightNow+1, rtt, o.Dropped)
	l.installLimit(newLimit)

	// Wake waiters (Broadcast is safe; extra wakeups just re-check the predicate).
	l.waitCond.Broadcast()
}

// newRelease returns a ReleaseFunc bound to the current time.
func (l *Limiter) newRelease() ReleaseFunc {
	r := &release{lim: l, start: l.clk.Now()}
	return r.finish
}

// installLimit rounds and stores a strategy-produced limit, re-clamping to the
// integer [minLimit, maxLimit] band as a final safety net.
func (l *Limiter) installLimit(v float64) {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return
	}
	n := int64(math.Round(v))
	if n < l.minLimit {
		n = l.minLimit
	}
	if n > l.maxLimit {
		n = l.maxLimit
	}
	atomic.StoreInt64(&l.limit, n)
}
