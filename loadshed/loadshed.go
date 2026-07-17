// Package loadshed implements a CoDel-style (Controlled Delay) admission
// controller with priority-aware shedding.
//
// # What it does
//
// Under overload, dropping *the right* requests preserves far more useful
// throughput (goodput) than a flat limit. This shedder keys its decision on the
// measured queue sojourn time — how long a unit of work waits before it starts
// executing — rather than on an absolute in-flight count. That is the core CoDel
// insight from Nichols & Jacobson's "Controlling Queue Delay": a persistently
// high minimum sojourn over a sliding window signals a standing queue, and a
// standing queue is what wrecks tail latency. When that happens the shedder
// enters a "dropping" state and rejects a growing fraction of admissions until
// the sojourn recovers below the target.
//
// # Call-site convention
//
// Wrap the queue/admission point like this:
//
//	accept, done := shedder.Admit(ctx)
//	if !accept {
//		return ErrShed // reject: send back-pressure to the caller
//	}
//	defer done()
//	// ... do the work ...
//
// The window between Admit and done() is treated as the sojourn/latency sample
// that feeds the controller. For a queue, call Admit at enqueue time and done()
// when the work actually starts (or completes) so the measured interval is the
// real waiting time. Admit itself measures the sojourn as the time from the
// call until the returned done() runs.
//
// # Priority
//
// A caller may attach an integer priority to the context with WithPriority. When
// the shedder is in its dropping state it drops the LOWEST priorities first: a
// request whose priority is at or above a dynamic threshold is admitted even
// while lower-priority requests are being shed. Higher integers mean more
// important. Requests with no attached priority use PriorityDefault (0). The
// dynamic threshold rises the deeper into overload the controller is, so a
// mild overload only sheds the very lowest tier while a severe one sheds all but
// the top tier. Mis-set priorities can starve low tiers — that is by design
// under sustained overload, and is the caller's responsibility to tune.
//
// All exported methods are safe for concurrent use.
package loadshed

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
)

// Default CoDel parameters. These mirror the values from the CoDel paper scaled
// for an in-process admission queue.
const (
	// DefaultTarget is the acceptable standing queue sojourn. While the minimum
	// sojourn over an Interval stays below this, nothing is shed.
	DefaultTarget = 5 * time.Millisecond
	// DefaultInterval is the sliding window over which the minimum sojourn is
	// tracked and the initial spacing between drops once shedding begins.
	DefaultInterval = 100 * time.Millisecond
)

// Priority tiers. Higher integers are more important and are shed last.
const (
	// PriorityLow is a convenience tier for best-effort, sheddable work.
	PriorityLow = -1
	// PriorityDefault is the priority assumed when none is attached to the ctx.
	PriorityDefault = 0
	// PriorityHigh is a convenience tier for latency-critical work.
	PriorityHigh = 1
	// PriorityCritical is a convenience tier that survives all but total overload.
	PriorityCritical = 2
)

// priorityCeil bounds how high the dynamic drop threshold can climb. Capping it
// at PriorityCritical means a request at PriorityCritical (prio == ceil, not <)
// is never shed: even a runaway overload leaves the top tier a chance.
const priorityCeil = PriorityCritical

// Config tunes the CoDel controller. The zero value is not usable directly; use
// New, which fills unset fields with the Default* values.
type Config struct {
	// Target is the acceptable standing-queue sojourn time. The controller only
	// begins shedding when the minimum observed sojourn stays at or above Target
	// for a full Interval. Defaults to DefaultTarget.
	Target time.Duration

	// Interval is the sliding window over which the minimum sojourn is tracked,
	// and the base spacing between successive drops once shedding starts.
	// Defaults to DefaultInterval.
	Interval time.Duration

	// PriorityStep is how many consecutive drops must accumulate before the
	// dynamic priority threshold climbs one tier (shedding a higher priority
	// band). Larger values make priority escalation more gradual. Defaults to
	// defaultPriorityStep. Must be >= 1 after New normalises it.
	PriorityStep int
}

const defaultPriorityStep = 8

// contextKey is an unexported type for context keys defined in this package, to
// avoid collisions with keys from other packages.
type contextKey int

const priorityKey contextKey = 0

// WithPriority returns a copy of ctx carrying the admission priority p. Higher
// integers are more important and shed last. See the package doc for the
// call-site convention.
func WithPriority(ctx context.Context, p int) context.Context {
	return context.WithValue(ctx, priorityKey, p)
}

// PriorityFromContext returns the priority attached to ctx, or PriorityDefault
// if none is present.
func PriorityFromContext(ctx context.Context) int {
	if v, ok := ctx.Value(priorityKey).(int); ok {
		return v
	}
	return PriorityDefault
}

// Option customises a Shedder at construction time.
type Option func(*Shedder)

// WithClock injects a clock.Clock. Use clock.NewManualClock in tests for fully
// deterministic behaviour. Defaults to clock.RealClock.
func WithClock(clk clock.Clock) Option {
	return func(s *Shedder) { s.clk = clk }
}

// Shedder is a CoDel admission controller. Create one with New and share it
// across goroutines.
type Shedder struct {
	target       time.Duration
	interval     time.Duration
	priorityStep int
	clk          clock.Clock

	mu sync.Mutex
	// dropping reports whether the controller is currently in the CoDel
	// "dropping" state.
	dropping bool
	// firstAbove is the time at which the sojourn first went at/above target
	// during the current above-target streak. Zero means "not currently above".
	firstAbove time.Time
	// dropNext is the time at which the next drop is scheduled while dropping.
	dropNext time.Time
	// count is the number of drops since the current dropping state began; it
	// drives the CoDel control law (next interval shrinks by interval/sqrt(count)).
	count int
	// lastSojourn is the most recent completed sojourn sample (observability).
	lastSojourn time.Duration
}

// New creates a Shedder from cfg, filling unset fields with defaults.
func New(cfg Config, opts ...Option) *Shedder {
	if cfg.Target <= 0 {
		cfg.Target = DefaultTarget
	}
	if cfg.Interval <= 0 {
		cfg.Interval = DefaultInterval
	}
	if cfg.PriorityStep < 1 {
		cfg.PriorityStep = defaultPriorityStep
	}
	s := &Shedder{
		target:       cfg.Target,
		interval:     cfg.Interval,
		priorityStep: cfg.PriorityStep,
		clk:          clock.RealClock{},
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Admit decides whether to admit the work carried by ctx and returns a done
// closure that MUST be called exactly once when the work starts (or completes)
// to record its sojourn/latency for the controller.
//
// When accept is false the request is shed; the returned done is still safe to
// call (it is a no-op) so callers may unconditionally `defer done()`.
//
// Shedding is priority-aware: while the controller is dropping, a request whose
// context priority is at or above the current dynamic threshold is admitted even
// as lower-priority requests are rejected.
func (s *Shedder) Admit(ctx context.Context) (accept bool, done func()) {
	start := s.clk.Now()
	prio := PriorityFromContext(ctx)

	if !s.shouldDrop(start, prio) {
		// Admitted: hand back a done bound to a single heap allocation that
		// records the sojourn exactly once (idempotent under repeated calls).
		a := &admission{s: s, start: start}
		return true, a.done
	}
	return false, noopDone
}

// noopDone is the shared no-op returned on a shed, so shedding allocates nothing.
func noopDone() {}

// admission carries the per-request state for a done() callback. Keeping it in
// one struct (returned as a bound method value) makes an admitted call a single
// allocation and its done idempotent via an atomic guard.
type admission struct {
	s     *Shedder
	start time.Time
	fired atomic.Bool
}

// done records the request's sojourn the first time it is called; later calls
// are no-ops, so callers may safely `defer done()` and re-invoke.
func (a *admission) done() {
	if a.fired.CompareAndSwap(false, true) {
		a.s.record(a.s.clk.Since(a.start))
	}
}

// AdmitSimple is a convenience wrapper around Admit for callers that do not need
// to feed sojourn/latency back (for example a pure gate in front of a
// synchronous handler). It records a zero-length sojourn on admission, which
// keeps the controller's minimum low and is appropriate when the caller is not
// itself the queue being controlled. Prefer Admit with a real done() whenever
// the guarded section's duration is the signal you want to control on.
func (s *Shedder) AdmitSimple(ctx context.Context) bool {
	accept, done := s.Admit(ctx)
	if accept {
		done()
	}
	return accept
}

// shouldDrop runs the CoDel control law plus priority gate under the lock and
// returns whether the request at priority prio should be dropped at time now.
func (s *Shedder) shouldDrop(now time.Time, prio int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Determine whether the tracked minimum sojourn is currently above target.
	// We approximate the CoDel "min over interval" using the standing-queue
	// signal maintained in record(): firstAbove is non-zero exactly while the
	// most recent samples have stayed at/above target.
	standingTooLong := !s.firstAbove.IsZero() && !now.Before(s.firstAbove.Add(s.interval))

	if !s.dropping {
		if !standingTooLong {
			return false
		}
		// Enter the dropping state.
		s.dropping = true
		s.count = 1
		s.dropNext = now.Add(s.interval)
		return s.priorityGate(prio)
	}

	// Already dropping. If the queue has drained below target, leave the state.
	if s.firstAbove.IsZero() {
		s.dropping = false
		s.count = 0
		return false
	}

	// Still overloaded: advance the CoDel control law. Every time we reach the
	// scheduled dropNext we tighten the next interval by interval/sqrt(count) —
	// so the drop cadence, and with it the dynamic priority threshold, escalates
	// the longer overload persists. count may advance several notches in one
	// call if the clock jumped past multiple scheduled instants.
	for !now.Before(s.dropNext) {
		s.count++
		s.dropNext = s.controlLaw(now)
	}

	// The priority gate decides admission for every request while dropping: a
	// request at or above the current dynamic threshold is admitted even as
	// lower-priority ones are shed. The threshold rises with count, so deeper
	// overload sheds progressively higher priority bands.
	return s.priorityGate(prio)
}

// controlLaw computes the next drop time: now + interval/sqrt(count). Must hold s.mu.
func (s *Shedder) controlLaw(now time.Time) time.Time {
	inv := 1.0 / math.Sqrt(float64(s.count))
	return now.Add(time.Duration(float64(s.interval) * inv))
}

// priorityGate reports whether a request at priority prio should be dropped
// given the current drop count. The dynamic threshold climbs one tier every
// priorityStep drops, so deeper overload sheds higher priority bands. A request
// at or above the threshold is admitted (returns false). Must hold s.mu.
func (s *Shedder) priorityGate(prio int) bool {
	// At overload onset (count 1) the threshold is PriorityDefault, so the
	// lowest tier (PriorityLow) is shed immediately. It climbs one tier every
	// priorityStep drops, capped at priorityCeil so PriorityCritical work always
	// has a chance.
	threshold := PriorityDefault + (s.count-1)/s.priorityStep
	if threshold > priorityCeil {
		threshold = priorityCeil
	}
	// Drop when the request's priority is strictly below the threshold.
	return prio < threshold
}

// record feeds a completed sojourn sample into the standing-queue tracker. It
// maintains firstAbove: set when a sample first reaches target and cleared as
// soon as a sample drops below, so shouldDrop can tell whether the queue has
// been standing above target for a full interval.
func (s *Shedder) record(sojourn time.Duration) {
	now := s.clk.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastSojourn = sojourn
	if sojourn >= s.target {
		if s.firstAbove.IsZero() {
			s.firstAbove = now
		}
	} else {
		// Sojourn recovered below target: the standing queue is gone.
		s.firstAbove = time.Time{}
	}
}

// Dropping reports whether the controller is currently in the CoDel dropping
// state. Intended for observability/tests.
func (s *Shedder) Dropping() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dropping
}

// DropCount returns the number of drops accumulated in the current dropping
// state (0 when not dropping). Intended for observability/tests.
func (s *Shedder) DropCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}

// LastSojourn returns the most recently recorded sojourn sample. Intended for
// observability/tests.
func (s *Shedder) LastSojourn() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastSojourn
}
