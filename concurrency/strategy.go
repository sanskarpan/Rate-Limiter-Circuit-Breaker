package concurrency

import (
	"math"
	"sync"
	"time"
)

// LimitStrategy is the pluggable control loop that recomputes the concurrency
// limit from each request's outcome. Update is called once per ReleaseFunc
// invocation with the number of requests that were in flight when this one
// started, the measured RTT, and whether the request was dropped (timeout,
// rejection, error). It returns the new limit as a float64; the Limiter rounds
// and installs it. Implementations must be safe for concurrent Update calls and
// must always return a value within the configured [MinLimit, MaxLimit] band.
type LimitStrategy interface {
	Update(inflight int, rtt time.Duration, dropped bool) (newLimit float64)
	// Limit reports the current (unrounded) limit without mutating state.
	Limit() float64
}

// baseTracker maintains a decaying windowed minimum RTT (the baseRTT), which
// every strategy uses as the "no queueing" reference latency. A plain running
// minimum would be pinned forever by a single fast outlier; instead we let the
// minimum drift back up toward observed RTTs over decayWindow samples so the
// baseline follows genuine shifts (e.g. a downstream deploy) without chasing
// noise. Guarded by mu because strategies are updated concurrently.
type baseTracker struct {
	mu         sync.Mutex
	base       time.Duration
	decayEvery int
	sinceDecay int
}

func newBaseTracker(decayWindow int) baseTracker {
	if decayWindow <= 0 {
		decayWindow = defaultBaseRTTDecayWindow
	}
	return baseTracker{decayEvery: decayWindow}
}

// observe records an RTT and returns the current baseRTT estimate. A zero or
// negative rtt is ignored (returns the existing base). The returned value is
// always > 0 once at least one positive sample has been seen.
func (b *baseTracker) observe(rtt time.Duration) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	if rtt > 0 {
		if b.base == 0 || rtt < b.base {
			b.base = rtt
		} else {
			// Slowly decay the minimum upward so a shifted baseline is tracked.
			b.sinceDecay++
			if b.sinceDecay >= b.decayEvery {
				b.sinceDecay = 0
				// Nudge base 1/16 of the way toward the observed rtt.
				b.base += (rtt - b.base) / 16
			}
		}
	}
	if b.base <= 0 {
		return time.Nanosecond
	}
	return b.base
}

// ---------------------------------------------------------------------------
// AIMD
// ---------------------------------------------------------------------------

// aimd is an additive-increase / multiplicative-decrease strategy. On a
// successful request whose RTT stays under the latency threshold it nudges the
// limit up by a constant increment; on a drop or a request slower than the
// threshold it multiplies the limit by backoffRatio. This is the simplest and
// most robust strategy — it makes no assumptions about the shape of the latency
// curve — at the cost of a sawtooth limit under steady load.
type aimd struct {
	cfg          Config
	base         baseTracker
	increment    float64
	backoffRatio float64
	tolerance    float64 // RTT threshold = baseRTT * tolerance

	mu    sync.Mutex
	limit float64
}

// NewAIMD constructs a Limiter driven by the AIMD strategy. increment is the
// additive step applied on success (defaults to 1); backoffRatio is the
// multiplier applied on a drop/slow request (defaults to 0.9, must be in
// (0,1)). Config.RTTTolerance scales the latency threshold above baseRTT.
func NewAIMD(cfg Config, opts ...Option) *Limiter {
	cfg = cfg.normalise()
	inc := 1.0
	backoff := 0.9
	tolerance := cfg.RTTTolerance
	if tolerance < 1 {
		tolerance = 1
	}
	s := &aimd{
		cfg:          cfg,
		base:         newBaseTracker(defaultBaseRTTDecayWindow),
		increment:    inc,
		backoffRatio: backoff,
		tolerance:    tolerance,
		limit:        float64(cfg.InitialLimit),
	}
	return newLimiter(s, cfg, opts...)
}

func (s *aimd) Limit() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.limit
}

func (s *aimd) Update(inflight int, rtt time.Duration, dropped bool) float64 {
	base := s.base.observe(rtt)
	lo := float64(s.cfg.MinLimit)
	hi := float64(s.cfg.MaxLimit)

	s.mu.Lock()
	defer s.mu.Unlock()

	slow := rtt > time.Duration(float64(base)*s.tolerance)
	if dropped || slow {
		s.limit = clamp(s.limit*s.backoffRatio, lo, hi)
		return s.limit
	}
	// Only grow when we are actually using most of the current limit; otherwise
	// there is no demand signal justifying a larger limit (avoids runaway).
	if float64(inflight) >= s.limit/2 {
		s.limit = clamp(s.limit+s.increment, lo, hi)
	}
	return s.limit
}

// ---------------------------------------------------------------------------
// Gradient2
// ---------------------------------------------------------------------------

// gradient2 implements Netflix's Gradient2 algorithm. It compares a short-term
// RTT (the most recent sample, itself lightly smoothed) against a long-term
// EWMA of RTT. The ratio gradient = longRTT/shortRTT is clamped to [0.5, 1.0]:
// when the short-term RTT rises above the long-term average (queueing building
// up) the gradient falls below 1 and the limit is pulled down; when latency is
// stable the gradient sits at 1 and the limit is allowed to grow by the queue
// headroom term. The whole thing is smoothed to damp oscillation.
type gradient2 struct {
	cfg       Config
	longRTT   ewma
	tolerance float64
	smoothing float64 // fraction of the newly computed limit blended in per update

	mu    sync.Mutex
	limit float64
}

// NewGradient2 constructs a Limiter driven by the Gradient2 strategy — the
// recommended default. Config.RTTTolerance widens the acceptable latency band
// (larger values make the limiter less twitchy).
func NewGradient2(cfg Config, opts ...Option) *Limiter {
	cfg = cfg.normalise()
	tolerance := cfg.RTTTolerance
	if tolerance < 1 {
		tolerance = 1
	}
	s := &gradient2{
		cfg:       cfg,
		longRTT:   newEWMA(),
		tolerance: tolerance,
		smoothing: 0.2,
		limit:     float64(cfg.InitialLimit),
	}
	return newLimiter(s, cfg, opts...)
}

func (s *gradient2) Limit() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.limit
}

func (s *gradient2) Update(inflight int, rtt time.Duration, dropped bool) float64 {
	lo := float64(s.cfg.MinLimit)
	hi := float64(s.cfg.MaxLimit)

	s.mu.Lock()
	defer s.mu.Unlock()

	// On a drop, halve toward the floor immediately — a drop is the strongest
	// possible overload signal and we should not wait for the gradient math.
	if dropped {
		s.limit = clamp(s.limit*0.5, lo, hi)
		return s.limit
	}
	if rtt <= 0 {
		return s.limit
	}

	shortRTT := float64(rtt)
	longRTT := s.longRTT.update(shortRTT)
	if longRTT <= 0 {
		return s.limit
	}

	// gradient in [0.5, 1.0]: <1 means short-term latency exceeds the long-term
	// baseline (queue forming) so we shrink; ==1 means stable so we may grow.
	// The tolerance factor lets short-term RTT run tolerance× hot before the
	// gradient starts biting.
	gradient := clamp((longRTT*s.tolerance)/shortRTT, 0.5, 1.0)

	// Queue headroom: how many extra slots we allow above what is strictly in
	// use, scaled by sqrt(limit) as Netflix does to grow faster when large.
	queueSize := math.Sqrt(s.limit)

	newLimit := s.limit*gradient + queueSize
	// Smooth to damp oscillation: blend the new target with the current limit.
	s.limit = clamp(s.limit*(1-s.smoothing)+newLimit*s.smoothing, lo, hi)
	return s.limit
}

// ---------------------------------------------------------------------------
// Vegas
// ---------------------------------------------------------------------------

// vegas implements a TCP-Vegas-style congestion estimate. It estimates the
// number of requests queued at the dependency as queue = limit*(1 - baseRTT/rtt)
// and steers the limit so that queue stays inside a target band [alpha, beta]:
// below alpha means we are under-utilising and can grow; above beta means a
// queue is forming and we must shrink. Vegas reacts to queueing delay before
// any packets are dropped, making it the most proactive of the three.
type vegas struct {
	cfg   Config
	base  baseTracker
	alpha float64
	beta  float64

	mu    sync.Mutex
	limit float64
}

// NewVegas constructs a Limiter driven by the Vegas strategy. alpha and beta
// are seeded from Config.RTTTolerance: alpha = 3*tolerance, beta = 6*tolerance
// queued requests, matching Netflix's default log-scaled band.
func NewVegas(cfg Config, opts ...Option) *Limiter {
	cfg = cfg.normalise()
	tolerance := cfg.RTTTolerance
	if tolerance <= 0 {
		tolerance = 1
	}
	s := &vegas{
		cfg:   cfg,
		base:  newBaseTracker(defaultBaseRTTDecayWindow),
		alpha: 3 * tolerance,
		beta:  6 * tolerance,
		limit: float64(cfg.InitialLimit),
	}
	return newLimiter(s, cfg, opts...)
}

func (s *vegas) Limit() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.limit
}

func (s *vegas) Update(inflight int, rtt time.Duration, dropped bool) float64 {
	base := s.base.observe(rtt)
	lo := float64(s.cfg.MinLimit)
	hi := float64(s.cfg.MaxLimit)

	s.mu.Lock()
	defer s.mu.Unlock()

	if dropped {
		s.limit = clamp(s.limit*0.5, lo, hi)
		return s.limit
	}
	if rtt <= 0 {
		return s.limit
	}

	// queue = limit * (1 - baseRTT/rtt). Ratio in [0,1]; if rtt < base (a new
	// minimum) the ratio exceeds 1 and queue goes slightly negative — clamp to
	// 0 so a fresh baseline never spuriously shrinks the limit.
	ratio := safeRatio(float64(base), float64(rtt), 1.0)
	if ratio > 1 {
		ratio = 1
	}
	queue := s.limit * (1 - ratio)
	if queue < 0 {
		queue = 0
	}

	switch {
	case queue < s.alpha:
		// Under-utilised: additive increase (log-scaled so large limits still move).
		s.limit += math.Log(s.limit + 1)
	case queue > s.beta:
		// Queue building: additive decrease.
		s.limit -= math.Log(s.limit + 1)
	}
	s.limit = clamp(s.limit, lo, hi)
	return s.limit
}

// ---------------------------------------------------------------------------
// EWMA helper
// ---------------------------------------------------------------------------

// ewma is a simple exponentially weighted moving average used as Gradient2's
// long-window RTT. The weight is deliberately small so the long window reacts
// slowly relative to the raw short-term sample.
type ewma struct {
	value float64
	alpha float64
	init  bool
}

func newEWMA() ewma { return ewma{alpha: 0.05} }

func (e *ewma) update(sample float64) float64 {
	if !e.init {
		e.value = sample
		e.init = true
		return e.value
	}
	e.value = e.value*(1-e.alpha) + sample*e.alpha
	return e.value
}
