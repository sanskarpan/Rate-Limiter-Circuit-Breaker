package retry

import (
	"sync"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
)

// Budget is a thread-safe retry budget that caps the *rate* of retries as a
// fraction of observed throughput, guarding against retry storms.
//
// # Motivation
//
// Unbudgeted retries amplify load exactly when a dependency is failing: a fleet
// of clients each retrying MaxAttempts times can multiply the offered load on a
// brownout dependency by that factor, turning a partial failure into a full
// outage. Mature stacks (Envoy retry budgets, gRPC retryThrottling, Polly,
// Hystrix, the Google SRE book) therefore cap retries as a fraction of
// throughput rather than per request. A single failing request may still retry;
// a whole cohort of failing requests cannot collectively amplify load beyond the
// budget.
//
// # Algorithm
//
// Budget is a token bucket whose tokens represent *permitted retries*.
//
//   - Capacity (burst) is Burst tokens. The bucket starts full.
//
//   - The bucket refills continuously at a rate of
//
//     refillRate = max(MinPerSecond, Ratio × requestRate)   tokens/second
//
//     where requestRate is the observed throughput of top-level calls, measured
//     as an exponentially weighted moving average (EWMA) of the inter-arrival
//     rate of RecordAttempt calls.
//
//   - CanRetry consumes one token if at least one whole token is available and
//     returns true; otherwise it consumes nothing and returns false. This is the
//     key retry-storm guard: once the bucket drains, retries are denied until
//     throughput/time refills it.
//
//   - Deposit(n) optionally returns tokens to the bucket (capped at Burst),
//     modelling gRPC retryThrottling's "return a token on every successful
//     request" behaviour so that a healthy dependency keeps the budget topped up.
//
// Because refillRate is proportional to throughput (floored at MinPerSecond), a
// high-traffic service is allowed proportionally more retries (≈ Ratio × rps),
// while a low-traffic service still gets a guaranteed floor of MinPerSecond
// retries so that occasional legitimate transient failures are not starved.
//
// All methods are safe for concurrent use.
type Budget struct {
	ratio        float64 // retries permitted per request of throughput
	minPerSecond float64 // floor on refill rate (tokens/sec)
	burst        float64 // bucket capacity (max tokens)

	clk clock.Clock

	mu       sync.Mutex
	tokens   float64   // current retry tokens available
	reqRate  float64   // EWMA of observed request rate (req/sec)
	lastReq  time.Time // timestamp of the previous RecordAttempt
	lastFill time.Time // timestamp tokens were last refilled
	haveReq  bool      // whether at least one RecordAttempt has been seen
}

// BudgetConfig configures a Budget.
//
// Zero value: NewBudget(BudgetConfig{}) does not panic — it yields a budget with
// Burst defaulted to 1 and no refill (Ratio 0, MinPerSecond 0), i.e. it permits
// a single retry and then stays exhausted. That is rarely what you want: set at
// least Ratio or MinPerSecond so the bucket actually refills. The zero value is
// tolerated for composability, not recommended as a preset.
type BudgetConfig struct {
	// Ratio is the fraction of throughput permitted as retries. For example,
	// Ratio 0.1 permits roughly one retry per ten top-level requests at steady
	// state. Must be >= 0; a value of 0 disables ratio-based refill (only the
	// MinPerSecond floor applies).
	Ratio float64

	// MinPerSecond is the guaranteed minimum retry refill rate (tokens/second),
	// applied even at very low throughput so that a low-traffic caller still gets
	// a baseline retry allowance. Must be >= 0.
	MinPerSecond float64

	// Burst is the bucket capacity: the maximum number of retry tokens that can
	// accumulate, and therefore the largest instantaneous burst of retries
	// permitted. If <= 0 it defaults to max(MinPerSecond, 1) so the bucket can
	// always hold at least one token.
	Burst float64
}

// BudgetOption customises a Budget.
type BudgetOption func(*Budget)

// WithBudgetClock sets the time source used by the Budget. Use a
// clock.ManualClock in tests for deterministic behaviour. If unset, a
// clock.RealClock is used.
func WithBudgetClock(clk clock.Clock) BudgetOption {
	return func(b *Budget) {
		if clk != nil {
			b.clk = clk
		}
	}
}

// NewBudget creates a retry Budget from cfg. The bucket starts full (at Burst
// tokens). See Budget for the algorithm.
func NewBudget(cfg BudgetConfig, opts ...BudgetOption) *Budget {
	burst := cfg.Burst
	if burst <= 0 {
		burst = cfg.MinPerSecond
		if burst < 1 {
			burst = 1
		}
	}
	b := &Budget{
		ratio:        maxF(cfg.Ratio, 0),
		minPerSecond: maxF(cfg.MinPerSecond, 0),
		burst:        burst,
		clk:          clock.RealClock{},
		tokens:       burst,
	}
	for _, opt := range opts {
		opt(b)
	}
	now := b.clk.Now()
	b.lastFill = now
	b.lastReq = now
	return b
}

// RecordAttempt records one top-level call (a unit of throughput). It updates
// the observed request-rate estimate that drives the ratio-based refill. Call it
// exactly once per top-level invocation, before any retries.
func (b *Budget) RecordAttempt() {
	now := b.clk.Now()
	b.mu.Lock()
	defer b.mu.Unlock()

	b.refillLocked(now)

	if !b.haveReq {
		// First observation: seed the rate at the floor so the ratio term has a
		// sane starting point rather than exploding from a near-zero interval.
		b.haveReq = true
		b.reqRate = b.minPerSecond
		b.lastReq = now
		return
	}

	dt := now.Sub(b.lastReq).Seconds()
	b.lastReq = now
	if dt <= 0 {
		// Simultaneous/duplicate observations under a manual clock: treat as an
		// instantaneous burst without corrupting the rate estimate.
		return
	}

	// Instantaneous rate of this single arrival, blended into an EWMA. The
	// smoothing factor decays with the gap so long idle periods pull the
	// estimate down toward zero (few requests => low rate => ratio term shrinks).
	inst := 1.0 / dt
	const tau = 1.0 // seconds; EWMA time constant
	alpha := 1 - expNeg(dt/tau)
	b.reqRate = b.reqRate + alpha*(inst-b.reqRate)
	if b.reqRate < 0 {
		b.reqRate = 0
	}
}

// CanRetry reports whether a retry is permitted. If a whole retry token is
// available it consumes one and returns true. If the budget is exhausted it
// consumes nothing and returns false. Call it before each *extra* attempt.
func (b *Budget) CanRetry() bool {
	now := b.clk.Now()
	b.mu.Lock()
	defer b.mu.Unlock()

	b.refillLocked(now)
	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}

// Deposit returns n retry tokens to the bucket (capped at Burst). This models
// gRPC retryThrottling's token-return-on-success: healthy requests replenish the
// retry budget so a recovered dependency quickly regains its full retry
// allowance. n <= 0 is a no-op. Deposit is optional; the ratio/time-based refill
// alone is sufficient for a working budget.
func (b *Budget) Deposit(n float64) {
	if n <= 0 {
		return
	}
	now := b.clk.Now()
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refillLocked(now)
	b.tokens += n
	if b.tokens > b.burst {
		b.tokens = b.burst
	}
}

// Tokens returns the current number of available retry tokens, refilled to the
// current time. Intended for tests and observability.
func (b *Budget) Tokens() float64 {
	now := b.clk.Now()
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refillLocked(now)
	return b.tokens
}

// refillRateLocked returns the current refill rate in tokens/second:
// max(MinPerSecond, Ratio × observed request rate).
func (b *Budget) refillRateLocked() float64 {
	rate := b.ratio * b.reqRate
	if rate < b.minPerSecond {
		rate = b.minPerSecond
	}
	return rate
}

// refillLocked adds tokens accrued since the last refill based on the current
// refill rate, capping at burst. Must hold b.mu.
func (b *Budget) refillLocked(now time.Time) {
	elapsed := now.Sub(b.lastFill).Seconds()
	if elapsed <= 0 {
		return
	}
	b.lastFill = now
	b.tokens += elapsed * b.refillRateLocked()
	if b.tokens > b.burst {
		b.tokens = b.burst
	}
}

func maxF(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

// expNeg returns exp(-x) for x >= 0 without importing math, using a small
// series/identity so the package stays dependency-light. For the EWMA smoothing
// factor an approximation is entirely sufficient. We clamp the result to (0,1].
func expNeg(x float64) float64 {
	if x <= 0 {
		return 1
	}
	if x > 40 { // exp(-40) is negligible
		return 0
	}
	// Reduce range by repeated squaring of exp(-x/2^k), computed via Taylor
	// series where the argument is small (< ~1), which converges quickly.
	k := 0
	for x > 0.5 {
		x /= 2
		k++
	}
	// Taylor series for exp(-x) with small x.
	term := 1.0
	sum := 1.0
	for i := 1; i <= 12; i++ {
		term *= -x / float64(i)
		sum += term
	}
	for ; k > 0; k-- {
		sum *= sum
	}
	if sum < 0 {
		return 0
	}
	if sum > 1 {
		return 1
	}
	return sum
}
