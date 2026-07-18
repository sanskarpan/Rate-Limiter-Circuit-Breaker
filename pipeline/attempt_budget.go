package pipeline

import (
	"context"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/retry"
)

// attemptBudgetConfig holds the resolved per-attempt deadline-budgeting policy
// for a retry stage. It is built from AttemptBudgetOption values.
type attemptBudgetConfig struct {
	// perAttempt, when > 0, is a fixed per-attempt timeout. Each attempt's
	// derived deadline is min(overall remaining, perAttempt).
	perAttempt time.Duration
	// divide, when true, auto-divides the remaining overall deadline evenly
	// across the attempts that have not yet run, so every attempt gets a fair,
	// shrinking slice. If both perAttempt and divide are set, the per-attempt
	// deadline is min(perAttempt, remaining/attemptsLeft, overall remaining).
	divide bool
	// clk is the time source used to read "now" when computing the remaining
	// overall deadline. Defaults to clock.RealClock{}. Injectable for tests.
	clk clock.Clock
}

// AttemptBudgetOption configures per-attempt deadline budgeting for a retry
// stage added with Builder.RetryBudgeted. Options are additive; a stage with no
// options behaves exactly like the plain Retry stage (each attempt inherits the
// caller's context deadline unchanged).
type AttemptBudgetOption func(*attemptBudgetConfig)

// WithPerAttemptTimeout enforces a fixed per-attempt timeout d. Each retry
// attempt runs with a derived context whose deadline is
// min(overall remaining, d). If d <= 0 the option is a no-op.
//
// This guarantees a single slow attempt cannot consume the whole overall
// deadline, while the min(...) clamp guarantees no attempt ever runs past the
// overall context deadline. When the overall deadline is exhausted before an
// attempt can start, RetryBudgeted stops retrying and returns
// context.DeadlineExceeded.
func WithPerAttemptTimeout(d time.Duration) AttemptBudgetOption {
	return func(c *attemptBudgetConfig) {
		if d > 0 {
			c.perAttempt = d
		}
	}
}

// WithAttemptBudgeting divides the remaining overall context deadline evenly
// across the retry attempts that have not yet run, giving each attempt a fair,
// shrinking slice (attempt 1 gets remaining/N, the next remaining'/(N-1), and
// so on). It requires the caller's context to carry a deadline; without one it
// is a no-op (there is nothing to divide) and attempts inherit the context
// unchanged.
//
// Combine with WithPerAttemptTimeout to also cap each slice at a fixed ceiling:
// the derived deadline is then min(perAttempt, remaining/attemptsLeft).
func WithAttemptBudgeting() AttemptBudgetOption {
	return func(c *attemptBudgetConfig) { c.divide = true }
}

// WithAttemptBudgetClock sets the time source used to compute the remaining
// overall deadline for per-attempt budgeting. It defaults to clock.RealClock{}.
// Tests inject a clock.ManualClock so budgeting is deterministic without
// wall-clock sleeps.
func WithAttemptBudgetClock(clk clock.Clock) AttemptBudgetOption {
	return func(c *attemptBudgetConfig) {
		if clk != nil {
			c.clk = clk
		}
	}
}

// clockOrReal returns the configured clock, defaulting to RealClock.
func (c *attemptBudgetConfig) clockOrReal() clock.Clock {
	if c.clk != nil {
		return c.clk
	}
	return clock.RealClock{}
}

// RetryBudgeted adds a retry stage with per-attempt deadline budgeting layered
// on top of the given policy. It behaves like Retry(p) — honouring the policy's
// MaxAttempts, backoff, RetryIf, and any attached Budget — but additionally
// derives a per-attempt context deadline for each attempt so that:
//
//   - No single attempt can consume the whole overall deadline.
//   - The total wall-time across all attempts never exceeds the caller's
//     overall context deadline (each attempt's deadline is clamped to the
//     overall remaining).
//   - Retrying stops early, returning context.DeadlineExceeded, once the
//     overall deadline is exhausted and no further attempt can start.
//
// With no options RetryBudgeted is identical to Retry(p): additive and
// backward compatible. Supply WithPerAttemptTimeout and/or WithAttemptBudgeting
// to enable budgeting. The supplied policy is not mutated.
func (b *Builder) RetryBudgeted(p *retry.Policy, opts ...AttemptBudgetOption) *Builder {
	cfg := &attemptBudgetConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	// No budgeting requested: fall back to the plain retry stage so behaviour
	// (and observable ordering) is byte-for-byte identical to Retry(p).
	if cfg.perAttempt <= 0 && !cfg.divide {
		return b.Retry(p)
	}

	max := p.MaxAttempts
	if max <= 0 {
		max = 1
	}
	clk := cfg.clockOrReal()

	b.stages = append(b.stages, builderStage{kindRetry, func(ctx context.Context, fn func(context.Context) error) error {
		// attemptsRun counts how many times the wrapped fn has been invoked by
		// the policy, so the divide mode knows how many attempts remain.
		attemptsRun := 0

		wrapped := func(attemptCtx context.Context) error {
			attemptsRun++

			// Compute the per-attempt deadline. Start from "no derived
			// deadline" (dur == 0 means "inherit ctx unchanged").
			var dur time.Duration

			overallRemaining, hasOverall := remaining(ctx, clk)
			if hasOverall {
				// The overall deadline is already blown: stop before running
				// another attempt.
				if overallRemaining <= 0 {
					return context.DeadlineExceeded
				}
				dur = overallRemaining
			}

			if cfg.divide && hasOverall {
				attemptsLeft := max - (attemptsRun - 1)
				if attemptsLeft < 1 {
					attemptsLeft = 1
				}
				slice := overallRemaining / time.Duration(attemptsLeft)
				if slice > 0 && (dur == 0 || slice < dur) {
					dur = slice
				}
			}

			if cfg.perAttempt > 0 && (dur == 0 || cfg.perAttempt < dur) {
				dur = cfg.perAttempt
			}

			// dur == 0 means no derived deadline (no overall deadline and no
			// fixed per-attempt timeout applicable): run unchanged.
			if dur <= 0 {
				return fn(attemptCtx)
			}

			// Derive the deadline from the injected clock's notion of "now" so
			// the applied deadline value is consistent with the same clock used
			// to read the remaining budget. Under RealClock this is equivalent
			// to context.WithTimeout(attemptCtx, dur); under a ManualClock it
			// makes the per-attempt deadline deterministic and inspectable.
			attemptCtx, cancel := context.WithDeadline(attemptCtx, clk.Now().Add(dur))
			defer cancel()
			return fn(attemptCtx)
		}

		return p.Do(ctx, wrapped)
	}})
	return b
}

// remaining reports the time left until ctx's deadline using clk as the time
// source, and whether ctx carries a deadline at all. When ctx has no deadline
// it returns (0, false).
func remaining(ctx context.Context, clk clock.Clock) (time.Duration, bool) {
	dl, ok := ctx.Deadline()
	if !ok {
		return 0, false
	}
	return clk.Until(dl), true
}
