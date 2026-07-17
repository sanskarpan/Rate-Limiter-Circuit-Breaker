// Package composite implements a composite rate limiter that combines multiple
// Limiter implementations using AND or OR logic.
//
// AND mode: All limiters must allow the request. Uses a two-phase approach:
//  1. Check all limiters (non-consuming)
//  2. Only consume from all if all would allow
//
// The two phases are serialized by an internal mutex so they are atomic with
// respect to other operations on the same composite. This prevents token loss:
// if limiter A would allow but limiter B would deny, A's token is NOT consumed,
// even under concurrent callers.
//
// OR mode: the first limiter to allow the request wins; limiters after it are
// not consulted, so their tokens are not consumed.
//
// All methods are safe for concurrent use.
package composite

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/metric"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
)

const (
	algorithmAND = "composite_and"
	algorithmOR  = "composite_or"
)

// Mode determines how multiple limiters are combined.
type Mode int

const (
	// AND requires all limiters to allow the request.
	AND Mode = iota
	// OR allows the request if any limiter allows it.
	OR
)

// CompositeLimiter combines multiple Limiter implementations.
// All methods are safe for concurrent use.
type CompositeLimiter struct {
	limiters []ratelimit.Limiter
	mode     Mode

	// mu serializes AND-mode check-then-consume so that the two phases are
	// atomic with respect to other operations on this composite. Without it,
	// concurrent callers can all pass the Peek phase and then each consume from
	// the early limiters even though a later limiter denies — leaking tokens
	// from the early limiters (the request is reported denied yet tokens were
	// spent). See the C-5 regression test. Note: this only serializes access
	// that goes *through this composite*; if the underlying limiters are also
	// consumed directly elsewhere, atomicity across the chain is not guaranteed.
	mu sync.Mutex

	// clock drives WaitN's retry timer. It defaults to the real clock; inject a
	// mock clock (WithClock) so WaitN is deterministic under a manual clock —
	// otherwise WaitN would sleep on real wall time while the underlying limiters
	// advance on the injected clock, and never observe refilled tokens (F-2).
	clock clock.Clock

	// rec records the composite's own final decision under composite_and /
	// composite_or. The underlying limiters keep their own recorders (default
	// Nop) so wiring is opt-in per layer.
	rec metric.Recorder

	// onDecision, when non-nil, is fired synchronously after every Allow/AllowN
	// decision. nil by default so the hot path stays a single nil check.
	onDecision func(key string, r ratelimit.Result)
}

// New creates a CompositeLimiter combining the given limiters.
func New(mode Mode, limiters ...ratelimit.Limiter) *CompositeLimiter {
	return &CompositeLimiter{
		limiters: limiters,
		mode:     mode,
		clock:    clock.RealClock{},
		rec:      metric.Default(),
	}
}

// WithClock sets the clock used by WaitN's retry timer and returns the limiter
// for chaining. Use a ManualClock in tests so WaitN wakes deterministically when
// the clock is advanced. Not safe to call concurrently with WaitN.
func (c *CompositeLimiter) WithClock(clk clock.Clock) *CompositeLimiter {
	c.clock = clk
	return c
}

// WithRecorder wires a metric.Recorder so the composite's own allow/deny
// decision and decision latency are emitted under composite_and/composite_or.
// Defaults to metric.Default() (a no-op) when unset. Returns the limiter for
// chaining. A nil recorder is ignored. Not safe to call concurrently with
// AllowN.
func (c *CompositeLimiter) WithRecorder(rec metric.Recorder) *CompositeLimiter {
	if rec != nil {
		c.rec = rec
	}
	return c
}

// WithOnDecision registers a hook fired after every Allow/AllowN decision (both
// allow and deny), receiving the key and the composite's final Result. The
// default is nil (a cheap no-op guarded by a nil check on the hot path).
// Returns the limiter for chaining. The hook runs synchronously on the calling
// goroutine before the decision is returned, so keep it fast and non-blocking.
// A nil hook is ignored. Not safe to call concurrently with AllowN.
func (c *CompositeLimiter) WithOnDecision(fn func(key string, r ratelimit.Result)) *CompositeLimiter {
	if fn != nil {
		c.onDecision = fn
	}
	return c
}

// record fires the configured metric.Recorder for a completed decision. With
// the default Nop recorder every call is an empty inlined method, so this stays
// allocation-free on the hot path.
func (c *CompositeLimiter) record(res ratelimit.Result, start time.Time) {
	if res.Allowed {
		c.rec.IncAllowed(c.algorithm())
	} else {
		c.rec.IncDenied(c.algorithm())
	}
	c.rec.ObserveDecision(c.algorithm(), c.clock.Now().Sub(start))
}

// setCost records the consumed cost in res.Metadata under the "cost" key,
// allocating the map lazily so the n==1 hot path stays allocation-free.
func setCost(res *ratelimit.Result, cost int) {
	if res.Metadata == nil {
		res.Metadata = ratelimit.Metadata{}
	}
	res.Metadata["cost"] = cost
}

// Allow checks if 1 request is allowed. Non-blocking.
func (c *CompositeLimiter) Allow(ctx context.Context, key string) ratelimit.Result {
	return c.AllowN(ctx, key, 1)
}

// AllowN checks if n requests are allowed. Non-blocking.
// AND mode: two-phase check then consume — atomic across all limiters.
// OR mode: first allow wins.
func (c *CompositeLimiter) AllowN(ctx context.Context, key string, n int) (res ratelimit.Result) {
	start := c.clock.Now()
	defer func() {
		if n != 1 {
			setCost(&res, n)
		}
		c.record(res, start)
		if c.onDecision != nil {
			c.onDecision(key, res)
		}
	}()

	if len(c.limiters) == 0 {
		return ratelimit.Result{
			Allowed:   false,
			Algorithm: c.algorithm(),
		}
	}
	switch c.mode {
	case AND:
		return c.allowAND(ctx, key, n)
	case OR:
		return c.allowOR(ctx, key, n)
	default:
		return c.allowAND(ctx, key, n)
	}
}

// allowAND implements two-phase check-then-consume for AND semantics.
// Phase 1: Peek all limiters (non-consuming)
// Phase 2: If all would allow, consume from all
// Phase 3: If any denies, return deny without consuming from the *allowing*
// limiters.
//
// The whole operation is serialized by c.mu so the Peek phase and the consume
// phase are atomic with respect to other operations on this composite. This is
// what makes AND mode leak-free (C-5): once Peek reports every limiter has
// capacity, no concurrent composite operation can consume it out from under us
// before phase 2 runs.
func (c *CompositeLimiter) allowAND(ctx context.Context, key string, n int) ratelimit.Result {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Phase 1: Check all via Peek (non-consuming).
	peeks := make([]ratelimit.State, len(c.limiters))
	denied := false
	for i, l := range c.limiters {
		peeks[i] = l.Peek(ctx, key)
		if peeks[i].Remaining < n {
			denied = true
		}
	}

	if denied {
		// Return deny WITHOUT consuming from any limiter. We obtain an accurate,
		// clock-correct RetryAfter/Remaining by calling AllowN only on the
		// limiters that would deny — a denied AllowN consumes nothing — and never
		// touch the limiters that would allow (calling AllowN on those would
		// consume and leak their tokens, which is exactly the C-5 bug).
		return c.denyResultAND(ctx, key, n, peeks)
	}

	// Phase 2: All would allow — consume from all.
	results := make([]ratelimit.Result, len(c.limiters))
	for i, l := range c.limiters {
		results[i] = l.AllowN(ctx, key, n)
		if !results[i].Allowed {
			// A limiter denied despite Peek reporting capacity. Under c.mu this can
			// only happen if the limiter is also consumed directly outside this
			// composite. Best effort: report the most restrictive result. The
			// already-consumed earlier limiters will refill naturally.
			return mostRestrictive(results[:i+1], c.algorithm())
		}
	}

	return leastRemaining(results, c.algorithm())
}

// denyResultAND builds a denied Result for AND mode with an accurate RetryAfter,
// sourced from the limiters that would deny (a denied AllowN does not consume).
func (c *CompositeLimiter) denyResultAND(ctx context.Context, key string, n int, peeks []ratelimit.State) ratelimit.Result {
	out := ratelimit.Result{
		Allowed:   false,
		Algorithm: c.algorithm(),
		Remaining: peeks[0].Remaining,
		Limit:     peeks[0].Limit,
	}
	for i, l := range c.limiters {
		if peeks[i].Remaining >= n {
			continue // this limiter would allow — do NOT call AllowN (would consume)
		}
		r := l.AllowN(ctx, key, n) // denies for the bottleneck limiter; no consumption
		if r.RetryAfter > out.RetryAfter {
			out.RetryAfter = r.RetryAfter
		}
		if r.Remaining < out.Remaining {
			out.Remaining = r.Remaining
			out.Limit = r.Limit
		}
	}
	return out
}

// allowOR implements OR semantics: the first limiter to allow wins, and no
// further limiters are consulted (so no tokens are consumed from later
// limiters). If all deny, the returned RetryAfter is the *shortest* across the
// limiters — this is intentional and differs from AND's "most restrictive"
// rule: under OR the caller may proceed as soon as *any* limiter would allow,
// so the soonest-available limiter is the relevant one to wait for.
func (c *CompositeLimiter) allowOR(ctx context.Context, key string, n int) ratelimit.Result {
	results := make([]ratelimit.Result, len(c.limiters))
	for i, l := range c.limiters {
		results[i] = l.AllowN(ctx, key, n)
		if results[i].Allowed {
			results[i].Algorithm = c.algorithm()
			return results[i]
		}
	}
	// All denied — return most lenient (shortest RetryAfter); see doc above.
	return shortestRetry(results, c.algorithm())
}

// Wait blocks until 1 request is allowed or ctx is cancelled.
func (c *CompositeLimiter) Wait(ctx context.Context, key string) error {
	return c.WaitN(ctx, key, 1)
}

// WaitN blocks until n requests are allowed or ctx is cancelled.
func (c *CompositeLimiter) WaitN(ctx context.Context, key string, n int) error {
	for {
		result := c.AllowN(ctx, key, n)
		if result.Allowed {
			return nil
		}
		wait := result.RetryAfter
		if wait <= 0 {
			wait = time.Millisecond
		}
		timer := c.clock.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return &ratelimit.RateLimitError{
				Algorithm:  c.algorithm(),
				Key:        key,
				RetryAfter: wait,
				Err:        ratelimit.ErrContextDone,
			}
		case <-timer.C():
			// retry
		}
	}
}

// Peek returns current state across all limiters without consuming.
// Returns the most restrictive state.
func (c *CompositeLimiter) Peek(ctx context.Context, key string) ratelimit.State {
	if len(c.limiters) == 0 {
		return ratelimit.State{Key: key, Algorithm: c.algorithm()}
	}
	states := make([]ratelimit.State, len(c.limiters))
	for i, l := range c.limiters {
		states[i] = l.Peek(ctx, key)
	}
	// Return most restrictive state (smallest Remaining)
	most := states[0]
	for _, s := range states[1:] {
		if s.Remaining < most.Remaining {
			most = s
		}
	}
	most.Algorithm = c.algorithm()
	return most
}

// Reset resets all state for the given key across all limiters.
func (c *CompositeLimiter) Reset(ctx context.Context, key string) error {
	var errs []string
	for _, l := range c.limiters {
		if err := l.Reset(ctx, key); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("composite reset errors: %s", strings.Join(errs, "; "))
	}
	return nil
}

// Close closes all underlying limiters.
func (c *CompositeLimiter) Close() error {
	var errs []string
	for _, l := range c.limiters {
		if err := l.Close(); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("composite close errors: %s", strings.Join(errs, "; "))
	}
	return nil
}

// algorithm returns the algorithm name based on mode.
func (c *CompositeLimiter) algorithm() string {
	if c.mode == OR {
		return algorithmOR
	}
	return algorithmAND
}

// mostRestrictive returns the result with the longest RetryAfter and smallest Remaining.
func mostRestrictive(results []ratelimit.Result, algo string) ratelimit.Result {
	worst := results[0]
	for _, r := range results[1:] {
		if r.RetryAfter > worst.RetryAfter {
			worst.RetryAfter = r.RetryAfter
		}
		if r.Remaining < worst.Remaining {
			worst.Remaining = r.Remaining
		}
	}
	worst.Allowed = false
	worst.Algorithm = algo
	return worst
}

// leastRemaining returns the result with smallest Remaining (most restrictive allowed result).
func leastRemaining(results []ratelimit.Result, algo string) ratelimit.Result {
	least := results[0]
	for _, r := range results[1:] {
		if r.Remaining < least.Remaining {
			least = r
		}
	}
	least.Algorithm = algo
	return least
}

// shortestRetry returns the result with the shortest RetryAfter (most optimistic denied result).
func shortestRetry(results []ratelimit.Result, algo string) ratelimit.Result {
	shortest := results[0]
	for _, r := range results[1:] {
		if r.RetryAfter < shortest.RetryAfter {
			shortest = r
		}
	}
	shortest.Algorithm = algo
	return shortest
}
