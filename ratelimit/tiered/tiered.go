package tiered

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/metric"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
)

const algorithmName = "tiered"

// Compile-time assertion that TieredLimiter implements the Limiter interface.
var _ ratelimit.Limiter = (*TieredLimiter)(nil)

// KeyFunc derives a tier-specific key from the request key passed to the tiered
// limiter. For example a per-tenant tier might map "acme:alice" to "acme", and a
// global tier might map any key to a fixed constant.
type KeyFunc func(requestKey string) string

// Tier is one level of the hierarchy: it maps the request key to this tier's own
// key via KeyFunc and enforces the mapped key against Limiter. Tiers are ordered
// from most specific (e.g. per-user) to least specific (e.g. global).
type Tier struct {
	// Name is a human-readable label surfaced in denied Result metadata under
	// "denied_tier". Optional; defaults to the tier's index when empty.
	Name string

	// KeyFunc derives this tier's key from the request key. Required; a nil
	// KeyFunc is treated as the identity function (the request key is used as-is).
	KeyFunc KeyFunc

	// Limiter enforces the derived key. Required.
	Limiter ratelimit.Limiter
}

// Crediter is an optional interface an underlying limiter may implement to
// return previously consumed tokens for a key. When a tier's limiter satisfies
// Crediter, TieredLimiter uses it to roll back a partially committed chain if a
// later tier denies during the commit phase (a rare condition that only arises
// when the underlying limiter is also consumed directly outside this tiered
// limiter). Limiters that do not implement Crediter are rolled back on a
// best-effort basis only.
type Crediter interface {
	// Credit returns n tokens for key, clamped to the limiter's capacity. It is
	// the inverse of a successful AllowN(ctx, key, n).
	Credit(ctx context.Context, key string, n int) error
}

// TieredLimiter enforces an ordered chain of Tiers with all-or-nothing token
// accounting. It implements ratelimit.Limiter and is safe for concurrent use.
type TieredLimiter struct {
	tiers []Tier

	// mu serializes the Peek (check) phase and the AllowN (commit) phase of a
	// single AllowN call so they are atomic with respect to other operations that
	// go through this tiered limiter. Without it, concurrent callers could each
	// pass the Peek phase and then each commit against the early tiers even
	// though a later tier is out of capacity — leaking tokens from the early
	// tiers. This only serializes access routed through this limiter; if an
	// underlying limiter is also consumed directly elsewhere, cross-tier
	// atomicity is not guaranteed (and the phase-2 rollback path handles that
	// case best-effort).
	mu sync.Mutex

	// clock drives WaitN's retry timer. Defaults to the real clock; inject a
	// mock clock via WithClock so WaitN is deterministic under a manual clock.
	clock clock.Clock

	// rec records this limiter's own final decision under the "tiered" algorithm.
	// Underlying tier limiters keep their own recorders.
	rec metric.Recorder

	// onDecision, when non-nil, fires synchronously after every Allow/AllowN
	// decision. nil by default so the hot path is a single nil check.
	onDecision func(key string, r ratelimit.Result)
}

// Option configures a TieredLimiter.
type Option func(*TieredLimiter)

// WithClock sets the clock used by WaitN's retry timer. Use a ManualClock in
// tests so WaitN wakes deterministically when the clock is advanced. A nil clock
// is ignored.
func WithClock(clk clock.Clock) Option {
	return func(t *TieredLimiter) {
		if clk != nil {
			t.clock = clk
		}
	}
}

// WithRecorder wires a metric.Recorder so the tiered limiter's own allow/deny
// decision and decision latency are emitted under the "tiered" algorithm.
// Defaults to metric.Default() (a no-op) when unset. A nil recorder is ignored.
func WithRecorder(rec metric.Recorder) Option {
	return func(t *TieredLimiter) {
		if rec != nil {
			t.rec = rec
		}
	}
}

// WithOnDecision registers a hook fired after every Allow/AllowN decision (both
// allow and deny), receiving the request key and the final Result. It runs
// synchronously on the calling goroutine before the decision returns, so keep it
// fast and non-blocking. A nil hook is ignored.
func WithOnDecision(fn func(key string, r ratelimit.Result)) Option {
	return func(t *TieredLimiter) {
		if fn != nil {
			t.onDecision = fn
		}
	}
}

// New creates a TieredLimiter enforcing tiers in order, from most specific to
// least specific. It panics if any Tier has a nil Limiter, since such a tier
// can never produce a working decision. A nil KeyFunc on a Tier is silently
// replaced with the identity function (the request key is used as-is).
func New(tiers []Tier, opts ...Option) *TieredLimiter {
	t := &TieredLimiter{
		clock: clock.RealClock{},
		rec:   metric.Default(),
	}
	for _, tier := range tiers {
		if tier.Limiter == nil {
			panic(fmt.Sprintf("tiered.New: tier %q has a nil Limiter", tier.Name))
		}
		if tier.KeyFunc == nil {
			tier.KeyFunc = identity
		}
		t.tiers = append(t.tiers, tier)
	}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

// identity is the default KeyFunc: it returns the request key unchanged.
func identity(requestKey string) string { return requestKey }

// Constant returns a KeyFunc that always yields key, useful for a global tier
// that shares a single bucket across all requests.
func Constant(key string) KeyFunc {
	return func(string) string { return key }
}

// Prefix returns a KeyFunc that keeps the segment of the request key before the
// first occurrence of sep. For example Prefix(":")("acme:alice") == "acme".
// If sep is not present the whole request key is returned.
func Prefix(sep string) KeyFunc {
	return func(requestKey string) string {
		if i := strings.Index(requestKey, sep); i >= 0 {
			return requestKey[:i]
		}
		return requestKey
	}
}

// tierName returns the display name for the tier at index i.
func (t *TieredLimiter) tierName(i int) string {
	if t.tiers[i].Name != "" {
		return t.tiers[i].Name
	}
	return fmt.Sprintf("tier[%d]", i)
}

// Allow checks if 1 token is available across every tier. Non-blocking.
func (t *TieredLimiter) Allow(ctx context.Context, key string) ratelimit.Result {
	return t.AllowN(ctx, key, 1)
}

// AllowN checks if n tokens are available across every tier and consumes them
// all-or-nothing: either every tier is debited n tokens or none is. Non-blocking.
func (t *TieredLimiter) AllowN(ctx context.Context, key string, n int) (res ratelimit.Result) {
	start := t.clock.Now()
	defer func() {
		if n != 1 {
			setCost(&res, n)
		}
		t.record(res, start)
		if t.onDecision != nil {
			t.onDecision(key, res)
		}
	}()

	if err := ratelimit.ValidateKey(key); err != nil {
		return ratelimit.Result{Allowed: false, Algorithm: algorithmName}
	}
	if err := ratelimit.ValidateN(n); err != nil {
		return ratelimit.Result{Allowed: false, Algorithm: algorithmName}
	}
	if len(t.tiers) == 0 {
		return ratelimit.Result{Allowed: false, Algorithm: algorithmName}
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// Phase 1: Peek every tier (non-consuming). If any tier lacks capacity, deny
	// without touching any tier that would allow.
	keys := make([]string, len(t.tiers))
	peeks := make([]ratelimit.State, len(t.tiers))
	for i := range t.tiers {
		keys[i] = t.tiers[i].KeyFunc(key)
		peeks[i] = t.tiers[i].Limiter.Peek(ctx, keys[i])
		if peeks[i].Remaining < n {
			return t.denyResult(ctx, i, keys, peeks, n)
		}
	}

	// Phase 2: every tier reported capacity — commit by consuming from each.
	committed := make([]int, 0, len(t.tiers))
	results := make([]ratelimit.Result, len(t.tiers))
	for i := range t.tiers {
		results[i] = t.tiers[i].Limiter.AllowN(ctx, keys[i], n)
		if !results[i].Allowed {
			// A tier denied despite Peek reporting capacity. Under t.mu this can
			// only happen when the underlying limiter is also consumed directly,
			// outside this tiered limiter. Roll back every tier already committed
			// in this call so the all-or-nothing property holds, then deny.
			t.rollback(ctx, committed, keys, n)
			return t.denyFromResult(i, keys[i], results[i])
		}
		committed = append(committed, i)
	}

	return t.allowResult(results)
}

// rollback returns n tokens to each committed tier (in reverse commit order).
// Tiers whose limiter implements Crediter are credited exactly; others cannot be
// un-consumed (there is no generic un-consume primitive in the Limiter
// interface) and are left to refill naturally — a best-effort degradation for
// the rare direct-external-consumption case that reaches phase 2.
func (t *TieredLimiter) rollback(ctx context.Context, committed []int, keys []string, n int) {
	for j := len(committed) - 1; j >= 0; j-- {
		i := committed[j]
		if cr, ok := t.tiers[i].Limiter.(Crediter); ok {
			_ = cr.Credit(ctx, keys[i], n)
		}
	}
}

// allowResult builds the allowed Result, reporting the tier with the least
// remaining capacity so callers see the true bottleneck.
func (t *TieredLimiter) allowResult(results []ratelimit.Result) ratelimit.Result {
	out := results[0]
	for _, r := range results[1:] {
		if r.Remaining < out.Remaining {
			out.Remaining = r.Remaining
			out.Limit = r.Limit
		}
		if r.ResetAfter > out.ResetAfter {
			out.ResetAfter = r.ResetAfter
		}
	}
	out.Allowed = true
	out.RetryAfter = 0
	out.Algorithm = algorithmName
	return out
}

// denyResult builds a denied Result for the phase-1 case where tier deniedIdx
// lacked capacity. It sources an accurate RetryAfter from that tier by calling
// AllowN on it (a denied AllowN consumes nothing) and never touches the tiers
// that would allow. The denying tier's name and key are recorded in metadata.
func (t *TieredLimiter) denyResult(ctx context.Context, deniedIdx int, keys []string, peeks []ratelimit.State, n int) ratelimit.Result {
	// A denied AllowN does not consume, so this yields a clock-correct
	// RetryAfter/Remaining for the bottleneck tier without leaking tokens.
	r := t.tiers[deniedIdx].Limiter.AllowN(ctx, keys[deniedIdx], n)
	out := ratelimit.Result{
		Allowed:    false,
		Algorithm:  algorithmName,
		Limit:      r.Limit,
		Remaining:  r.Remaining,
		ResetAfter: r.ResetAfter,
		RetryAfter: r.RetryAfter,
	}
	if out.RetryAfter == 0 {
		// Fall back to the peeked state if the limiter did not populate RetryAfter.
		out.Limit = peeks[deniedIdx].Limit
		out.Remaining = peeks[deniedIdx].Remaining
	}
	setDeniedTier(&out, t.tierName(deniedIdx), keys[deniedIdx])
	return out
}

// denyFromResult builds a denied Result from an already-obtained denying tier
// result (the phase-2 anomaly path).
func (t *TieredLimiter) denyFromResult(deniedIdx int, deniedKey string, r ratelimit.Result) ratelimit.Result {
	out := r
	out.Allowed = false
	out.Algorithm = algorithmName
	setDeniedTier(&out, t.tierName(deniedIdx), deniedKey)
	return out
}

// Wait blocks until 1 token is available across every tier or ctx is cancelled.
func (t *TieredLimiter) Wait(ctx context.Context, key string) error {
	return t.WaitN(ctx, key, 1)
}

// WaitN blocks until n tokens are available across every tier or ctx is
// cancelled. It repeatedly attempts AllowN, sleeping for the returned RetryAfter
// between attempts. Because AllowN is all-or-nothing, no tier is left debited
// while waiting.
func (t *TieredLimiter) WaitN(ctx context.Context, key string, n int) error {
	if err := ratelimit.ValidateKey(key); err != nil {
		return err
	}
	if err := ratelimit.ValidateN(n); err != nil {
		return err
	}
	for {
		res := t.AllowN(ctx, key, n)
		if res.Allowed {
			return nil
		}
		wait := res.RetryAfter
		if wait <= 0 {
			wait = time.Millisecond
		}
		timer := t.clock.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return &ratelimit.RateLimitError{
				Algorithm:  algorithmName,
				Key:        key,
				Limit:      res.Limit,
				RetryAfter: wait,
				Err:        ratelimit.ErrContextDone,
			}
		case <-timer.C():
			// retry
		}
	}
}

// Peek returns the most restrictive state across all tiers without consuming.
func (t *TieredLimiter) Peek(ctx context.Context, key string) ratelimit.State {
	if len(t.tiers) == 0 {
		return ratelimit.State{Key: key, Algorithm: algorithmName}
	}
	most := ratelimit.State{}
	found := false
	for i := range t.tiers {
		tk := t.tiers[i].KeyFunc(key)
		s := t.tiers[i].Limiter.Peek(ctx, tk)
		if !found || s.Remaining < most.Remaining {
			most = s
			found = true
		}
	}
	most.Key = key
	most.Algorithm = algorithmName
	return most
}

// Reset resets state for the given key across every tier, using each tier's
// derived key. Errors from individual tiers are joined.
func (t *TieredLimiter) Reset(ctx context.Context, key string) error {
	var errs []error
	for i := range t.tiers {
		tk := t.tiers[i].KeyFunc(key)
		if err := t.tiers[i].Limiter.Reset(ctx, tk); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", t.tierName(i), err))
		}
	}
	return errors.Join(errs...)
}

// Close closes every underlying tier limiter. Errors are joined. Note that if
// the same limiter is shared across tiers it will be closed more than once;
// callers should avoid sharing limiter instances across tiers if that is a
// problem for the underlying implementation.
func (t *TieredLimiter) Close() error {
	var errs []error
	for i := range t.tiers {
		if err := t.tiers[i].Limiter.Close(); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", t.tierName(i), err))
		}
	}
	return errors.Join(errs...)
}

// record fires the configured metric.Recorder for a completed decision.
func (t *TieredLimiter) record(res ratelimit.Result, start time.Time) {
	if res.Allowed {
		t.rec.IncAllowed(algorithmName)
	} else {
		t.rec.IncDenied(algorithmName)
	}
	t.rec.ObserveDecision(algorithmName, t.clock.Now().Sub(start))
}

// setCost records the consumed cost in res.Metadata under "cost", allocating the
// map lazily so the n==1 hot path stays allocation-free.
func setCost(res *ratelimit.Result, cost int) {
	if res.Metadata == nil {
		res.Metadata = ratelimit.Metadata{}
	}
	res.Metadata["cost"] = cost
}

// setDeniedTier records which tier caused a denial in res.Metadata.
func setDeniedTier(res *ratelimit.Result, name, key string) {
	if res.Metadata == nil {
		res.Metadata = ratelimit.Metadata{}
	}
	res.Metadata["denied_tier"] = name
	res.Metadata["denied_key"] = key
}
