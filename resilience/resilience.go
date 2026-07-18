package resilience

import (
	"context"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/bulkhead"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/pipeline"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/retry"
)

// KeyFunc extracts a per-request rate-limit key from a context. It is a
// re-export of pipeline.KeyFunc so callers configuring the rate-limit layer do
// not need to import the pipeline package as well.
type KeyFunc = pipeline.KeyFunc

// KeyByValue returns a KeyFunc that always returns the same fixed key, for
// global (all-callers) rate limiting. It is a re-export of pipeline.KeyByValue.
func KeyByValue(key string) KeyFunc { return pipeline.KeyByValue(key) }

// KeyFromContext returns a KeyFunc that reads a key from the context using
// extract, falling back to defaultKey when extract returns "". It is a
// re-export of pipeline.KeyFromContext.
func KeyFromContext(extract func(ctx context.Context) string, defaultKey string) KeyFunc {
	return pipeline.KeyFromContext(extract, defaultKey)
}

// Stack is an immutable, ready-to-run resilience stack produced by a Builder.
// It composes rate limiting, circuit breaking, retry (with an optional shared
// budget), bulkhead concurrency limiting, timeout, and fallback into a single
// executable unit. A Stack is safe for concurrent use.
//
// # Layer wrapping order (outermost first)
//
// Regardless of the order the builder methods are called in, Build assembles
// the layers into this fixed, production-correct order (outermost wraps
// innermost):
//
//  1. Fallback      — outermost. Catches an error from ANY inner layer
//     (including a rate-limit denial, an open circuit, a bulkhead rejection,
//     a timeout, or the operation itself) and resolves a substitute result.
//  2. Rate limiter  — fail fast before consuming any resources.
//  3. Bulkhead      — bound concurrency once the rate check passes.
//  4. Timeout       — start the deadline clock only after acquiring a slot.
//  5. Circuit breaker — trip on real downstream failures, not queue-drain waits.
//  6. Retry         — retry only the innermost operation, honouring any budget.
//  7. Operation     — the func(ctx) you pass to Execute.
//
// Why this order (layers 2–6 mirror pipeline.Pipeline, whose rationale this
// builder reuses verbatim):
//
//   - Fallback outermost so it is the single place that turns any failure the
//     stack produces into a graceful degraded result. Putting it inside the
//     rate limiter, for instance, would let limiter denials escape unhandled.
//   - Rate limit before bulkhead: don't spend a concurrency slot on a request
//     you will deny anyway.
//   - Bulkhead before timeout: don't start the timeout countdown while a caller
//     is still queued waiting for a slot.
//   - Timeout before circuit breaker: the breaker should observe genuine
//     failures, not timeouts induced by slow queue drain.
//   - Circuit breaker before retry: if the circuit is open, reject immediately
//     rather than burning retry attempts.
//   - Retry innermost: retry only the actual operation, never the whole stack
//     (which would re-run rate limiting, re-acquire slots, etc.).
type Stack struct {
	pipe     *pipeline.Pipeline
	fallback func(context.Context, error) error
}

// Execute runs fn through every configured layer in the documented order and
// returns the resulting error. A Stack with no layers configured passes fn
// through directly. It is safe to call concurrently.
//
// The error returned is the error produced by the innermost layer that failed,
// propagated verbatim through the outer layers unless a fallback resolves it —
// so sentinel checks keep working, e.g. errors.Is(err, pipeline.ErrRateLimited),
// errors.Is(err, circuitbreaker.ErrCircuitOpen),
// errors.Is(err, bulkhead.ErrBulkheadFull), or
// errors.Is(err, context.DeadlineExceeded) from the timeout layer.
func (s *Stack) Execute(ctx context.Context, fn func(context.Context) error) error {
	if s.fallback != nil {
		if err := s.runCore(ctx, fn); err != nil {
			return s.fallback(ctx, err)
		}
		return nil
	}
	return s.runCore(ctx, fn)
}

// runCore executes fn through the ordered core pipeline (layers 2–6).
func (s *Stack) runCore(ctx context.Context, fn func(context.Context) error) error {
	if s.pipe == nil {
		return fn(ctx)
	}
	return s.pipe.Execute(ctx, fn)
}

// Execute runs fn through stack and returns fn's typed result. It is the
// generic, value-returning counterpart of Stack.Execute for operations that
// produce a value; it mirrors the resiliencex helpers.
//
// The core layers (rate limit → bulkhead → timeout → circuit breaker → retry)
// run exactly as they do for Stack.Execute. This entry point does NOT apply the
// func(ctx) error fallback configured with WithFallback (whose signature cannot
// produce a typed value); to also recover a typed value on failure, use
// ExecuteWithFallback. On any error the returned value is the zero value of T;
// the value is meaningful only when err is nil, and the error is propagated
// verbatim so sentinel checks keep working.
//
// Because Go methods cannot take type parameters, Execute is a package-level
// function rather than a method on Stack (a small, documented asymmetry that
// mirrors resiliencex and the retry/timeout/fallback DoWithResult helpers).
func Execute[T any](ctx context.Context, s *Stack, fn func(context.Context) (T, error)) (T, error) {
	var result T
	err := s.runCore(ctx, func(ctx context.Context) error {
		var e error
		result, e = fn(ctx)
		return e
	})
	if err != nil {
		var zero T
		return zero, err
	}
	return result, nil
}

// ExecuteWithFallback runs fn through stack's core layers and, if any layer (a
// rate-limit denial, an open circuit, a bulkhead rejection, a timeout, or fn
// itself) fails, resolves a typed substitute value via fb.
//
// It is the typed, value-returning analogue of a WithFallback-configured
// Stack.Execute: fb receives the verbatim error from the core layers and its
// (value, error) result is returned. When fb itself returns a non-nil error the
// returned value is the zero value of T and that error is propagated. The
// stack's WithFallback (the func(ctx) error fallback) is intentionally ignored
// here so the two fallback paths never double-fire.
func ExecuteWithFallback[T any](
	ctx context.Context,
	s *Stack,
	fn func(context.Context) (T, error),
	fb func(context.Context, error) (T, error),
) (T, error) {
	result, err := Execute(ctx, s, fn)
	if err != nil {
		fbResult, fbErr := fb(ctx, err)
		if fbErr != nil {
			var zero T
			return zero, fbErr
		}
		return fbResult, nil
	}
	return result, nil
}

// Builder assembles a resilience Stack using a fluent, functional-options-style
// API. Every configuration method returns the Builder so calls chain; the order
// of the calls does not matter because Build sorts the layers into the fixed
// canonical order documented on Stack. The zero value is not usable; start with
// New.
//
// # Constructor-option parity
//
// The core primitives this builder composes historically exposed inconsistent
// constructors (tokenbucket.New(cap, rate, opts...), gcra.New(limit, burst,
// window, opts...), circuitbreaker.New(Config{...}), bulkhead.New(max, wait,
// opts...), retry.New(opts...)). This builder gives them one consistent
// With<Layer> vocabulary so composing a stack no longer requires remembering
// each primitive's positional-argument shape. You still construct each
// primitive with its own (already type-safe) constructor and hand the ready
// value to the matching With method — the builder does not hide the primitives,
// it unifies how they are wired together.
type Builder struct {
	pb *pipeline.Builder

	hasRateLimit bool
	hasBulkhead  bool
	hasTimeout   bool
	hasBreaker   bool
	hasRetry     bool

	fallback func(context.Context, error) error
}

// New returns a new, empty Builder.
func New() *Builder {
	return &Builder{pb: pipeline.New()}
}

// WithRateLimit adds the rate-limiting layer. keyFn derives the per-request key
// on each Execute (use KeyByValue for a global limit). When the limiter denies a
// request, Execute returns a *pipeline.RateLimitError (errors.Is-matchable
// against pipeline.ErrRateLimited). Calling it more than once keeps only stages
// of distinct kinds ordered; a second rate-limit layer is applied in insertion
// order after the first.
func (b *Builder) WithRateLimit(l ratelimit.Limiter, keyFn KeyFunc) *Builder {
	b.pb.RateLimit(l, keyFn)
	b.hasRateLimit = true
	return b
}

// WithBulkhead adds a concurrency-limiting layer backed by a pre-constructed
// *bulkhead.Bulkhead, so callers control its name, wait timeout, and recorder.
// When the bulkhead is saturated Execute returns a *bulkhead.BulkheadError
// (errors.Is-matchable against bulkhead.ErrBulkheadFull).
func (b *Builder) WithBulkhead(bh *bulkhead.Bulkhead) *Builder {
	b.pb.BulkheadWith(bh)
	b.hasBulkhead = true
	return b
}

// WithBulkheadLimit is a convenience that constructs a bulkhead with the given
// maximum concurrency and slot-acquisition wait (0 = non-blocking) and adds it
// as the concurrency layer. Use WithBulkhead for full control.
func (b *Builder) WithBulkheadLimit(maxConcurrency int, maxWait time.Duration) *Builder {
	b.pb.Bulkhead(maxConcurrency, maxWait)
	b.hasBulkhead = true
	return b
}

// WithTimeout adds a per-operation timeout layer. A non-positive d is a no-op
// (no timeout layer is added). On expiry the operation's context is cancelled
// and the timeout surfaces as context.DeadlineExceeded.
func (b *Builder) WithTimeout(d time.Duration) *Builder {
	if d > 0 {
		b.pb.Timeout(d)
		b.hasTimeout = true
	}
	return b
}

// WithCircuitBreaker adds the circuit-breaker layer backed by a pre-constructed
// *circuitbreaker.CircuitBreaker. When the circuit is open Execute returns a
// *circuitbreaker.CircuitError (errors.Is-matchable against
// circuitbreaker.ErrCircuitOpen / ErrTooManyRequests).
func (b *Builder) WithCircuitBreaker(cb *circuitbreaker.CircuitBreaker) *Builder {
	b.pb.CircuitBreaker(cb)
	b.hasBreaker = true
	return b
}

// WithRetry adds the retry layer using the given policy. If the policy already
// carries a Budget (p.Budget != nil) that shared retry-storm guard is honoured.
func (b *Builder) WithRetry(p *retry.Policy) *Builder {
	b.pb.Retry(p)
	b.hasRetry = true
	return b
}

// WithRetryBudget adds the retry layer sharing an explicit retry Budget
// (retry-storm guard) without mutating p: a shallow copy with the budget
// attached is used, so one *retry.Budget can be shared across several stacks to
// cap their aggregate retry rate. A nil budget is equivalent to WithRetry(p).
func (b *Builder) WithRetryBudget(p *retry.Policy, budget *retry.Budget) *Builder {
	b.pb.RetryWithBudget(p, budget)
	b.hasRetry = true
	return b
}

// WithFallback adds the outermost fallback layer. fb is invoked with the error
// produced by any inner layer (a rate-limit denial, an open circuit, a bulkhead
// rejection, a timeout, or the operation) and its result becomes the outcome of
// Execute. fb is never called when the inner layers succeed. A nil fb clears any
// previously configured fallback.
//
// WithFallback governs the func(ctx) error path (Stack.Execute). The generic
// value-returning path applies a fallback only through the package-level
// ExecuteWithFallback helper (which takes a typed fallback explicitly); the
// plain Execute[T] never invokes this fallback.
func (b *Builder) WithFallback(fb func(context.Context, error) error) *Builder {
	b.fallback = fb
	return b
}

// WithCustom adds a custom innermost layer, an escape hatch for behaviour the
// builder does not model directly. Custom layers run innermost (just around the
// operation, inside retry) and preserve their relative insertion order. It
// delegates to pipeline.Builder.Use.
func (b *Builder) WithCustom(s func(ctx context.Context, fn func(context.Context) error) error) *Builder {
	b.pb.Use(s)
	return b
}

// Build finalises the configuration into an immutable Stack, ordering the core
// layers into the canonical sequence documented on Stack. It may be called once;
// the returned Stack is safe for concurrent use. Continuing to configure the
// Builder after Build does not affect an already-built Stack.
func (b *Builder) Build() *Stack {
	return &Stack{
		pipe:     b.pb.Build(),
		fallback: b.fallback,
	}
}

// Layers reports which core layers are configured, in outermost-to-innermost
// order, as human-readable names. It is primarily an introspection/observability
// aid (and lets tests assert the composed shape without executing it). The
// fallback layer, when configured, appears first.
func (b *Builder) Layers() []string {
	var out []string
	if b.fallback != nil {
		out = append(out, "fallback")
	}
	if b.hasRateLimit {
		out = append(out, "ratelimit")
	}
	if b.hasBulkhead {
		out = append(out, "bulkhead")
	}
	if b.hasTimeout {
		out = append(out, "timeout")
	}
	if b.hasBreaker {
		out = append(out, "circuitbreaker")
	}
	if b.hasRetry {
		out = append(out, "retry")
	}
	return out
}
