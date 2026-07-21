// Package pipeline provides a resilience pipeline builder that chains multiple
// resilience patterns in a fixed, production-correct order.
//
// Stage ordering (fixed and non-configurable). Build() sorts the configured
// stages into this canonical order regardless of the order the builder methods
// were called in, so the guarantee below always holds:
//
//  1. Load shedder — admission control, shed under overload before anything else
//  2. Rate limiter — fail fast before consuming any resources
//  3. Bulkhead     — control concurrency after rate check passes
//  4. Timeout      — start the clock after acquiring a worker slot
//  5. Circuit breaker — detect and stop cascading failures quickly
//  6. Retry        — retry only the innermost operation, not the whole pipeline
//  7. Custom (Use) — innermost, wraps the operation itself
//
// Why this order?
//   - Load shed first: it is admission control. Under overload we want to shed
//     (priority-aware) before spending any rate-limit accounting or slots on a
//     request we may drop anyway.
//   - Rate limit next: don't waste a bulkhead slot on a request you'll deny anyway.
//   - Bulkhead before timeout: don't start the timeout countdown while waiting for a slot.
//   - Timeout before CB: the CB should see real failures, not timeouts from slow queue drain.
//   - CB before retry: don't retry if the circuit is open — that would be wasted effort.
//   - Retry innermost: only retry the actual call, not the entire pipeline.
//
// Stages of the same kind keep their relative insertion order (the sort is
// stable), so adding two custom stages preserves the order you added them.
package pipeline

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/bulkhead"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/concurrency"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/loadshed"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/retry"
)

// stageKind is the canonical rank of a stage. Lower kinds run outermost.
type stageKind int

const (
	kindLoadShed stageKind = iota
	kindRateLimit
	kindBulkhead
	kindConcurrency
	kindTimeout
	kindCircuitBreaker
	kindRetry
	kindCustom
)

// KeyFunc extracts a rate-limit key from a context.
type KeyFunc func(ctx context.Context) string

// KeyByValue returns a KeyFunc that always returns the same fixed key.
// Useful for global rate limiting across all callers.
func KeyByValue(key string) KeyFunc {
	return func(ctx context.Context) string { return key }
}

// KeyFromContext returns a KeyFunc that reads a key from the context using the
// provided extractor. Falls back to defaultKey if the extractor returns "".
func KeyFromContext(extract func(ctx context.Context) string, defaultKey string) KeyFunc {
	return func(ctx context.Context) string {
		if k := extract(ctx); k != "" {
			return k
		}
		return defaultKey
	}
}

// Pipeline executes a function through a chain of resilience stages.
// A zero-value Pipeline (no stages configured) passes through directly.
type Pipeline struct {
	stages []stage
	kinds  []stageKind // canonical kind of each stage, parallel to stages (observability/tests)
}

// stage is a single middleware-style wrapper.
type stage func(ctx context.Context, fn func(context.Context) error) error

// builderStage pairs a stage with its canonical kind so Build can order them.
type builderStage struct {
	kind stageKind
	fn   stage
}

// Builder constructs a Pipeline using the fluent builder pattern.
type Builder struct {
	stages []builderStage
}

// New creates a new Pipeline builder.
//
// Zero value: the zero Builder is valid — &Builder{} (what New returns) is an
// empty pipeline; Build on it yields a pipeline that simply invokes the wrapped
// function with no resilience stages. Add stages fluently before Build.
func New() *Builder {
	return &Builder{}
}

// LoadShed adds a CoDel-style priority-aware load-shedding stage. It is the
// outermost stage (admission control): under sustained overload the shedder
// drops requests — lowest priority first — before any rate-limit accounting or
// resource acquisition happens. On a shed it returns ErrLoadShed; otherwise the
// downstream call's duration feeds the shedder's sojourn-time control loop.
//
// Attach a priority to the request context with loadshed.WithPriority so the
// shedder can protect high-priority work while shedding low-priority work.
func (b *Builder) LoadShed(s *loadshed.Shedder) *Builder {
	b.stages = append(b.stages, builderStage{kindLoadShed, func(ctx context.Context, fn func(context.Context) error) error {
		accept, done := s.Admit(ctx)
		if !accept {
			return ErrLoadShed
		}
		defer done()
		return fn(ctx)
	}})
	return b
}

// RateLimit adds a rate-limiting stage.
// keyFn is called on each Execute to derive the per-request key.
// Returns ErrRateLimited if the limiter denies the request.
func (b *Builder) RateLimit(l ratelimit.Limiter, keyFn KeyFunc) *Builder {
	b.stages = append(b.stages, builderStage{kindRateLimit, func(ctx context.Context, fn func(context.Context) error) error {
		key := ""
		if keyFn != nil {
			key = keyFn(ctx)
		}
		result := l.Allow(ctx, key)
		if !result.Allowed {
			return &RateLimitError{RetryAfter: result.RetryAfter}
		}
		return fn(ctx)
	}})
	return b
}

// Bulkhead adds a concurrency-limiting stage.
// maxConcurrency is the maximum number of concurrent executions.
// maxWait is how long to wait for a slot (0 = non-blocking).
func (b *Builder) Bulkhead(maxConcurrency int, maxWait time.Duration) *Builder {
	bh := bulkhead.New(maxConcurrency, maxWait)
	b.stages = append(b.stages, builderStage{kindBulkhead, func(ctx context.Context, fn func(context.Context) error) error {
		return bh.Execute(ctx, fn)
	}})
	return b
}

// BulkheadWith adds a concurrency-limiting stage using a pre-configured Bulkhead.
func (b *Builder) BulkheadWith(bh *bulkhead.Bulkhead) *Builder {
	b.stages = append(b.stages, builderStage{kindBulkhead, func(ctx context.Context, fn func(context.Context) error) error {
		return bh.Execute(ctx, fn)
	}})
	return b
}

// Concurrency adds an adaptive concurrency-limiting stage (Netflix-style). It
// runs between Bulkhead and Timeout: if the limiter is at its current limit the
// request is shed with ErrConcurrencyLimited; otherwise the call's latency and
// outcome feed the limiter's adaptive algorithm. See package concurrency.
func (b *Builder) Concurrency(lim *concurrency.Limiter) *Builder {
	b.stages = append(b.stages, builderStage{kindConcurrency, func(ctx context.Context, fn func(context.Context) error) error {
		release, ok := lim.Acquire(ctx)
		if !ok {
			return ErrConcurrencyLimited
		}
		start := time.Now()
		err := fn(ctx)
		release(concurrency.Outcome{RTT: time.Since(start), Dropped: err != nil})
		return err
	}})
	return b
}

// Timeout adds a timeout stage. If d <= 0, no timeout is applied.
func (b *Builder) Timeout(d time.Duration) *Builder {
	if d <= 0 {
		return b
	}
	b.stages = append(b.stages, builderStage{kindTimeout, func(ctx context.Context, fn func(context.Context) error) error {
		ctx, cancel := context.WithTimeout(ctx, d)
		defer cancel()
		return fn(ctx)
	}})
	return b
}

// CircuitBreaker adds a circuit breaker stage.
func (b *Builder) CircuitBreaker(cb *circuitbreaker.CircuitBreaker) *Builder {
	b.stages = append(b.stages, builderStage{kindCircuitBreaker, func(ctx context.Context, fn func(context.Context) error) error {
		return cb.Execute(ctx, fn)
	}})
	return b
}

// Retry adds a retry stage using the provided policy. If the policy already has
// a Budget attached (p.Budget != nil), that shared retry budget is honoured, so
// callers can retry-storm-guard the pipeline simply by passing a
// budget-configured Policy.
func (b *Builder) Retry(p *retry.Policy) *Builder {
	b.stages = append(b.stages, builderStage{kindRetry, func(ctx context.Context, fn func(context.Context) error) error {
		return p.Do(ctx, fn)
	}})
	return b
}

// RetryWithBudget adds a retry stage that shares the given retry budget
// (retry-storm guard). The supplied policy is not mutated: a shallow copy with
// budget attached is used, so the same *retry.Budget can be shared across
// several pipelines/stages to cap their aggregate retry rate. A nil budget is
// equivalent to Retry(p).
func (b *Builder) RetryWithBudget(p *retry.Policy, budget *retry.Budget) *Builder {
	if budget == nil {
		return b.Retry(p)
	}
	pc := *p
	pc.Budget = budget
	return b.Retry(&pc)
}

// Use adds a custom stage. The stage wraps the downstream fn in any way.
// This is an escape hatch for patterns not covered by the builder. Custom
// stages run innermost (after retry), closest to the operation itself, and
// keep their relative insertion order.
func (b *Builder) Use(s func(ctx context.Context, fn func(context.Context) error) error) *Builder {
	b.stages = append(b.stages, builderStage{kindCustom, s})
	return b
}

// Build constructs the Pipeline, ordering the configured stages into the fixed
// canonical sequence (load shed → rate limit → bulkhead → timeout → circuit
// breaker → retry → custom) regardless of the order the builder methods were
// called in.
// The sort is stable, so stages of the same kind keep their insertion order.
func (b *Builder) Build() *Pipeline {
	ordered := make([]builderStage, len(b.stages))
	copy(ordered, b.stages)
	sort.SliceStable(ordered, func(i, j int) bool {
		return ordered[i].kind < ordered[j].kind
	})
	stages := make([]stage, len(ordered))
	kinds := make([]stageKind, len(ordered))
	for i, s := range ordered {
		stages[i] = s.fn
		kinds[i] = s.kind
	}
	return &Pipeline{stages: stages, kinds: kinds}
}

// Execute runs fn through all configured stages in order.
func (p *Pipeline) Execute(ctx context.Context, fn func(context.Context) error) error {
	return chain(p.stages, 0, ctx, fn)
}

// chain recursively calls stages[i] with the remaining stages as the wrapped fn.
func chain(stages []stage, i int, ctx context.Context, fn func(context.Context) error) error {
	if i >= len(stages) {
		return fn(ctx)
	}
	return stages[i](ctx, func(ctx context.Context) error {
		return chain(stages, i+1, ctx, fn)
	})
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

// RateLimitError is returned when the rate limiter denies a request.
type RateLimitError struct {
	// RetryAfter is the suggested wait before retrying. Zero means the rate
	// limiter did not provide a hint.
	RetryAfter time.Duration
}

func (e *RateLimitError) Error() string {
	if e.RetryAfter > 0 {
		return "pipeline: rate limited, retry after " + e.RetryAfter.String()
	}
	return "pipeline: rate limited"
}

// ErrRateLimited is a sentinel that callers can use with errors.Is.
var ErrRateLimited = errors.New("pipeline: rate limited")

// ErrConcurrencyLimited is returned when the adaptive concurrency stage sheds a
// request because it is at its current in-flight limit.
var ErrConcurrencyLimited = errors.New("pipeline: concurrency limited")

// ErrLoadShed is returned when the CoDel load-shedding stage sheds a request
// under overload (its priority was below the current dynamic drop threshold).
var ErrLoadShed = errors.New("pipeline: load shed")

// Unwrap returns ErrRateLimited so errors.Is(err, ErrRateLimited) matches a *RateLimitError,
// following the same pattern as BulkheadError and TimeoutError in this package.
func (e *RateLimitError) Unwrap() error { return ErrRateLimited }
