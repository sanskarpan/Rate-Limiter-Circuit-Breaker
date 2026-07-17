// Package pipeline provides a resilience pipeline builder that chains multiple
// resilience patterns in a fixed, production-correct order.
//
// Stage ordering (fixed and non-configurable). Build() sorts the configured
// stages into this canonical order regardless of the order the builder methods
// were called in, so the guarantee below always holds:
//
//  1. Rate limiter — fail fast before consuming any resources
//  2. Bulkhead     — control concurrency after rate check passes
//  3. Timeout      — start the clock after acquiring a worker slot
//  4. Circuit breaker — detect and stop cascading failures quickly
//  5. Retry        — retry only the innermost operation, not the whole pipeline
//  6. Custom (Use) — innermost, wraps the operation itself
//
// Why this order?
//   - Rate limit first: don't waste a bulkhead slot on a request you'll deny anyway.
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

	"github.com/sanskarpan/resilience/bulkhead"
	"github.com/sanskarpan/resilience/circuitbreaker"
	"github.com/sanskarpan/resilience/ratelimit"
	"github.com/sanskarpan/resilience/retry"
)

// stageKind is the canonical rank of a stage. Lower kinds run outermost.
type stageKind int

const (
	kindRateLimit stageKind = iota
	kindBulkhead
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
func New() *Builder {
	return &Builder{}
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

// Retry adds a retry stage using the provided policy.
func (b *Builder) Retry(p *retry.Policy) *Builder {
	b.stages = append(b.stages, builderStage{kindRetry, func(ctx context.Context, fn func(context.Context) error) error {
		return p.Do(ctx, fn)
	}})
	return b
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
// canonical sequence (rate limit → bulkhead → timeout → circuit breaker →
// retry → custom) regardless of the order the builder methods were called in.
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

// Is implements errors.Is for RateLimitError.
func (e *RateLimitError) Is(target error) bool {
	return target == ErrRateLimited
}
