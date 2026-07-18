// Package resilience provides a single, discoverable fluent builder that
// composes the toolkit's resilience primitives — rate limiting, circuit
// breaking, retry (with an optional shared budget), bulkhead concurrency
// limiting, timeout, and fallback — into one executable Stack.
//
// It is a thin, ergonomic facade over the lower-level primitives and the
// existing pipeline.Builder: the core layers are assembled through a
// pipeline.Pipeline (whose fixed, production-correct stage ordering this package
// reuses), and fallback is wrapped around the whole stack as the outermost
// layer. The facade complements pipeline.Builder rather than replacing it — reach
// for pipeline.Builder when you need load-shedding, adaptive concurrency, or
// per-attempt deadline budgeting stages; reach for resilience.Builder when you
// want the common rate-limit → breaker → retry → bulkhead → timeout → fallback
// stack behind one uniform With<Layer> vocabulary and a generic Execute[T].
//
// # Layer wrapping order
//
// Build assembles the layers into a fixed order regardless of the order the
// builder methods are called in (outermost wraps innermost):
//
//	Fallback → Rate limiter → Bulkhead → Timeout → Circuit breaker → Retry → Operation
//
// The rationale for each boundary is documented in detail on the Stack type.
//
// # Constructor-option parity
//
// Each wrapped primitive historically exposed a differently-shaped constructor
// (positional args differing in count and meaning). This builder unifies how
// they are wired together behind a consistent With<Layer> API — you still build
// each primitive with its own type-safe constructor, but you compose them
// uniformly. The audit of which primitives expose functional options is
// summarised on the Builder type.
//
// # Errors
//
// Every layer's error is propagated verbatim through the outer layers (unless a
// fallback resolves it), so sentinel checks keep working end to end:
// errors.Is(err, pipeline.ErrRateLimited), errors.Is(err,
// circuitbreaker.ErrCircuitOpen), errors.Is(err, bulkhead.ErrBulkheadFull), and
// errors.Is(err, context.DeadlineExceeded) from the timeout layer.
//
// # Zero dependencies
//
// This package imports only the standard library and other packages in this
// module; it introduces no external runtime dependency and is covered by the
// module's zero-dependency CI gate.
package resilience
