package otel

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// tracerName is the instrumentation scope used by the span helpers. Tracers are
// resolved from the global provider so the helpers work regardless of whether a
// *Provider is threaded through the call site.
const tracerName = "github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/observability/otel"

// Attribute keys following OpenTelemetry semantic-convention style (lowercase,
// dotted namespaces). These describe resilience-specific dimensions that have no
// upstream semconv equivalent.
const (
	AttrRateLimitAlgorithm = attribute.Key("resilience.ratelimit.algorithm")
	AttrRateLimitKey       = attribute.Key("resilience.ratelimit.key")
	AttrBreakerName        = attribute.Key("resilience.circuitbreaker.name")
	AttrBulkheadName       = attribute.Key("resilience.bulkhead.name")
	AttrDecisionAllowed    = attribute.Key("resilience.decision.allowed")
	AttrOutcome            = attribute.Key("resilience.outcome")
)

// Outcome attribute values.
const (
	OutcomeAllowed  = "allowed"
	OutcomeRejected = "rejected"
	OutcomeError    = "error"
)

// tracer returns the shared tracer from the global provider.
func tracer() trace.Tracer {
	return otel.Tracer(tracerName)
}

// StartRateLimit starts a client-kind span around a rate-limiter Allow decision,
// tagged with the algorithm and key.
func StartRateLimit(ctx context.Context, algorithm, key string) (context.Context, trace.Span) {
	return tracer().Start(ctx, "ratelimit.Allow",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			AttrRateLimitAlgorithm.String(algorithm),
			AttrRateLimitKey.String(key),
		),
	)
}

// StartCircuitBreaker starts a span around a circuit-breaker Execute call.
func StartCircuitBreaker(ctx context.Context, name string) (context.Context, trace.Span) {
	return tracer().Start(ctx, "circuitbreaker.Execute",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			AttrBreakerName.String(name),
		),
	)
}

// StartBulkhead starts a span around a bulkhead Acquire call.
func StartBulkhead(ctx context.Context, name string) (context.Context, trace.Span) {
	return tracer().Start(ctx, "bulkhead.Acquire",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			AttrBulkheadName.String(name),
		),
	)
}

// EndDecision records the allow/reject outcome of a resilience operation and
// ends the span. It does not mark the span as an error: a rejected request is a
// normal, expected outcome, not a fault.
func EndDecision(span trace.Span, allowed bool) {
	outcome := OutcomeRejected
	if allowed {
		outcome = OutcomeAllowed
	}
	span.SetAttributes(
		AttrDecisionAllowed.Bool(allowed),
		AttrOutcome.String(outcome),
	)
	span.End()
}

// EndError records err on the span, sets an error status, and ends the span. A
// nil err is treated as a successful (non-error) completion.
func EndError(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.SetAttributes(AttrOutcome.String(OutcomeError))
	} else {
		span.SetStatus(codes.Ok, "")
	}
	span.End()
}
