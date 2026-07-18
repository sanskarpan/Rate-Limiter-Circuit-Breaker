// Package ratelimitctx provides typed context helpers for propagating a
// request's cost (weight) and priority through call chains, so that middleware,
// pipeline stages, load shedders, and handlers can agree on those values
// without threading extra parameters (ENHANCEMENTS §2.8).
//
// Cost models weighted/points-based rate limits (a bulk write costs more than a
// health check; see GitHub/Stripe/Shopify APIs). Priority lets shedding and
// tiered admission drop the least-important requests first.
//
// Values are stored under unexported key types, never string keys, so they
// cannot collide with context values set by other packages and cannot be
// overwritten or read by code that does not import this package.
package ratelimitctx

import (
	"context"
	"net/http"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/middleware"
)

// costKey and priorityKey are unexported, zero-size, distinct types used as
// context keys. Using dedicated types (rather than strings) makes collisions
// impossible and keeps the values unreachable outside this package.
type (
	costKey     struct{}
	priorityKey struct{}
)

// DefaultCost is the cost assumed for a request that carries no explicit cost
// in its context. It matches the middleware convention that every request costs
// at least one token.
const DefaultCost = 1

// WithCost returns a copy of ctx that carries the given request cost (weight).
// A cost below 1 is clamped to 1, since every request consumes at least one
// token. Retrieve it with CostFromContext.
func WithCost(ctx context.Context, cost int) context.Context {
	if cost < 1 {
		cost = 1
	}
	return context.WithValue(ctx, costKey{}, cost)
}

// CostFromContext returns the cost stored by WithCost and true, or (0, false)
// if no cost is present. Callers that want the effective cost with the default
// applied should use CostOrDefault.
func CostFromContext(ctx context.Context) (int, bool) {
	c, ok := ctx.Value(costKey{}).(int)
	return c, ok
}

// CostOrDefault returns the cost stored by WithCost, or DefaultCost if none is
// present. The returned value is always >= 1.
func CostOrDefault(ctx context.Context) int {
	if c, ok := CostFromContext(ctx); ok {
		return c
	}
	return DefaultCost
}

// WithPriority returns a copy of ctx that carries the given request priority.
// The numeric convention (higher vs. lower = more important) is defined by the
// consumer; this package only propagates the value. Retrieve it with
// PriorityFromContext.
func WithPriority(ctx context.Context, p int) context.Context {
	return context.WithValue(ctx, priorityKey{}, p)
}

// PriorityFromContext returns the priority stored by WithPriority and true, or
// (0, false) if no priority is present.
func PriorityFromContext(ctx context.Context) (int, bool) {
	p, ok := ctx.Value(priorityKey{}).(int)
	return p, ok
}

// PriorityOrDefault returns the priority stored by WithPriority, or def if none
// is present.
func PriorityOrDefault(ctx context.Context, def int) int {
	if p, ok := PriorityFromContext(ctx); ok {
		return p
	}
	return def
}

// CostFuncFromContext returns a middleware.CostFunc that reads the per-request
// cost from the request's context (as set by WithCost), defaulting to
// DefaultCost when no cost is present. It adapts the context-propagated cost to
// the signature the rate-limit HTTP middleware expects, so a handler upstream
// of the middleware can set the cost via WithCost and have the middleware honor
// it without a bespoke CostFunc:
//
//	mw := middleware.RateLimit(limiter, middleware.WithCost(ratelimitctx.CostFuncFromContext()))
//
// The returned value is always >= 1.
func CostFuncFromContext() middleware.CostFunc {
	return func(r *http.Request) int {
		return CostOrDefault(r.Context())
	}
}

// GRPCCostFuncFromContext returns a middleware.GRPCCostFunc that reads the
// per-call cost from the gRPC call's context (as set by WithCost), defaulting
// to DefaultCost when no cost is present. It is the gRPC analogue of
// CostFuncFromContext. The returned value is always >= 1.
func GRPCCostFuncFromContext() middleware.GRPCCostFunc {
	return func(ctx context.Context, _ string) int {
		return CostOrDefault(ctx)
	}
}
