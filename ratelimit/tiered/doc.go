// Package tiered implements hierarchical (tiered) rate limiting: a request is
// checked against an ordered chain of tiers and is allowed only if every tier
// in the chain allows it. Each tier derives its own key from the incoming key
// via a KeyFunc, so the same request can be limited per-user, per-tenant and
// globally at the same time.
//
// # Model
//
// A [Tier] pairs a KeyFunc (how to derive this tier's key from the request key)
// with an underlying [ratelimit.Limiter]. Tiers are ordered from most specific
// to least specific, for example:
//
//	limiter := tiered.New(
//	    []tiered.Tier{
//	        {Name: "user",   KeyFunc: nil,                    Limiter: userLimiter},
//	        {Name: "tenant", KeyFunc: tiered.Prefix(":"),     Limiter: tenantLimiter},
//	        {Name: "global", KeyFunc: tiered.Constant("all"), Limiter: globalLimiter},
//	    },
//	)
//	res := limiter.Allow(ctx, "acme:alice")
//
// # All-or-nothing token accounting
//
// The central correctness property is that a call either debits every tier or
// debits none of them. If any tier would deny, no tier is left with tokens
// consumed. This is achieved with a serialized check-then-commit protocol:
//
//  1. Under a single mutex, Peek every tier (non-consuming) to confirm each has
//     capacity for the requested cost.
//  2. If any tier lacks capacity, return a deny WITHOUT calling AllowN on the
//     tiers that would allow — so their tokens are never touched.
//  3. Otherwise call AllowN on each tier in order. In the rare case a tier
//     denies in this phase (only possible if that tier's underlying limiter is
//     also consumed directly, outside this tiered limiter), the tiers already
//     committed in this call are rolled back on a best-effort basis via
//     re-crediting, and the call reports a deny.
//
// The mutex makes the Peek phase and the commit phase atomic with respect to
// other operations that go through this tiered limiter, which is what makes the
// common path leak-free even under concurrent callers.
//
// The Metadata of a denied Result carries the name and derived key of the tier
// that caused the denial under the keys "denied_tier" and "denied_key", giving
// callers observability into which layer of the hierarchy is the bottleneck.
//
// All methods are safe for concurrent use. Time is sourced through a
// clock.Clock so Wait/WaitN are deterministic under a mock clock in tests.
package tiered
