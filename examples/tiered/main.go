// Package main demonstrates hierarchical (tiered) rate limiting with
// ratelimit/tiered. A tiered limiter enforces an ordered chain of limits — for
// example "10 req/s per user AND 20 req/s per org AND 100 req/s globally" — with
// all-or-nothing token accounting: if any tier would deny, no tier is charged,
// and the denying tier is reported in the Result metadata under "denied_tier".
//
// Tiers are ordered from most specific (per-user) to least specific (global).
// Each tier derives its own key from the request key via a KeyFunc. Here request
// keys look like "acme:alice" (org:user), so the per-org tier keeps the prefix
// before ":" and the global tier collapses every key to one shared bucket.
package main

import (
	"context"
	"fmt"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tiered"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
)

func main() {
	// Most specific first. Small numbers keep the demo output readable.
	limiter := tiered.New(
		tiered.Tier{ // per-user: 3 req burst, keyed by the full "org:user" string
			Name:    "per-user",
			Limiter: tokenbucket.New(3, 3),
		},
		tiered.Tier{ // per-org: 5 req burst, keyed by the "org" prefix
			Name:    "per-org",
			KeyFunc: tiered.Prefix(":"),
			Limiter: tokenbucket.New(5, 5),
		},
		tiered.Tier{ // global: 8 req burst across everyone
			Name:    "global",
			KeyFunc: tiered.Constant("global"),
			Limiter: tokenbucket.New(8, 8),
		},
	)
	defer limiter.Close()

	ctx := context.Background()

	// alice hits her own 3-req/user cap first, even though the org/global tiers
	// still have capacity — the tightest tier wins.
	fmt.Println("== alice (per-user cap is 3) ==")
	for i := 1; i <= 4; i++ {
		r := limiter.Allow(ctx, "acme:alice")
		report(i, "acme:alice", r)
	}

	// bob shares acme's per-org bucket. alice already spent 3 org tokens, so bob
	// gets the remaining 2 before the per-org tier (5) denies him.
	fmt.Println("\n== bob, same org (per-org cap is 5, 3 already spent by alice) ==")
	for i := 1; i <= 3; i++ {
		r := limiter.Allow(ctx, "acme:bob")
		report(i, "acme:bob", r)
	}
}

func report(i int, key string, r ratelimit.Result) {
	if r.Allowed {
		fmt.Printf("  req %d %-11s allowed (remaining=%d)\n", i, key, r.Remaining)
		return
	}
	fmt.Printf("  req %d %-11s DENIED by tier %q\n", i, key, r.Metadata["denied_tier"])
}
