package proptest

import (
	"context"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/fixedwindow"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/gcra"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/slidingwindow"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
)

// limiterConfig captures the derived quantities a property needs about a built
// limiter.
type limiterConfig struct {
	unit         time.Duration // natural time-scale for scaling schedule advances
	maxRemaining int           // upper bound Result.Remaining is clamped to
	freshBurst   int           // # of single requests admittable from a full/empty state
	// maxTotalAdvance bounds the cumulative clock advance a schedule may use.
	// The two sliding-window limiters expose no idle-cleanup option, so their
	// background eviction ticker fires on any advance and can asynchronously
	// delete the active key once it has been idle past the eviction threshold
	// (2×window for the log, 5×window for the counter). That async deletion is
	// goroutine-scheduling dependent and would make the OBSERVABLE decision
	// sequence nondeterministic. Keeping the total advance below the eviction
	// threshold guarantees the active key is never evicted, so the synchronous
	// core algorithm's determinism is what the property actually measures. Zero
	// means "unbounded" (used by limiters whose cleanup is disabled via
	// WithIdleCleanup).
	maxTotalAdvance time.Duration
}

// limiterFactory builds a fresh limiter bound to the given clock, plus its
// derived config. maxRemaining and freshBurst differ for GCRA (both = burst)
// from the window/capacity-based limiters (both = limit/capacity).
type limiterFactory struct {
	name  string
	build func(clk *clock.ManualClock) (ratelimit.Limiter, limiterConfig)
}

// syncFactories are the non-blocking, purely synchronous limiters. Leaky bucket
// is intentionally excluded: its Allow blocks on a background leaker goroutine
// driven by a ManualClock ticker, so it cannot be driven by a simple synchronous
// op/advance loop without careful goroutine choreography. It is covered
// separately by the queue-shaped properties in leakybucket_test.go.
func syncFactories() []limiterFactory {
	return []limiterFactory{
		{
			name: "fixedwindow",
			build: func(clk *clock.ManualClock) (ratelimit.Limiter, limiterConfig) {
				limit := 7
				window := 3 * time.Second
				return fixedwindow.New(limit, window,
						fixedwindow.WithClock(clk),
						fixedwindow.WithIdleCleanup(disableCleanup)),
					limiterConfig{unit: window, maxRemaining: limit, freshBurst: limit}
			},
		},
		{
			name: "tokenbucket",
			build: func(clk *clock.ManualClock) (ratelimit.Limiter, limiterConfig) {
				capacity := 8
				return tokenbucket.New(float64(capacity), 4,
						tokenbucket.WithClock(clk),
						tokenbucket.WithIdleCleanup(disableCleanup)),
					limiterConfig{unit: time.Second, maxRemaining: capacity, freshBurst: capacity}
			},
		},
		{
			name: "gcra",
			build: func(clk *clock.ManualClock) (ratelimit.Limiter, limiterConfig) {
				limit, burst := 10, 5
				window := 5 * time.Second
				return gcra.New(limit, burst, window,
						gcra.WithClock(clk),
						gcra.WithIdleCleanup(disableCleanup)),
					limiterConfig{unit: window / time.Duration(limit), maxRemaining: burst, freshBurst: burst}
			},
		},
		{
			name: "slidingwindowlog",
			build: func(clk *clock.ManualClock) (ratelimit.Limiter, limiterConfig) {
				limit := 7
				window := 3 * time.Second
				return slidingwindow.NewLog(limit, window,
						slidingwindow.WithLogClock(clk)),
					limiterConfig{
						unit: window, maxRemaining: limit, freshBurst: limit,
						// Eviction at idle >= 2*window; stay well under it.
						maxTotalAdvance: window,
					}
			},
		},
		{
			name: "slidingwindowcounter",
			build: func(clk *clock.ManualClock) (ratelimit.Limiter, limiterConfig) {
				limit := 7
				window := 3 * time.Second
				return slidingwindow.NewCounter(limit, window,
						slidingwindow.WithClock(clk)),
					limiterConfig{
						unit: window, maxRemaining: limit, freshBurst: limit,
						// Eviction at idle >= 5*window; stay well under it.
						maxTotalAdvance: 3 * window,
					}
			},
		},
	}
}

// decision is the observable outcome of a single Allow/AllowN call that must be
// stable across identical replays.
type decision struct {
	allowed   bool
	remaining int
}

// TestPropertyDeterminism (property 3): replaying an identical op/advance
// schedule against a fresh limiter + fresh ManualClock yields byte-identical
// decisions. Any hidden nondeterminism (e.g. map iteration order affecting
// state, wall-clock leakage) would surface as a mismatch.
func TestPropertyDeterminism(t *testing.T) {
	for _, f := range syncFactories() {
		f := f
		t.Run(f.name, func(t *testing.T) {
			rapid.Check(t, func(t *rapid.T) {
				// Draw a unit-agnostic schedule; scale by each limiter's unit.
				// We need the schedule before we know the unit, so build the
				// limiter once to learn the unit, then generate against it.
				clk0 := clock.NewManualClock(epoch)
				_, cfg := f.build(clk0)
				steps := genScheduleCapped(t, cfg.unit, cfg.maxRemaining+3, cfg.maxTotalAdvance)

				run := func() []decision {
					clk := clock.NewManualClock(epoch)
					lim, _ := f.build(clk)
					defer lim.Close()
					ctx := context.Background()
					var out []decision
					for _, s := range steps {
						if s.isAdvance {
							clk.Advance(s.dt)
							continue
						}
						res := lim.AllowN(ctx, key, s.n)
						out = append(out, decision{res.Allowed, res.Remaining})
					}
					return out
				}

				a := run()
				b := run()
				if len(a) != len(b) {
					t.Fatalf("%s: replay length mismatch %d vs %d", f.name, len(a), len(b))
				}
				for i := range a {
					if a[i] != b[i] {
						t.Fatalf("%s: replay decision %d mismatch %+v vs %+v",
							f.name, i, a[i], b[i])
					}
				}
			})
		})
	}
}

// TestPropertyAllowNAtomicity (property 4): AllowN(n) is all-or-nothing. After
// any AllowN call we verify via Peek that the observable remaining capacity
// changed by exactly n on an allow and by exactly 0 on a deny — never a partial
// amount. This catches a limiter that consumed some-but-not-all of a denied
// batch.
//
// We use Peek (side-effect-free) immediately before and after each AllowN to
// read the remaining capacity. For the exact-accounting limiters (fixed window,
// sliding log, GCRA, token bucket) Peek's Remaining moves by exactly n on an
// allow. The sliding-window counter reports a floored, fractional-derived
// Remaining, so it is asserted with a tolerance rather than exactly.
func TestPropertyAllowNAtomicity(t *testing.T) {
	for _, f := range syncFactories() {
		f := f
		t.Run(f.name, func(t *testing.T) {
			rapid.Check(t, func(t *rapid.T) {
				clk := clock.NewManualClock(epoch)
				lim, cfg := f.build(clk)
				defer lim.Close()
				ctx := context.Background()

				approx := f.name == "slidingwindowcounter"

				for _, s := range genScheduleCapped(t, cfg.unit, cfg.maxRemaining+3, cfg.maxTotalAdvance) {
					if s.isAdvance {
						clk.Advance(s.dt)
						continue
					}
					before := lim.Peek(ctx, key).Remaining
					res := lim.AllowN(ctx, key, s.n)
					after := lim.Peek(ctx, key).Remaining
					checkRemaining(t, res, cfg.maxRemaining)

					if res.Allowed {
						delta := before - after
						if approx {
							// Approximate limiter: allow a small rounding slack.
							if delta < s.n-1 || delta > s.n+1 {
								t.Fatalf("%s: allowed AllowN(%d) changed remaining by %d (want ~%d)",
									f.name, s.n, delta, s.n)
							}
						} else if delta != s.n {
							t.Fatalf("%s: allowed AllowN(%d) changed remaining by %d, not %d (partial?)",
								f.name, s.n, delta, s.n)
						}
					} else {
						// Denied: no tokens may have been consumed. Remaining must
						// not DROP because of the denied call. (It can only rise if
						// the clock advanced, but we didn't advance here, so equal.)
						if !approx && after < before {
							t.Fatalf("%s: denied AllowN(%d) reduced remaining %d -> %d (partial consume!)",
								f.name, s.n, before, after)
						}
						if approx && after < before-1 {
							t.Fatalf("%s: denied AllowN(%d) reduced remaining %d -> %d (partial consume!)",
								f.name, s.n, before, after)
						}
					}
				}
			})
		})
	}
}

// TestPropertyDeniedThenSingleTokenHonoured (property 4, sharper form): after a
// DENIED AllowN(n), a subsequent AllowN(1) succeeds only if at least one token
// is genuinely available. We assert the standard token-bucket-style contract:
// if AllowN(n) was denied and AllowN(1) then succeeds, the pre-existing
// remaining (via Peek) must have been >= 1. This proves the denied batch did not
// secretly consume the last token.
func TestPropertyDeniedThenSingleTokenHonoured(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		capacity := rapid.IntRange(2, 20).Draw(t, "capacity")
		refillRate := float64(rapid.IntRange(1, 10).Draw(t, "refillRate"))
		clk := clock.NewManualClock(epoch)
		lim := tokenbucket.New(float64(capacity), refillRate,
			tokenbucket.WithClock(clk),
			tokenbucket.WithIdleCleanup(disableCleanup),
		)
		defer lim.Close()
		ctx := context.Background()

		for _, s := range genSchedule(t, time.Second, capacity+5) {
			if s.isAdvance {
				clk.Advance(s.dt)
				continue
			}
			// Request more than could ever fit sometimes, to force denials.
			n := s.n
			availBefore := lim.Peek(ctx, key).Remaining
			res := lim.AllowN(ctx, key, n)
			if !res.Allowed {
				// Denied batch must not have consumed anything: a follow-up single
				// token succeeds iff a token was actually available.
				availAfterDeny := lim.Peek(ctx, key).Remaining
				if availAfterDeny != availBefore {
					t.Fatalf("token bucket: denied AllowN(%d) changed availability %d -> %d",
						n, availBefore, availAfterDeny)
				}
				single := lim.AllowN(ctx, key, 1)
				if single.Allowed && availAfterDeny < 1 {
					t.Fatalf("token bucket: AllowN(1) succeeded but only %d available before it",
						availAfterDeny)
				}
			}
		}
	})
}

// TestPropertyResetClearsState (property 5): after Reset, a fresh full burst is
// admittable — i.e. the key behaves like a brand-new one. We drain the limiter
// (consume everything), assert the next single request is denied, Reset, then
// assert a full burst of `max` single requests all pass.
func TestPropertyResetClearsState(t *testing.T) {
	for _, f := range syncFactories() {
		f := f
		t.Run(f.name, func(t *testing.T) {
			rapid.Check(t, func(t *rapid.T) {
				clk := clock.NewManualClock(epoch)
				lim, cfg := f.build(clk)
				defer lim.Close()
				ctx := context.Background()

				// Drain: issue enough single requests to saturate. Whatever the
				// algorithm, after this (with no time advance) it is saturated.
				for i := 0; i < 4*cfg.freshBurst; i++ {
					lim.Allow(ctx, key)
				}
				// Now saturated: the very next request must be denied.
				if lim.Allow(ctx, key).Allowed {
					t.Fatalf("%s: expected denial after draining, but was allowed", f.name)
				}

				if err := lim.Reset(ctx, key); err != nil {
					t.Fatalf("%s: Reset returned error: %v", f.name, err)
				}

				// Post-reset a fresh burst of `freshBurst` single requests must all
				// be admitted (bucket/window is full again). We do not advance the
				// clock — Reset alone must restore capacity.
				for i := 0; i < cfg.freshBurst; i++ {
					r := lim.Allow(ctx, key)
					if !r.Allowed {
						t.Fatalf("%s: request %d/%d after Reset was denied (state not cleared)",
							f.name, i+1, cfg.freshBurst)
					}
				}
			})
		})
	}
}
