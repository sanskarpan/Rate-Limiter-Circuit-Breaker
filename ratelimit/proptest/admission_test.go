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

// epoch is a fixed clock start aligned to a whole second so that windowed
// algorithms (which snap window boundaries to multiples of the window duration
// from the Unix epoch) start exactly on a window boundary. This makes the
// per-window admission accounting exact rather than off-by-one.
var epoch = time.Unix(1_700_000_000, 0).UTC()

const key = "k"

// disableCleanup returns an idle-cleanup interval long enough that the
// background cleanup goroutine's ticker never fires during a bounded schedule,
// so eviction can't perturb the invariants we assert. The schedules below never
// advance anywhere near this far.
const disableCleanup = 24 * time.Hour

// step is one entry in a random schedule: either an Allow/AllowN of n tokens or
// an advance of the manual clock by dt.
type step struct {
	// isAdvance selects between an advance (true) and an allow (false).
	isAdvance bool
	n         int           // tokens requested when !isAdvance
	dt        time.Duration // advance amount when isAdvance
}

// genSchedule builds a random schedule over a limiter configured with the given
// window/period as the natural time unit. Advances are drawn as fractions and
// small multiples of unit so that both intra-window and multi-window behaviour
// is exercised. maxN caps the per-call token count.
func genSchedule(t *rapid.T, unit time.Duration, maxN int) []step {
	return genScheduleCapped(t, unit, maxN, 0)
}

// genScheduleCapped is genSchedule with an optional cap on the cumulative clock
// advance. When maxTotalAdvance > 0, once the running total of advances would
// exceed it, further advance steps are clamped to zero (turned into no-ops) so
// the total never crosses the cap. This is used to keep the two sliding-window
// limiters (which have no configurable idle-cleanup and evict asynchronously)
// below their eviction threshold, so the observable decision sequence stays a
// deterministic function of the synchronous algorithm rather than of
// goroutine-scheduling-dependent async eviction.
func genScheduleCapped(t *rapid.T, unit time.Duration, maxN int, maxTotalAdvance time.Duration) []step {
	nSteps := rapid.IntRange(1, 60).Draw(t, "nSteps")
	steps := make([]step, nSteps)
	var total time.Duration
	for i := range steps {
		if rapid.Bool().Draw(t, "isAdvance") {
			// Advance by a fraction or small multiple of the unit. Use integer
			// nanosecond fractions to keep everything deterministic.
			num := rapid.IntRange(0, 5).Draw(t, "advNum")
			den := rapid.SampledFrom([]int64{1, 2, 3, 4}).Draw(t, "advDen")
			dt := time.Duration(int64(num)*int64(unit)) / time.Duration(den)
			if maxTotalAdvance > 0 && total+dt > maxTotalAdvance {
				dt = 0
			}
			total += dt
			steps[i] = step{isAdvance: true, dt: dt}
		} else {
			steps[i] = step{n: rapid.IntRange(1, maxN).Draw(t, "n")}
		}
	}
	return steps
}

// -------------------- Fixed window --------------------

// Fixed window: the counter resets at each wall-clock window boundary and admits
// at most `limit` tokens per window. Bound: admitted within any single window
// index never exceeds limit. We track admissions per distinct window index
// (floor(elapsed/window)) and assert each bucket stays <= limit.
func TestPropertyFixedWindowAdmission(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		limit := rapid.IntRange(1, 20).Draw(t, "limit")
		window := time.Duration(rapid.IntRange(1, 10).Draw(t, "windowSec")) * time.Second
		clk := clock.NewManualClock(epoch)
		lim := fixedwindow.New(limit, window,
			fixedwindow.WithClock(clk),
			fixedwindow.WithIdleCleanup(disableCleanup),
		)
		defer lim.Close()

		ctx := context.Background()
		admittedPerWindow := map[int64]int{}
		now := epoch

		for _, s := range genSchedule(t, window, limit+3) {
			if s.isAdvance {
				clk.Advance(s.dt)
				now = now.Add(s.dt)
				continue
			}
			res := lim.AllowN(ctx, key, s.n)
			checkRemaining(t, res, limit)
			if res.Allowed {
				// Compute the window index EXACTLY as the limiter does:
				// floor(now.UnixNano() / window.Nanoseconds()). Using an
				// epoch-relative index would misattribute admissions whenever the
				// clock start is not window-aligned.
				widx := now.UnixNano() / window.Nanoseconds()
				admittedPerWindow[widx] += s.n
			}
		}
		for widx, got := range admittedPerWindow {
			if got > limit {
				t.Fatalf("fixed window: window %d admitted %d tokens, exceeds limit %d",
					widx, got, limit)
			}
		}
	})
}

// -------------------- Token bucket --------------------

// Token bucket: total tokens ever consumed <= capacity + refillRate*elapsedSecs.
// The bucket starts full (capacity) and refills at refillRate tokens/sec capped
// at capacity, so the cumulative admitted count can never exceed the initial
// fill plus everything that could have refilled over the total elapsed time.
func TestPropertyTokenBucketAdmission(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		capacity := rapid.IntRange(1, 20).Draw(t, "capacity")
		refillRate := float64(rapid.IntRange(1, 20).Draw(t, "refillRate"))
		clk := clock.NewManualClock(epoch)
		lim := tokenbucket.New(float64(capacity), refillRate,
			tokenbucket.WithClock(clk),
			tokenbucket.WithIdleCleanup(disableCleanup),
		)
		defer lim.Close()

		ctx := context.Background()
		var admitted int
		var elapsed time.Duration
		// unit ~ 1s is the natural refill time-scale.
		for _, s := range genSchedule(t, time.Second, capacity+3) {
			if s.isAdvance {
				clk.Advance(s.dt)
				elapsed += s.dt
				continue
			}
			res := lim.AllowN(ctx, key, s.n)
			checkRemaining(t, res, capacity)
			if res.Allowed {
				admitted += s.n
			}
		}
		// Bound: initial capacity + refilled tokens over total elapsed. Add a
		// tiny epsilon-free ceiling: since refill is capped at capacity the exact
		// analytic max is min-bounded, but capacity + rate*elapsed is always a
		// valid upper bound and is tight when the bucket is drained continuously.
		maxAdmit := float64(capacity) + refillRate*elapsed.Seconds()
		// +1 for float flooring inside the limiter (it stores fractional tokens;
		// a request is allowed on current >= n where current may be a hair above
		// the analytic value only due to same-instant arithmetic, never above the
		// true rate). Use a strict analytic bound; flooring only ever REDUCES what
		// the limiter grants, so no epsilon is required. Guard anyway.
		if float64(admitted) > maxAdmit+1e-9 {
			t.Fatalf("token bucket: admitted %d tokens, exceeds bound %.6f (cap=%d rate=%.0f elapsed=%s)",
				admitted, maxAdmit, capacity, refillRate, elapsed)
		}
	})
}

// -------------------- GCRA --------------------

// GCRA: within any interval of length `elapsed`, admitted requests <=
// burst + elapsed/emissionInterval, where emissionInterval = window/limit.
// We assert the cumulative form over the whole run: total admitted (counting n
// per AllowN) <= burst + totalElapsed/emissionInterval. GCRA's TAT can advance
// at most one emissionInterval per admitted token, and starts with up to `burst`
// of slack, so the cumulative admitted tokens are bounded by that expression.
func TestPropertyGCRAAdmission(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		limit := rapid.IntRange(1, 20).Draw(t, "limit")
		burst := rapid.IntRange(1, 10).Draw(t, "burst")
		window := time.Duration(rapid.IntRange(1, 10).Draw(t, "windowSec")) * time.Second
		emission := window / time.Duration(limit)
		clk := clock.NewManualClock(epoch)
		lim := gcra.New(limit, burst, window,
			gcra.WithClock(clk),
			gcra.WithIdleCleanup(disableCleanup),
		)
		defer lim.Close()

		ctx := context.Background()
		var admitted int
		var elapsed time.Duration
		for _, s := range genSchedule(t, emission, burst+3) {
			if s.isAdvance {
				clk.Advance(s.dt)
				elapsed += s.dt
				continue
			}
			// AllowN with n > burst is always denied by GCRA; still valid to call.
			res := lim.AllowN(ctx, key, s.n)
			// GCRA reports Remaining as the number of further single requests
			// admittable right now, capped at burst (not limit).
			checkRemaining(t, res, burst)
			if res.Allowed {
				admitted += s.n
			}
		}
		// TAT-based bound. Each admitted token pushes TAT forward by exactly one
		// emissionInterval; the allow condition keeps TAT within
		// now + burst*emissionInterval. Over the whole run, elapsed advances the
		// "now" ceiling, so admitted*emission <= elapsed + burst*emission.
		maxAdmit := float64(burst) + float64(elapsed)/float64(emission)
		if float64(admitted) > maxAdmit+1e-9 {
			t.Fatalf("gcra: admitted %d, exceeds bound %.6f (burst=%d emission=%s elapsed=%s)",
				admitted, maxAdmit, burst, emission, elapsed)
		}
	})
}

// -------------------- Sliding window log (exact) --------------------

// Sliding window log is EXACT: at any instant the number of admitted timestamps
// within the trailing `window` never exceeds `limit`. We reconstruct the log of
// admission timestamps ourselves and, after every admission, assert that the
// count of our recorded timestamps within the trailing window is <= limit.
func TestPropertySlidingWindowLogAdmission(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		limit := rapid.IntRange(1, 20).Draw(t, "limit")
		window := time.Duration(rapid.IntRange(1, 10).Draw(t, "windowSec")) * time.Second
		clk := clock.NewManualClock(epoch)
		lim := slidingwindow.NewLog(limit, window,
			slidingwindow.WithLogClock(clk),
		)
		defer lim.Close()

		ctx := context.Background()
		var stamps []time.Time
		now := epoch
		// Cap total advance below the log's async-eviction threshold (2*window)
		// so the active key is never asynchronously deleted mid-schedule.
		for _, s := range genScheduleCapped(t, window, limit+3, window) {
			if s.isAdvance {
				clk.Advance(s.dt)
				now = now.Add(s.dt)
				continue
			}
			res := lim.AllowN(ctx, key, s.n)
			checkRemaining(t, res, limit)
			if res.Allowed {
				for i := 0; i < s.n; i++ {
					stamps = append(stamps, now)
				}
				// Count our recorded stamps within the trailing window.
				cutoff := now.Add(-window)
				cnt := 0
				for _, ts := range stamps {
					// The limiter prunes with `!ts.Before(cutoff)` (i.e. ts >= cutoff).
					if !ts.Before(cutoff) {
						cnt++
					}
				}
				if cnt > limit {
					t.Fatalf("sliding log: %d admitted in trailing window, exceeds limit %d",
						cnt, limit)
				}
			}
		}
	})
}

// -------------------- Sliding window counter (approximate) --------------------

// Sliding window counter is APPROXIMATE: it can over-admit near window
// boundaries by up to the previous window's residual. The documented ceiling is
// limit*(1 + 1/N). For the admission accounting we use a robust bound that
// always holds: within any two consecutive fixed sub-windows the admitted count
// cannot exceed 2*limit (the previous window contributes at most `limit`, the
// current at most `limit`). We assert admitted within any trailing `window`
// <= 2*limit, which is the true worst case of the two-bucket approximation.
func TestPropertySlidingWindowCounterAdmission(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		limit := rapid.IntRange(1, 20).Draw(t, "limit")
		window := time.Duration(rapid.IntRange(1, 10).Draw(t, "windowSec")) * time.Second
		clk := clock.NewManualClock(epoch)
		lim := slidingwindow.NewCounter(limit, window,
			slidingwindow.WithClock(clk),
		)
		defer lim.Close()

		ctx := context.Background()
		var stamps []time.Time
		now := epoch
		// Cap total advance below the counter's async-eviction threshold
		// (5*window) so the active key is never asynchronously deleted.
		for _, s := range genScheduleCapped(t, window, limit+3, 3*window) {
			if s.isAdvance {
				clk.Advance(s.dt)
				now = now.Add(s.dt)
				continue
			}
			res := lim.AllowN(ctx, key, s.n)
			checkRemaining(t, res, limit)
			if res.Allowed {
				for i := 0; i < s.n; i++ {
					stamps = append(stamps, now)
				}
				cutoff := now.Add(-window)
				cnt := 0
				for _, ts := range stamps {
					if !ts.Before(cutoff) {
						cnt++
					}
				}
				// Two-bucket approximation worst case: 2*limit within a trailing
				// window (a full previous bucket + a full current bucket).
				if cnt > 2*limit {
					t.Fatalf("sliding counter: %d admitted in trailing window, exceeds 2*limit=%d",
						cnt, 2*limit)
				}
			}
		}
	})
}

// checkRemaining asserts Result.Remaining ∈ [0, max] (property 2) for every
// decision, allowed or denied.
func checkRemaining(t *rapid.T, res ratelimit.Result, max int) {
	t.Helper()
	if res.Remaining < 0 {
		t.Fatalf("Remaining %d is negative", res.Remaining)
	}
	if res.Remaining > max {
		t.Fatalf("Remaining %d exceeds max %d", res.Remaining, max)
	}
}
