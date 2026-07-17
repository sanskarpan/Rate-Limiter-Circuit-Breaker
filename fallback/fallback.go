// Package fallback provides fallback and hedge request patterns for resilience.
//
// Fallback: run a primary function; if it fails, run a secondary function.
// Hedge: fire the primary request; if it doesn't complete within hedgeDelay, fire
// a backup in parallel. The first response wins and the loser is cancelled.
//
// Hedge requests reduce P99 latency at the cost of ~5% extra requests when
// hedgeDelay is set to the P95 latency of the target service.
package fallback

import (
	"context"
	"sync"
	"time"
)

// HedgeResult holds the outcome of a hedged request.
type HedgeResult struct {
	// Primary is true if the primary request won the race.
	Primary bool
	// Err is the error returned by the winning request (nil on success).
	Err error
}

// Do executes fn; if fn returns an error or panics, fb is called with the original
// context and the error. If fn succeeds (returns nil), fb is never called.
//
// This is a simple synchronous fallback — suitable for static fallbacks like
// cached values, default responses, or alternative backends.
func Do(ctx context.Context, fn func(context.Context) error, fb func(context.Context, error) error) error {
	err := fn(ctx)
	if err != nil {
		return fb(ctx, err)
	}
	return nil
}

// DoWithResult executes fn; on error, calls fb and returns its result.
func DoWithResult[T any](
	ctx context.Context,
	fn func(context.Context) (T, error),
	fb func(context.Context, error) (T, error),
) (T, error) {
	result, err := fn(ctx)
	if err != nil {
		return fb(ctx, err)
	}
	return result, nil
}

// Hedge executes fn and, after hedgeDelay, if fn has not completed, fires a
// second concurrent call to fn. The first call to complete (success or failure)
// wins; the other is cancelled via its context.
//
// Returns a HedgeResult indicating which request won.
//
// Hedge is useful when the target service has a long-tail latency distribution.
// Setting hedgeDelay to the P95 latency eliminates most of the P99 tail at the
// cost of roughly 5% additional requests to the downstream service.
func Hedge(ctx context.Context, hedgeDelay time.Duration, fn func(context.Context) error) HedgeResult {
	// A non-positive hedgeDelay would make the hedge timer fire immediately,
	// launching a redundant backup even when the primary would return fast.
	// Treat it as "no hedging": a single synchronous call.
	if hedgeDelay <= 0 {
		return HedgeResult{Primary: true, Err: fn(ctx)}
	}

	type result struct {
		err     error
		primary bool
	}

	resultCh := make(chan result, 2) // buffered so goroutines never block

	// Launch primary
	primaryCtx, primaryCancel := context.WithCancel(ctx)
	defer primaryCancel()

	go func() {
		err := fn(primaryCtx)
		resultCh <- result{err: err, primary: true}
	}()

	// Hedge timer
	timer := time.NewTimer(hedgeDelay)
	defer timer.Stop()

	select {
	case r := <-resultCh:
		// Primary finished before hedge delay — no backup to cancel
		return HedgeResult{Primary: r.primary, Err: r.err}
	case <-ctx.Done():
		return HedgeResult{Primary: true, Err: ctx.Err()}
	case <-timer.C:
		// Hedge delay elapsed — fall through to launch backup
	}

	// Launch backup after hedge delay
	backupCtx, backupCancel := context.WithCancel(ctx)
	defer backupCancel()
	go func() {
		err := fn(backupCtx)
		resultCh <- result{err: err, primary: false}
	}()

	// Wait for first result
	r := <-resultCh
	if !r.primary {
		// Backup won — cancel primary
		primaryCancel()
	}

	// Drain the second result to avoid goroutine leak
	go func() {
		<-resultCh
		// discard
	}()

	return HedgeResult{Primary: r.primary, Err: r.err}
}

// HedgeCond executes fn and, after hedgeDelay, fires a backup only if
// the primary has not yet completed AND shouldHedge returns true.
// shouldHedge can inspect the context to decide (e.g., only hedge GET requests).
func HedgeCond(
	ctx context.Context,
	hedgeDelay time.Duration,
	fn func(context.Context) error,
	shouldHedge func(context.Context) bool,
) HedgeResult {
	if !shouldHedge(ctx) {
		return HedgeResult{Primary: true, Err: fn(ctx)}
	}
	return Hedge(ctx, hedgeDelay, fn)
}

// Fallback wraps a primary function and a fallback function.
// It is a composable struct for use in the pipeline.
type Fallback struct {
	fb func(context.Context, error) error
}

// New creates a Fallback that calls fb whenever the primary function fails.
func New(fb func(context.Context, error) error) *Fallback {
	return &Fallback{fb: fb}
}

// Execute runs fn; on error, runs the fallback.
func (f *Fallback) Execute(ctx context.Context, fn func(context.Context) error) error {
	return Do(ctx, fn, f.fb)
}

// HedgeN fires up to n concurrent requests and returns the first successful
// response. All losers are cancelled. If all fail, the last error is returned.
// n must be >= 1; if n == 1, the single request is made with no hedging.
//
// This is the "speculative execution" pattern used by systems like Google Bigtable.
func HedgeN(ctx context.Context, hedgeDelay time.Duration, n int, fn func(context.Context) error) error {
	if n < 1 {
		n = 1
	}
	// A non-positive hedgeDelay makes the hedge timer fire immediately, which
	// would launch every backup at once regardless of primary progress. Treat
	// it as "no hedging" and make a single call (mirrors Hedge, M-11).
	if n == 1 || hedgeDelay <= 0 {
		return fn(ctx)
	}

	type attempt struct {
		err error
		idx int
	}

	resultCh := make(chan attempt, n)
	cancels := make([]context.CancelFunc, 0, n)
	var mu sync.Mutex

	// firedTotal counts how many attempts have ever been launched (<= n).
	// outstanding counts attempts still in flight (not yet reported a result).
	firedTotal := 0
	outstanding := 0

	fireAttempt := func(idx int) {
		attemptCtx, cancel := context.WithCancel(ctx) //nolint:gosec // G118 FP: cancel is stored in `cancels` and invoked on completion/cleanup
		mu.Lock()
		cancels = append(cancels, cancel)
		mu.Unlock()
		firedTotal++
		outstanding++
		go func() {
			err := fn(attemptCtx)
			resultCh <- attempt{err: err, idx: idx}
		}()
	}

	cancelAll := func() {
		mu.Lock()
		for _, c := range cancels {
			c()
		}
		mu.Unlock()
	}

	// drainRemaining consumes the outstanding results in the background so the
	// attempt goroutines never block sending on resultCh (leak prevention).
	drainRemaining := func(remaining int) {
		go func() {
			for remaining > 0 {
				<-resultCh
				remaining--
			}
		}()
	}

	// Fire first attempt immediately.
	fireAttempt(0)

	timer := time.NewTimer(hedgeDelay)
	defer timer.Stop()

	var lastErr error
	for {
		select {
		case r := <-resultCh:
			outstanding--
			if r.err == nil {
				// A winner: cancel everyone else and drain their results.
				cancelAll()
				drainRemaining(outstanding)
				return nil
			}
			lastErr = r.err
			// This attempt failed. If we have budget left, fire the next
			// attempt right away rather than waiting for the hedge timer —
			// otherwise a burst of fast failures could exhaust `outstanding`
			// before the full budget of n attempts is ever tried (H-18).
			if firedTotal < n {
				fireAttempt(firedTotal)
				timer.Reset(hedgeDelay)
			} else if outstanding == 0 {
				// All n attempts fired and none are left in flight: give up.
				return lastErr
			}
		case <-timer.C:
			if firedTotal < n {
				fireAttempt(firedTotal)
				timer.Reset(hedgeDelay)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
