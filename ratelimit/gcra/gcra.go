// Package gcra implements the Generic Cell Rate Algorithm (GCRA) for rate limiting.
//
// GCRA is used by Stripe, Shopify, and many high-performance APIs because:
//  1. A single timestamp per key (not a counter + window, not N timestamps)
//  2. Mathematically exact — no approximation unlike sliding window counter
//  3. Perfectly suited for distributed systems (Redis SET on single key, CAS operation)
//  4. Allows precise burst control via burstOffset
//
// Reference: "Traffic Management" in ATM Forum specification; also see
// https://brandur.org/rate-limiting for a practical implementation guide.
//
// Core formula:
//
//	emissionInterval = window / limit
//	burstOffset      = emissionInterval * (burst - 1)
//	TAT              = max(lastTAT[key], now) + emissionInterval
//	allowed          = TAT - burstOffset <= now
//	RetryAfter       = TAT - burstOffset - now  (when denied)
//	Remaining        = (now + burstOffset - TAT) / emissionInterval
//
// All arithmetic uses integer nanoseconds — no floating point on the critical path.
//
// All methods on GCRA are safe for concurrent use.
package gcra

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/metric"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
)

const algorithmName = "gcra"

// entry holds per-key GCRA state: a single TAT timestamp.
type entry struct {
	mu         sync.Mutex
	tat        time.Time // Theoretical Arrival Time
	lastAccess time.Time
}

// GCRA implements the Limiter interface using the Generic Cell Rate Algorithm.
// All methods are safe for concurrent use.
type GCRA struct {
	limit     int
	burst     int
	window    time.Duration
	idleClean time.Duration

	// Pre-computed constants (immutable after construction)
	emissionInterval time.Duration // window / limit
	burstOffset      time.Duration // emissionInterval * (burst - 1)

	mu      sync.RWMutex
	entries map[string]*entry

	clock  clock.Clock
	rec    metric.Recorder
	done   chan struct{}
	wg     sync.WaitGroup
	closed bool

	// onDecision, when non-nil, is fired synchronously after every Allow/AllowN
	// decision. nil by default so the hot path stays a single nil check.
	onDecision func(key string, r ratelimit.Result)
}

// New creates a new GCRA with the given limit per window, burst size, and window duration.
// burst >= 1; burst=1 means no additional burst (strict rate limiting).
// burst=5 means up to 5 requests can be sent immediately before rate limiting kicks in.
func New(limit int, burst int, window time.Duration, opts ...Option) *GCRA {
	// Validate config before any division (M-5). limit <= 0 would otherwise
	// trigger an integer divide-by-zero panic in the emissionInterval math; a
	// non-positive window yields a zero emission interval and a nonsensical rate.
	if limit <= 0 {
		panic(fmt.Sprintf("gcra.New: limit must be > 0, got %d", limit))
	}
	if window <= 0 {
		panic(fmt.Sprintf("gcra.New: window must be > 0, got %s", window))
	}
	if burst < 1 {
		burst = 1
	}
	emissionInterval := window / time.Duration(limit)
	burstOffset := emissionInterval * time.Duration(burst-1)
	g := &GCRA{
		limit:            limit,
		burst:            burst,
		window:           window,
		idleClean:        5 * time.Minute,
		emissionInterval: emissionInterval,
		burstOffset:      burstOffset,
		clock:            clock.RealClock{},
		rec:              metric.Default(),
		done:             make(chan struct{}),
		entries:          make(map[string]*entry),
	}
	for _, opt := range opts {
		opt(g)
	}
	g.wg.Add(1)
	go g.cleanupLoop()
	return g
}

// Allow checks if 1 request is allowed for the given key. Non-blocking.
func (g *GCRA) Allow(ctx context.Context, key string) ratelimit.Result {
	return g.AllowN(ctx, key, 1)
}

// AllowN checks if n requests are allowed. Consumes all n or none (atomic).
// Non-blocking. Safe for concurrent use.
func (g *GCRA) AllowN(_ context.Context, key string, n int) (res ratelimit.Result) {
	start := g.clock.Now()
	defer func() {
		g.record(res, start)
		if g.onDecision != nil {
			g.onDecision(key, res)
		}
	}()

	if err := ratelimit.ValidateKey(key); err != nil {
		return ratelimit.Result{Allowed: false, Algorithm: algorithmName}
	}
	if err := ratelimit.ValidateN(n); err != nil {
		return ratelimit.Result{Allowed: false, Algorithm: algorithmName}
	}
	if n > g.burst {
		res = ratelimit.Result{
			Allowed:   false,
			Limit:     g.limit,
			Remaining: 0,
			Algorithm: algorithmName,
		}
		setCost(&res, n)
		return res
	}
	e := g.getOrCreate(key)
	res = g.consume(e, n)
	if n != 1 {
		setCost(&res, n)
	}
	return res
}

// setCost records the consumed cost in res.Metadata under the "cost" key,
// allocating the map lazily so the n==1 hot path stays allocation-free.
func setCost(res *ratelimit.Result, cost int) {
	if res.Metadata == nil {
		res.Metadata = ratelimit.Metadata{}
	}
	res.Metadata["cost"] = cost
}

// record fires the configured metric.Recorder for a completed decision. With
// the default Nop recorder every call is an empty inlined method, so this stays
// allocation-free on the hot path.
func (g *GCRA) record(res ratelimit.Result, start time.Time) {
	if res.Allowed {
		g.rec.IncAllowed(algorithmName)
	} else {
		g.rec.IncDenied(algorithmName)
	}
	g.rec.ObserveDecision(algorithmName, g.clock.Now().Sub(start))
}

// Wait blocks until 1 request is allowed or ctx is cancelled.
func (g *GCRA) Wait(ctx context.Context, key string) error {
	return g.WaitN(ctx, key, 1)
}

// WaitN blocks until n requests are allowed or ctx is cancelled.
func (g *GCRA) WaitN(ctx context.Context, key string, n int) error {
	if err := ratelimit.ValidateKey(key); err != nil {
		return err
	}
	if err := ratelimit.ValidateN(n); err != nil {
		return err
	}
	// Impossible request: n exceeds the burst ceiling, so no amount of waiting
	// will ever let it through. Return immediately instead of looping forever (M-4).
	if n > g.burst {
		return &ratelimit.RateLimitError{
			Algorithm: algorithmName,
			Key:       key,
			Limit:     g.limit,
			Err:       fmt.Errorf("%w: n=%d exceeds burst=%d", ratelimit.ErrLimitExceeded, n, g.burst),
		}
	}
	e := g.getOrCreate(key)
	for {
		result := g.consume(e, n)
		if result.Allowed {
			return nil
		}
		wait := result.RetryAfter
		if wait <= 0 {
			wait = g.emissionInterval
		}
		timer := g.clock.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return &ratelimit.RateLimitError{
				Algorithm:  algorithmName,
				Key:        key,
				Limit:      g.limit,
				RetryAfter: wait,
				Err:        ratelimit.ErrContextDone,
			}
		case <-timer.C():
			// retry
		}
	}
}

// Peek returns current state without consuming a request.
func (g *GCRA) Peek(_ context.Context, key string) ratelimit.State {
	e := g.getOrCreate(key)
	e.mu.Lock()
	now := g.clock.Now()
	tat := e.tat
	e.mu.Unlock()

	// Compute remaining without consuming (same formula as consume)
	var remaining int
	var retryAfter time.Duration
	effectiveTAT := tat
	if effectiveTAT.IsZero() {
		effectiveTAT = now
	}
	// TAT if we were to make 1 request now
	tatAfterNext := maxTime(effectiveTAT, now).Add(g.emissionInterval)
	limitWindow := now.Add(g.emissionInterval * time.Duration(g.burst))
	if !tatAfterNext.After(limitWindow) {
		// Would be allowed — compute how many more could be sent right now
		slack := limitWindow.Sub(effectiveTAT)
		if slack >= 0 {
			remaining = int(slack / g.emissionInterval)
		}
		if remaining > g.burst {
			remaining = g.burst
		}
		if remaining < 0 {
			remaining = 0
		}
	} else {
		retryAfter = tatAfterNext.Sub(limitWindow)
	}

	return ratelimit.State{
		Key:       key,
		Algorithm: algorithmName,
		Limit:     g.limit,
		Remaining: remaining,
		ResetAt:   tat.Add(g.emissionInterval),
		Extra: map[string]any{
			"tat":               tat,
			"emission_interval": g.emissionInterval,
			"burst_offset":      g.burstOffset,
			"retry_after":       retryAfter,
		},
	}
}

// Reset removes all state for the given key.
func (g *GCRA) Reset(_ context.Context, key string) error {
	g.mu.Lock()
	delete(g.entries, key)
	g.mu.Unlock()
	return nil
}

// Close stops the background cleanup goroutine.
func (g *GCRA) Close() error {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return nil
	}
	g.closed = true
	g.mu.Unlock()
	// Signal shutdown AFTER releasing the mutex so cleanupLoop is never
	// blocked on g.mu.Lock() while Close holds it.
	close(g.done)
	g.wg.Wait()
	return nil
}

// String returns a human-readable description.
func (g *GCRA) String() string {
	return fmt.Sprintf("GCRA(limit=%d/%s, burst=%d, emissionInterval=%s)",
		g.limit, g.window, g.burst, g.emissionInterval)
}

// consume atomically computes TAT and checks/updates it.
// Uses integer nanosecond arithmetic — no floating point.
//
// GCRA formula (Brandur/Stripe variant):
//
//	TAT_candidate = max(stored_TAT, now) + emissionInterval * n
//	limit_window  = now + emissionInterval * burst  (maximum TAT we allow)
//	allowed       = TAT_candidate <= limit_window
//	RetryAfter    = TAT_candidate - limit_window     (when denied)
//	remaining     = floor((limit_window - TAT_candidate) / emissionInterval)
//
// For an absent key, stored_TAT = now (no backlog).
func (g *GCRA) consume(e *entry, n int) ratelimit.Result {
	e.mu.Lock()
	defer e.mu.Unlock()

	now := g.clock.Now()
	e.lastAccess = now

	// For new key, stored TAT starts at now (no backlog).
	storedTAT := e.tat
	if storedTAT.IsZero() {
		storedTAT = now
	}

	// TAT candidate for this request of n tokens
	tat := maxTime(storedTAT, now).Add(g.emissionInterval * time.Duration(n))

	// Maximum allowed TAT: now + burst * emissionInterval
	// This is the key insight: burst=1 means TAT must be <= now + emissionInterval
	// For a fresh key (storedTAT=now): tat = now + n*emissionInterval
	// limitWindow = now + burst*emissionInterval
	// allowed iff tat <= limitWindow → n <= burst ✓ (always true since we check n<=burst)
	limitWindow := now.Add(g.emissionInterval * time.Duration(g.burst))

	if tat.After(limitWindow) {
		// Denied — TAT would exceed the burst window
		retryAfter := tat.Sub(limitWindow)
		return ratelimit.Result{
			Allowed:    false,
			Limit:      g.limit,
			Remaining:  0,
			RetryAfter: retryAfter,
			Algorithm:  algorithmName,
		}
	}

	// Allowed — update stored TAT
	e.tat = tat

	// Remaining: how many single requests can still be made right now
	// remaining = floor((limitWindow - tat) / emissionInterval)
	slack := limitWindow.Sub(tat)
	remaining := int(slack / g.emissionInterval)
	if remaining < 0 {
		remaining = 0
	}
	if remaining > g.burst {
		remaining = g.burst
	}

	return ratelimit.Result{
		Allowed:    true,
		Limit:      g.limit,
		Remaining:  remaining,
		RetryAfter: 0,
		Algorithm:  algorithmName,
	}
}

// getOrCreate returns the entry for key, creating it if needed.
func (g *GCRA) getOrCreate(key string) *entry {
	g.mu.RLock()
	e, ok := g.entries[key]
	g.mu.RUnlock()
	if ok {
		return e
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if e, ok = g.entries[key]; ok {
		return e
	}
	now := g.clock.Now()
	e = &entry{lastAccess: now}
	// TAT starts as zero time — first request always allowed since max(zero, now) = now
	g.entries[key] = e
	return e
}

// cleanupLoop periodically evicts entries that haven't been accessed recently.
func (g *GCRA) cleanupLoop() {
	defer g.wg.Done()
	if g.idleClean <= 0 {
		return
	}
	ticker := g.clock.NewTicker(g.idleClean)
	defer ticker.Stop()
	for {
		select {
		case <-g.done:
			return
		case <-ticker.C():
			cutoff := g.clock.Now().Add(-g.idleClean)
			g.mu.Lock()
			for k, e := range g.entries {
				e.mu.Lock()
				idle := e.lastAccess.Before(cutoff)
				e.mu.Unlock()
				if idle {
					delete(g.entries, k)
				}
			}
			g.mu.Unlock()
		}
	}
}

// maxTime returns the later of two time values.
// Uses integer comparison — no floating point.
func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
