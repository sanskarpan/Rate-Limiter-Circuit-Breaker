// Package leakybucket implements the Leaky Bucket rate limiting algorithm.
//
// Theory: A bucket has a fixed queue capacity. Requests enter the bucket
// (queue) and are processed (leaked) at a constant rate. If the bucket is
// full, incoming requests are immediately rejected.
//
// Properties vs Token Bucket:
//   - Token bucket permits bursting; leaky bucket enforces constant output rate.
//   - Leaky bucket smooths bursty traffic to a steady stream.
//   - Allow() is non-blocking: either queued or denied immediately.
//   - Wait() blocks until the queued request is processed.
//
// Time complexity: O(1) per Allow call.
// Space complexity: O(keys * queueCapacity).
//
// All methods on LeakyBucket are safe for concurrent use.
//
// Caveats (L-8):
//   - Context cancellation semantics: once a request has been enqueued, its queue
//     slot is occupied until the background leaker dequeues it. If the caller's
//     context is cancelled while waiting, the leaker still consumes the token
//     later and denies it — the slot is not freed early. Callers that cancel
//     mid-wait may therefore transiently hold a queue slot.
//   - Metrics (Peek queue_depth / Remaining / estimated wait) are derived from
//     len(chan), which is a racy point-in-time snapshot: concurrent enqueues and
//     leaks can change it immediately after it is read. These figures are best
//     treated as advisory, not exact.
package leakybucket

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/metric"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
)

const algorithmName = "leaky_bucket"

// token represents a queued request.
type token struct {
	result chan ratelimit.Result
	ctx    context.Context //nolint:containedctx
}

// keyQueue holds per-key state: a buffered channel of pending tokens plus an
// optional priority heap of waiters (see priority.go). The channel is the
// default FIFO path; the heap is allocated lazily on the first priority waiter.
type keyQueue struct {
	ch         chan token
	capacity   int
	leakRate   float64 // requests per second
	lastAccess time.Time
	mu         sync.Mutex

	// pq is the per-key priority heap of waiters, nil until the first priority
	// waiter arrives. All access is serialized under mu. nextSeq is a per-key
	// arrival counter used as a FIFO tie-breaker within a priority level.
	pq      *priorityQueue
	nextSeq uint64
}

// depthLocked returns the current queue depth (channel + priority heap). Callers
// must hold q.mu.
func (q *keyQueue) depthLocked() int {
	d := len(q.ch)
	if q.pq != nil {
		d += q.pq.Len()
	}
	return d
}

// ensurePQ lazily allocates the priority heap. Callers must hold q.mu.
func (q *keyQueue) ensurePQ() {
	if q.pq == nil {
		pq := make(priorityQueue, 0, 4)
		q.pq = &pq
	}
}

// popReadyPriorityLocked pops and returns the highest-priority waiter whose
// context is still live, discarding any cancelled waiters it encounters along
// the way (they are denied by the caller's own cancellation path, so here they
// are simply dropped). Returns nil when the heap has no live waiter. Callers
// must hold q.mu.
func (q *keyQueue) popReadyPriorityLocked() *pqToken {
	if q.pq == nil {
		return nil
	}
	for q.pq.Len() > 0 {
		t := heapPop(q.pq)
		if t.ctx.Err() != nil {
			// Cancelled while queued: its own AllowP cancellation branch will (or
			// already did) return a denial; drop it without consuming a leak slot.
			continue
		}
		return t
	}
	return nil
}

// LeakyBucket implements the Limiter interface using the leaky bucket algorithm.
// All methods are safe for concurrent use.
type LeakyBucket struct {
	capacity  int
	leakRate  float64 // requests per second
	idleClean time.Duration

	mu     sync.RWMutex
	queues map[string]*keyQueue

	clock  clock.Clock
	rec    metric.Recorder
	done   chan struct{}
	wg     sync.WaitGroup
	closed bool

	// onDecision, when non-nil, is fired synchronously after every Allow/AllowN
	// decision. nil by default so the hot path stays a single nil check.
	onDecision func(key string, r ratelimit.Result)
}

// New creates a new LeakyBucket with the given queue capacity and leak rate (requests/second).
//
// It panics if capacity <= 0 or leakRate <= 0 (M-5). A non-positive leak rate
// would mean the queue never drains, so every Wait/WaitN would block forever;
// a non-positive capacity cannot hold any request. This mirrors the standard
// library convention (e.g. time.NewTicker panics on a non-positive interval).
func New(capacity int, leakRate float64, opts ...Option) *LeakyBucket {
	if capacity <= 0 {
		panic(fmt.Sprintf("leakybucket.New: capacity must be > 0, got %d", capacity))
	}
	if leakRate <= 0 {
		panic(fmt.Sprintf("leakybucket.New: leakRate must be > 0, got %v", leakRate))
	}
	lb := &LeakyBucket{
		capacity:  capacity,
		leakRate:  leakRate,
		idleClean: 5 * time.Minute,
		clock:     clock.RealClock{},
		rec:       metric.Default(),
		done:      make(chan struct{}),
		queues:    make(map[string]*keyQueue),
	}
	for _, opt := range opts {
		opt(lb)
	}
	lb.wg.Add(1)
	go lb.cleanupLoop()
	return lb
}

// record fires the configured metric.Recorder for a completed decision. With
// the default Nop recorder every call is an empty inlined method, so this stays
// allocation-free on the hot path.
func (lb *LeakyBucket) record(res ratelimit.Result, start time.Time) {
	if res.Allowed {
		lb.rec.IncAllowed(algorithmName)
	} else {
		lb.rec.IncDenied(algorithmName)
	}
	lb.rec.ObserveDecision(algorithmName, lb.clock.Now().Sub(start))
}

// setCost records the consumed cost in res.Metadata under the "cost" key,
// allocating the map lazily so the n==1 hot path stays allocation-free.
func setCost(res *ratelimit.Result, cost int) {
	if res.Metadata == nil {
		res.Metadata = ratelimit.Metadata{}
	}
	res.Metadata["cost"] = cost
}

// Allow attempts to queue a request for key. Non-blocking.
// If the queue is full, returns Allowed=false immediately.
func (lb *LeakyBucket) Allow(ctx context.Context, key string) (res ratelimit.Result) {
	start := lb.clock.Now()
	defer func() {
		lb.record(res, start)
		if lb.onDecision != nil {
			lb.onDecision(key, res)
		}
	}()
	res = lb.allow1(ctx, key)
	return res
}

// allow1 is the unrecorded single-request path, shared by Allow and the n==1
// fast path of AllowN so a decision is recorded exactly once.
func (lb *LeakyBucket) allow1(ctx context.Context, key string) ratelimit.Result {
	if err := ratelimit.ValidateKey(key); err != nil {
		return ratelimit.Result{Allowed: false, Algorithm: algorithmName}
	}
	q := lb.getOrCreate(key)
	result := make(chan ratelimit.Result, 1)
	t := token{result: result, ctx: ctx}

	// Enqueue under q.mu so this single-token enqueue is serialized against the
	// atomic batch enqueue in AllowN (H-6). Both paths are the only senders into
	// q.ch, so serializing them guarantees the queue never exceeds capacity and
	// AllowN's capacity check cannot be invalidated by a concurrent Allow.
	q.mu.Lock()
	if q.depthLocked() >= lb.capacity {
		qDepth := q.depthLocked()
		q.mu.Unlock()
		retryAfter := time.Duration(float64(qDepth+1) / lb.leakRate * float64(time.Second))
		return ratelimit.Result{
			Allowed:    false,
			Limit:      lb.capacity,
			Remaining:  0,
			RetryAfter: retryAfter,
			Algorithm:  algorithmName,
		}
	}
	q.ch <- t
	q.mu.Unlock()

	// Queued — wait for leaker to process it.
	select {
	case r := <-result:
		return r
	case <-ctx.Done():
		return ratelimit.Result{
			Allowed:    false,
			Limit:      lb.capacity,
			Remaining:  0,
			RetryAfter: lb.estimatedWait(q),
			Algorithm:  algorithmName,
		}
	}
}

// AllowN checks if n requests can be queued atomically.
// All n tokens are enqueued or none — if fewer than n slots are free, the
// entire batch is denied immediately without enqueuing any token.
func (lb *LeakyBucket) AllowN(ctx context.Context, key string, n int) (res ratelimit.Result) {
	start := lb.clock.Now()
	defer func() {
		if n != 1 {
			setCost(&res, n)
		}
		lb.record(res, start)
		if lb.onDecision != nil {
			lb.onDecision(key, res)
		}
	}()

	if err := ratelimit.ValidateKey(key); err != nil {
		return ratelimit.Result{Allowed: false, Algorithm: algorithmName}
	}
	if err := ratelimit.ValidateN(n); err != nil {
		return ratelimit.Result{Allowed: false, Algorithm: algorithmName}
	}
	if n > lb.capacity {
		return ratelimit.Result{
			Allowed:   false,
			Limit:     lb.capacity,
			Remaining: 0,
			Algorithm: algorithmName,
		}
	}
	if n == 1 {
		return lb.allow1(ctx, key)
	}

	q := lb.getOrCreate(key)

	// Prepare the n result channels up front (no lock needed).
	results := make([]chan ratelimit.Result, n)
	for i := range results {
		results[i] = make(chan ratelimit.Result, 1)
	}

	// H-6: hold q.mu across BOTH the capacity check AND the enqueue so the
	// operation is truly atomic (all-or-nothing). Previously q.mu was released
	// before the enqueue loop, so a concurrent AllowN could steal slots between
	// the check and the sends, leaving some of this batch's tokens stranded in
	// q.ch even though we reported the whole batch denied. The only sender into
	// q.ch is AllowN under q.mu (Allow uses a non-blocking send, which can only
	// ever succeed when there is genuine free space), so holding q.mu here means
	// no interleaved AllowN can consume the slots we just verified.
	q.mu.Lock()
	if lb.capacity-q.depthLocked() < n {
		qDepth := q.depthLocked()
		q.mu.Unlock()
		retryAfter := time.Duration(float64(qDepth+1) / lb.leakRate * float64(time.Second))
		return ratelimit.Result{
			Allowed:    false,
			Limit:      lb.capacity,
			Remaining:  0,
			RetryAfter: retryAfter,
			Algorithm:  algorithmName,
		}
	}
	// Capacity is guaranteed available and cannot be taken from under us while we
	// hold q.mu, so every send succeeds. Use a plain (blocking) send; it never
	// actually blocks because we verified free space.
	for i := 0; i < n; i++ {
		q.ch <- token{result: results[i], ctx: ctx}
	}
	q.mu.Unlock()

	// Wait for all n tokens to be processed.
	for i := 0; i < n; i++ {
		select {
		case r := <-results[i]:
			if !r.Allowed {
				return r
			}
		case <-ctx.Done():
			return ratelimit.Result{
				Allowed:    false,
				Limit:      lb.capacity,
				Remaining:  0,
				RetryAfter: lb.estimatedWait(q),
				Algorithm:  algorithmName,
			}
		}
	}

	remaining := lb.capacity - len(q.ch)
	if remaining < 0 {
		remaining = 0
	}
	return ratelimit.Result{
		Allowed:   true,
		Limit:     lb.capacity,
		Remaining: remaining,
		Algorithm: algorithmName,
	}
}

// Wait blocks until the request is processed (token consumed from queue) or ctx is cancelled.
//
// Note on partial-consume semantics: a queued token that has already been placed
// in the leaker's queue is consumed from the queue's perspective even if the
// caller's context is cancelled before the leaker processes it (the leaker will
// later dequeue it and, seeing the cancelled context, deny it). Callers that
// cancel mid-wait may therefore have occupied a queue slot briefly.
func (lb *LeakyBucket) Wait(ctx context.Context, key string) error {
	if err := ratelimit.ValidateKey(key); err != nil {
		return err
	}
	// Defense in depth (M-4): a zero leak rate means the queue never drains, so a
	// queued token would block forever. New already rejects leakRate <= 0, but
	// guard here too so no code path can hang indefinitely on context.Background().
	if lb.leakRate <= 0 {
		return &ratelimit.RateLimitError{
			Algorithm: algorithmName,
			Key:       key,
			Limit:     lb.capacity,
			Err:       fmt.Errorf("%w: leak rate is zero, queue never drains", ratelimit.ErrLimitExceeded),
		}
	}
	result := lb.Allow(ctx, key)
	if result.Allowed {
		return nil
	}
	if ctx.Err() != nil {
		return &ratelimit.RateLimitError{
			Algorithm:  algorithmName,
			Key:        key,
			Limit:      lb.capacity,
			RetryAfter: result.RetryAfter,
			Err:        ratelimit.ErrContextDone,
		}
	}
	return &ratelimit.RateLimitError{
		Algorithm:  algorithmName,
		Key:        key,
		Limit:      lb.capacity,
		RetryAfter: result.RetryAfter,
		Err:        ratelimit.ErrLimitExceeded,
	}
}

// WaitN blocks until n tokens are enqueued and processed, or ctx is cancelled.
//
// It routes through the atomic AllowN batch (post H-6) rather than looping n
// single Waits, so the whole batch of n slots is reserved all-or-nothing: on a
// full queue it retries the entire batch instead of partially occupying slots.
// If n exceeds capacity the request is impossible and an error is returned
// immediately (M-4) rather than looping forever.
//
// Partial-consume note: as with Wait, if the context is cancelled after a batch
// has been enqueued, those tokens are still dequeued (and denied) by the leaker,
// so they briefly occupy queue slots.
func (lb *LeakyBucket) WaitN(ctx context.Context, key string, n int) error {
	if err := ratelimit.ValidateKey(key); err != nil {
		return err
	}
	if err := ratelimit.ValidateN(n); err != nil {
		return err
	}
	// Impossible request or dead queue: fail fast instead of looping forever (M-4).
	if n > lb.capacity {
		return &ratelimit.RateLimitError{
			Algorithm: algorithmName,
			Key:       key,
			Limit:     lb.capacity,
			Err:       fmt.Errorf("%w: n=%d exceeds capacity=%d", ratelimit.ErrLimitExceeded, n, lb.capacity),
		}
	}
	if lb.leakRate <= 0 {
		return &ratelimit.RateLimitError{
			Algorithm: algorithmName,
			Key:       key,
			Limit:     lb.capacity,
			Err:       fmt.Errorf("%w: leak rate is zero, queue never drains", ratelimit.ErrLimitExceeded),
		}
	}
	for {
		result := lb.AllowN(ctx, key, n)
		if result.Allowed {
			return nil
		}
		if ctx.Err() != nil {
			return &ratelimit.RateLimitError{
				Algorithm:  algorithmName,
				Key:        key,
				Limit:      lb.capacity,
				RetryAfter: result.RetryAfter,
				Err:        ratelimit.ErrContextDone,
			}
		}
		// Queue was full for the whole batch — wait roughly one slot's drain time
		// then retry the atomic batch.
		wait := result.RetryAfter
		if wait <= 0 {
			wait = time.Duration(float64(time.Second) / lb.leakRate)
		}
		timer := lb.clock.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return &ratelimit.RateLimitError{
				Algorithm:  algorithmName,
				Key:        key,
				Limit:      lb.capacity,
				RetryAfter: wait,
				Err:        ratelimit.ErrContextDone,
			}
		case <-timer.C():
			// retry
		}
	}
}

// Peek returns current state without consuming a slot.
//
// queue_depth counts both the FIFO channel and the priority heap (see
// priority.go), so it reflects the true backlog under a mix of Allow and AllowP
// waiters. It remains an advisory point-in-time snapshot per the package caveats.
func (lb *LeakyBucket) Peek(_ context.Context, key string) ratelimit.State {
	q := lb.getOrCreate(key)
	q.mu.Lock()
	qDepth := q.depthLocked()
	q.mu.Unlock()
	remaining := lb.capacity - qDepth
	if remaining < 0 {
		remaining = 0
	}
	waitUntilFull := time.Duration(float64(lb.capacity) / lb.leakRate * float64(time.Second))
	return ratelimit.State{
		Key:       key,
		Algorithm: algorithmName,
		Limit:     lb.capacity,
		Remaining: remaining,
		ResetAt:   lb.clock.Now().Add(waitUntilFull),
		Extra: map[string]any{
			"queue_depth":       qDepth,
			"queue_capacity":    lb.capacity,
			"leak_rate_per_s":   lb.leakRate,
			"estimated_wait_ms": int64(lb.estimatedWait(q).Milliseconds()),
		},
	}
}

// Reset removes all state for the given key.
func (lb *LeakyBucket) Reset(_ context.Context, key string) error {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	if q, ok := lb.queues[key]; ok {
		// Drain the queue, denying all pending requests
		for {
			select {
			case t := <-q.ch:
				t.result <- ratelimit.Result{
					Allowed:   false,
					Limit:     lb.capacity,
					Remaining: 0,
					Algorithm: algorithmName,
				}
			default:
				goto drained
			}
		}
	drained:
		lb.drainPriority(q)
		delete(lb.queues, key)
	}
	return nil
}

// Close stops the background leaker goroutine.
func (lb *LeakyBucket) Close() error {
	lb.mu.Lock()
	if lb.closed {
		lb.mu.Unlock()
		return nil
	}
	lb.closed = true
	lb.mu.Unlock()
	// Signal shutdown AFTER releasing the mutex so background goroutines are never
	// blocked on lb.mu.Lock() while Close holds it.
	close(lb.done)
	lb.wg.Wait()
	return nil
}

// String returns a human-readable description.
func (lb *LeakyBucket) String() string {
	return fmt.Sprintf("LeakyBucket(capacity=%d, leakRate=%.0f/s)", lb.capacity, lb.leakRate)
}

// getOrCreate returns the queue for key, creating it if needed.
func (lb *LeakyBucket) getOrCreate(key string) *keyQueue {
	lb.mu.RLock()
	q, ok := lb.queues[key]
	lb.mu.RUnlock()
	if ok {
		return q
	}
	lb.mu.Lock()
	defer lb.mu.Unlock()
	if q, ok = lb.queues[key]; ok {
		return q
	}
	now := lb.clock.Now()
	q = &keyQueue{
		ch:         make(chan token, lb.capacity),
		capacity:   lb.capacity,
		leakRate:   lb.leakRate,
		lastAccess: now,
	}
	lb.queues[key] = q
	// Start a per-key leaker goroutine
	lb.wg.Add(1)
	go lb.leaker(key, q)
	return q
}

// leaker processes requests from the key's queue at the configured leak rate.
func (lb *LeakyBucket) leaker(key string, q *keyQueue) {
	defer lb.wg.Done()
	if lb.leakRate <= 0 {
		return
	}
	interval := time.Duration(float64(time.Second) / lb.leakRate)
	ticker := lb.clock.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-lb.done:
			// Drain remaining tokens — deny them
			lb.drainQueue(q)
			return
		case <-ticker.C():
			// Priority-first: a live priority-heap waiter (see priority.go) is
			// served before the FIFO channel. Only when the heap has no live
			// waiter does this tick fall back to the channel, so the default
			// (channel-only) path is unchanged. Exactly one token is served per
			// tick either way, preserving the constant leak rate.
			q.mu.Lock()
			q.lastAccess = lb.clock.Now()
			pt := q.popReadyPriorityLocked()
			if pt != nil {
				remaining := lb.capacity - q.depthLocked() - 1
				q.mu.Unlock()
				if remaining < 0 {
					remaining = 0
				}
				pt.result <- ratelimit.Result{
					Allowed:   true,
					Limit:     lb.capacity,
					Remaining: remaining,
					Algorithm: algorithmName,
				}
				lb.mu.RLock()
				_, exists := lb.queues[key]
				lb.mu.RUnlock()
				if !exists {
					return
				}
				continue
			}
			q.mu.Unlock()

			select {
			case t := <-q.ch:
				// Update last access
				q.mu.Lock()
				q.lastAccess = lb.clock.Now()
				q.mu.Unlock()
				// Check if context still valid
				if t.ctx.Err() != nil {
					// Context cancelled — deny and continue
					t.result <- ratelimit.Result{
						Allowed:    false,
						Limit:      lb.capacity,
						Remaining:  0,
						RetryAfter: 0,
						Algorithm:  algorithmName,
					}
				} else {
					remaining := lb.capacity - len(q.ch) - 1
					if remaining < 0 {
						remaining = 0
					}
					t.result <- ratelimit.Result{
						Allowed:    true,
						Limit:      lb.capacity,
						Remaining:  remaining,
						RetryAfter: 0,
						Algorithm:  algorithmName,
					}
				}
				// Check if this key is still needed — cleanup idle queues
				lb.mu.RLock()
				_, exists := lb.queues[key]
				lb.mu.RUnlock()
				if !exists {
					return
				}
			default:
				// Nothing queued — idle
			}
		}
	}
}

// drainQueue denies all pending requests in a queue (both the FIFO channel and
// the priority heap).
func (lb *LeakyBucket) drainQueue(q *keyQueue) {
	for {
		select {
		case t := <-q.ch:
			t.result <- ratelimit.Result{
				Allowed:   false,
				Limit:     lb.capacity,
				Remaining: 0,
				Algorithm: algorithmName,
			}
		default:
			lb.drainPriority(q)
			return
		}
	}
}

// drainPriority denies and clears every waiter currently parked in the key's
// priority heap. Safe to call with q.mu unheld (it takes the lock itself); the
// per-token result channels are buffered so the sends never block.
func (lb *LeakyBucket) drainPriority(q *keyQueue) {
	q.mu.Lock()
	if q.pq == nil || q.pq.Len() == 0 {
		q.mu.Unlock()
		return
	}
	pending := make([]*pqToken, 0, q.pq.Len())
	for q.pq.Len() > 0 {
		pending = append(pending, heapPop(q.pq))
	}
	q.mu.Unlock()
	for _, t := range pending {
		t.result <- ratelimit.Result{
			Allowed:   false,
			Limit:     lb.capacity,
			Remaining: 0,
			Algorithm: algorithmName,
		}
	}
}

// estimatedWait returns estimated wait time for a queued request.
func (lb *LeakyBucket) estimatedWait(q *keyQueue) time.Duration {
	depth := len(q.ch)
	if depth == 0 {
		return 0
	}
	return time.Duration(float64(depth) / lb.leakRate * float64(time.Second))
}

// cleanupLoop periodically evicts queues that haven't been accessed recently.
func (lb *LeakyBucket) cleanupLoop() {
	defer lb.wg.Done()
	if lb.idleClean <= 0 {
		return
	}
	ticker := lb.clock.NewTicker(lb.idleClean)
	defer ticker.Stop()
	for {
		select {
		case <-lb.done:
			return
		case <-ticker.C():
			cutoff := lb.clock.Now().Add(-lb.idleClean)
			lb.mu.Lock()
			for k, q := range lb.queues {
				q.mu.Lock()
				idle := q.lastAccess.Before(cutoff) && q.depthLocked() == 0
				q.mu.Unlock()
				if idle {
					delete(lb.queues, k)
				}
			}
			lb.mu.Unlock()
		}
	}
}
