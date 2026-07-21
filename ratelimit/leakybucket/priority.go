package leakybucket

// Priority-aware waiting for the leaky bucket (ENHANCEMENTS §1.7).
//
// The default Allow/AllowN/Wait/WaitN path enqueues a request into the per-key
// buffered channel q.ch and the leaker serves it in arrival (FIFO) order. That
// behaviour is unchanged and is exactly equivalent to priority 0.
//
// AllowP/WaitP add an OPTIONAL per-key max-heap of waiters keyed by priority.
// On each leak tick the leaker serves the highest-priority live waiter from the
// heap first, and only falls back to the FIFO channel when the heap is empty.
// Higher priority values are served first; within a single priority level
// waiters are served in arrival order (FIFO), which keeps the scheme fair within
// a level. The heap and the channel share the bucket's capacity: a request is
// admitted only when len(q.ch)+heap size < capacity, so the two paths can never
// jointly exceed the configured queue depth.
//
// Cancellation: a heap waiter whose context is cancelled while waiting removes
// itself from the heap under q.mu, so cancelled waiters never occupy a slot and
// are never served. The design is deadlock-free — the leaker and the waiters
// only ever contend on q.mu for O(log n) heap operations and never block while
// holding it.

import (
	"container/heap"
	"context"
	"fmt"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
)

// Compile-time assertion: LeakyBucket must satisfy the PriorityLimiter interface.
var _ ratelimit.PriorityLimiter = (*LeakyBucket)(nil)

// pqToken is a single priority waiter parked in a key's priority heap.
type pqToken struct {
	result   chan ratelimit.Result
	ctx      context.Context //nolint:containedctx
	priority int
	// seq is a per-key monotonically increasing arrival sequence number used as a
	// FIFO tie-breaker so that, within one priority level, the earliest arrival is
	// served first (fairness within a level).
	seq uint64
	// index is the token's position in the heap, maintained by heap.Interface so a
	// cancelled waiter in the middle of the heap can be removed in O(log n).
	index int
}

// priorityQueue is a max-heap of pqToken ordered by (priority desc, seq asc).
// It is never accessed concurrently: every operation happens under the owning
// keyQueue's mu.
type priorityQueue []*pqToken

func (pq priorityQueue) Len() int { return len(pq) }

func (pq priorityQueue) Less(i, j int) bool {
	if pq[i].priority != pq[j].priority {
		return pq[i].priority > pq[j].priority // higher priority first
	}
	return pq[i].seq < pq[j].seq // earlier arrival first within a level
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *priorityQueue) Push(x any) {
	t := x.(*pqToken)
	t.index = len(*pq)
	*pq = append(*pq, t)
}

func (pq *priorityQueue) Pop() any {
	old := *pq
	n := len(old)
	t := old[n-1]
	old[n-1] = nil // avoid memory leak
	t.index = -1
	*pq = old[:n-1]
	return t
}

// heapPop pops the top (highest-priority) token from pq. Callers must hold the
// owning keyQueue's mu. It is a thin typed wrapper over container/heap.Pop.
func heapPop(pq *priorityQueue) *pqToken {
	return heap.Pop(pq).(*pqToken)
}

// AllowP is the priority-aware form of Allow. It behaves exactly like Allow but
// the request is parked in the key's priority heap: under contention, requests
// with a higher priority value acquire the next leaked slot first. Priority 0 is
// the default and is served after any positive-priority waiter but ahead of the
// FIFO channel only when the heap is chosen — i.e. AllowP(ctx, key, 0) is still
// prioritised over plain Allow callers on the same key because heap waiters are
// served before channel waiters.
//
// Use plain Allow/Wait when you do not need prioritisation; they remain the
// zero-overhead default (no heap is ever allocated for a key until its first
// priority waiter arrives).
func (lb *LeakyBucket) AllowP(ctx context.Context, key string, priority int) (res ratelimit.Result) {
	start := lb.clock.Now()
	defer func() {
		lb.record(res, start)
		if lb.onDecision != nil {
			lb.onDecision(key, res)
		}
	}()
	res = lb.allowP1(ctx, key, priority)
	return res
}

// allowP1 enqueues a single request into the key's priority heap and waits for
// the leaker to serve or the context to be cancelled.
func (lb *LeakyBucket) allowP1(ctx context.Context, key string, priority int) ratelimit.Result {
	if err := ratelimit.ValidateKey(key); err != nil {
		return ratelimit.Result{Allowed: false, Algorithm: algorithmName}
	}
	q := lb.getOrCreate(key)
	result := make(chan ratelimit.Result, 1)

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
	q.ensurePQ()
	t := &pqToken{result: result, ctx: ctx, priority: priority, seq: q.nextSeq}
	q.nextSeq++
	heap.Push(q.pq, t)
	q.mu.Unlock()

	select {
	case r := <-result:
		return r
	case <-ctx.Done():
		// Remove ourselves from the heap so a cancelled waiter never occupies a
		// slot. If the leaker already popped us (t.index < 0) the result channel
		// carries the (denied) decision; drain it non-blockingly.
		q.mu.Lock()
		if t.index >= 0 && t.index < q.pq.Len() && (*q.pq)[t.index] == t {
			heap.Remove(q.pq, t.index)
			q.mu.Unlock()
		} else {
			q.mu.Unlock()
			select {
			case <-result:
			default:
			}
		}
		return ratelimit.Result{
			Allowed:    false,
			Limit:      lb.capacity,
			Remaining:  0,
			RetryAfter: lb.estimatedWait(q),
			Algorithm:  algorithmName,
		}
	}
}

// WaitP is the priority-aware form of Wait. Higher-priority callers acquire the
// next leaked slot before lower-priority ones on the same key. Wait is exactly
// equivalent to WaitP(ctx, key, 0) except that plain Wait uses the FIFO channel
// path while WaitP uses the priority heap (heap waiters are served before
// channel waiters).
func (lb *LeakyBucket) WaitP(ctx context.Context, key string, priority int) error {
	if err := ratelimit.ValidateKey(key); err != nil {
		return err
	}
	if lb.leakRate <= 0 {
		return &ratelimit.RateLimitError{
			Algorithm: algorithmName,
			Key:       key,
			Limit:     lb.capacity,
			Err:       fmt.Errorf("%w: leak rate is zero, queue never drains", ratelimit.ErrLimitExceeded),
		}
	}
	result := lb.AllowP(ctx, key, priority)
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
