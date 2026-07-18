// Package bulkhead provides a Bulkhead pattern implementation that limits
// concurrent executions using a semaphore. It protects downstream resources
// from being overwhelmed by too many simultaneous requests.
package bulkhead

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/metric"
)

// ErrBulkheadFull is returned when no slot is available within the configured
// maxWait duration.
var ErrBulkheadFull = errors.New("bulkhead: concurrent request limit exceeded")

// Bulkhead limits concurrent executions using a semaphore.
// maxConcurrency controls how many requests may run at once.
// maxWait controls how long Execute will block waiting for a slot;
// a zero value makes Execute non-blocking.
type Bulkhead struct {
	sem      chan struct{}
	maxWait  time.Duration
	inflight atomic.Int64
	rejected atomic.Int64

	// waiting is the number of callers currently blocked in the maxWait
	// wait (queue depth). It is incremented when a caller enters the wait
	// and decremented when it acquires a slot or gives up (timeout/cancel).
	waiting atomic.Int64

	// waitStats aggregates how long callers waited before acquiring or
	// giving up. It is allocation-light and thread-safe.
	waitStats waitAccumulator

	// now supplies the current time; it is time.Now by default and may be
	// overridden in tests via WithClock to make wait-time assertions
	// deterministic.
	now func() time.Time

	name string
	rec  metric.Recorder
}

// waitAccumulator is a small, thread-safe aggregate of caller wait times. It
// keeps running count/sum/min/max plus a coarse bucketed histogram so callers
// can reason about the wait-time distribution without per-observation
// allocation. All access is guarded by a single mutex; the wait path is off
// the hot (non-blocking) path, so contention here is inherently bounded by the
// number of concurrently-waiting callers.
type waitAccumulator struct {
	mu      sync.Mutex
	count   int64
	sum     time.Duration
	min     time.Duration
	max     time.Duration
	last    time.Duration
	buckets [len(waitBucketBounds) + 1]int64
}

// waitBucketBounds are the upper bounds (inclusive) of the wait-time histogram
// buckets. A final overflow bucket captures anything larger than the last
// bound. Chosen to span sub-millisecond stalls up to multi-second waits.
var waitBucketBounds = [...]time.Duration{
	1 * time.Millisecond,
	5 * time.Millisecond,
	10 * time.Millisecond,
	50 * time.Millisecond,
	100 * time.Millisecond,
	500 * time.Millisecond,
	1 * time.Second,
	5 * time.Second,
}

func (w *waitAccumulator) observe(d time.Duration) {
	if d < 0 {
		d = 0
	}
	w.mu.Lock()
	w.count++
	w.sum += d
	w.last = d
	if w.count == 1 || d < w.min {
		w.min = d
	}
	if d > w.max {
		w.max = d
	}
	idx := len(waitBucketBounds)
	for i, b := range waitBucketBounds {
		if d <= b {
			idx = i
			break
		}
	}
	w.buckets[idx]++
	w.mu.Unlock()
}

// WaitStats is a point-in-time snapshot of the caller wait-time distribution.
// Durations are zero when no caller has ever waited (Count == 0).
type WaitStats struct {
	// Count is the total number of callers that entered the wait (whether
	// they eventually acquired a slot or gave up).
	Count int64
	// Sum is the cumulative time all callers spent waiting.
	Sum time.Duration
	// Min and Max bound the observed per-caller wait times.
	Min time.Duration
	Max time.Duration
	// Last is the most recently observed wait time.
	Last time.Duration
	// Buckets is a histogram of wait times. Buckets[i] counts observations
	// whose wait was <= BucketBounds[i]; the final element counts waits
	// larger than the last bound (overflow).
	Buckets []int64
	// BucketBounds are the inclusive upper bounds for Buckets[:len-1].
	BucketBounds []time.Duration
}

// Avg returns the mean wait time, or 0 when no caller has waited.
func (s WaitStats) Avg() time.Duration {
	if s.Count == 0 {
		return 0
	}
	return s.Sum / time.Duration(s.Count)
}

func (w *waitAccumulator) snapshot() WaitStats {
	w.mu.Lock()
	defer w.mu.Unlock()
	buckets := make([]int64, len(w.buckets))
	copy(buckets, w.buckets[:])
	bounds := make([]time.Duration, len(waitBucketBounds))
	copy(bounds, waitBucketBounds[:])
	return WaitStats{
		Count:        w.count,
		Sum:          w.sum,
		Min:          w.min,
		Max:          w.max,
		Last:         w.last,
		Buckets:      buckets,
		BucketBounds: bounds,
	}
}

// Option configures a Bulkhead.
type Option func(*Bulkhead)

// WithName sets the bulkhead's name, used as the "name" label on emitted
// metrics. Defaults to "default".
func WithName(name string) Option {
	return func(b *Bulkhead) { b.name = name }
}

// WithRecorder wires a metric.Recorder so in-flight saturation and rejections
// are emitted. Defaults to metric.Default() (a no-op) when unset, keeping the
// hot path allocation-free. A nil recorder is ignored.
func WithRecorder(rec metric.Recorder) Option {
	return func(b *Bulkhead) {
		if rec != nil {
			b.rec = rec
		}
	}
}

// WithClock overrides the time source used to measure caller wait times.
// It is primarily intended for tests that need deterministic wait durations;
// production code should leave it unset (defaulting to time.Now). A nil clock
// is ignored.
func WithClock(now func() time.Time) Option {
	return func(b *Bulkhead) {
		if now != nil {
			b.now = now
		}
	}
}

// New creates a new Bulkhead with the given concurrency limit and wait timeout.
// maxConcurrency must be greater than zero.
// maxWait of 0 means non-blocking: if no slot is available the call is
// rejected immediately.
func New(maxConcurrency int, maxWait time.Duration, opts ...Option) *Bulkhead {
	if maxConcurrency <= 0 {
		panic("bulkhead: maxConcurrency must be greater than zero")
	}
	b := &Bulkhead{
		sem:     make(chan struct{}, maxConcurrency),
		maxWait: maxWait,
		now:     time.Now,
		name:    "default",
		rec:     metric.Default(),
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// Execute acquires a concurrency slot, runs fn with the provided context, then
// releases the slot. The slot is always released via defer, so it is safe even
// if fn panics. If no slot is available within the configured maxWait duration
// (or immediately when maxWait is 0), Execute returns ErrBulkheadFull.
func (b *Bulkhead) Execute(ctx context.Context, fn func(context.Context) error) error {
	// Try to acquire a slot.
	if b.maxWait == 0 {
		// Non-blocking: attempt to send without waiting.
		select {
		case b.sem <- struct{}{}:
			// Slot acquired.
		default:
			b.reject()
			return b.full()
		}
	} else {
		// Blocking with timeout: race between slot available, context done,
		// and the maxWait timer.
		timer := time.NewTimer(b.maxWait)
		defer timer.Stop()

		// Enter the wait: bump queue depth and start the wait-time clock.
		// The counter is decremented and the wait time recorded on every
		// exit path (acquire, cancel, timeout) below.
		start := b.now()
		b.waiting.Add(1)

		select {
		case b.sem <- struct{}{}:
			// Slot acquired.
			b.waiting.Add(-1)
			b.waitStats.observe(b.now().Sub(start))
		case <-ctx.Done():
			b.waiting.Add(-1)
			b.waitStats.observe(b.now().Sub(start))
			b.reject()
			return ctx.Err()
		case <-timer.C:
			b.waiting.Add(-1)
			b.waitStats.observe(b.now().Sub(start))
			b.reject()
			return b.full()
		}
	}

	// Slot is held. Always release it, even on panic.
	n := b.inflight.Add(1)
	b.rec.SetBulkheadInflight(b.name, int(n))
	defer func() {
		<-b.sem
		b.rec.SetBulkheadInflight(b.name, int(b.inflight.Add(-1)))
	}()

	return fn(ctx)
}

// reject records a rejection on both the internal counter and the recorder.
// (metric.Default() makes the recorder call a no-op.)
func (b *Bulkhead) reject() {
	b.rejected.Add(1)
	b.rec.IncBulkheadRejected(b.name)
}

// full builds the structured *BulkheadError describing this bulkhead's
// saturation at the moment of rejection. It wraps ErrBulkheadFull, so callers
// using errors.Is(err, ErrBulkheadFull) are unaffected.
func (b *Bulkhead) full() error {
	return &BulkheadError{
		Name:     b.name,
		Capacity: cap(b.sem),
		Inflight: int(b.inflight.Load()),
		Waiting:  int(b.waiting.Load()),
	}
}

// Name returns the bulkhead's configured name (the metric "name" label).
func (b *Bulkhead) Name() string { return b.name }

// Inflight returns the number of executions currently in progress.
func (b *Bulkhead) Inflight() int64 {
	return b.inflight.Load()
}

// Rejected returns the total number of requests that were rejected because no
// slot was available within the configured maxWait duration.
func (b *Bulkhead) Rejected() int64 {
	return b.rejected.Load()
}

// Waiting returns the number of callers currently blocked waiting for a slot
// (the "U" — utilization/saturation — signal in USE). It is always 0 when
// maxWait is 0 because such calls never enter the wait.
func (b *Bulkhead) Waiting() int {
	return int(b.waiting.Load())
}

// QueueDepth returns the current queue depth: the number of callers waiting for
// a slot. When maxWait is 0 there is no queue, so it is always 0. It is an
// alias for Waiting expressed in queueing terms.
//
// NOTE: this saturation signal is intentionally exposed only via accessors
// rather than metric.Recorder, which today carries only SetBulkheadInflight /
// IncBulkheadRejected. A future metric.Recorder extension (see ENHANCEMENTS
// §4.1/§4.6) can surface queue depth and the wait-time distribution as
// first-class metrics without changing this package's public API.
func (b *Bulkhead) QueueDepth() int {
	if b.maxWait == 0 {
		return 0
	}
	return int(b.waiting.Load())
}

// WaitStats returns a point-in-time snapshot of how long callers waited for a
// slot before acquiring one or giving up (timeout/cancellation). The returned
// value includes count, sum, min, max, last, and a coarse histogram; use
// WaitStats.Avg for the mean. It is safe to call concurrently.
func (b *Bulkhead) WaitStats() WaitStats {
	return b.waitStats.snapshot()
}
