// Package bulkhead provides a Bulkhead pattern implementation that limits
// concurrent executions using a semaphore. It protects downstream resources
// from being overwhelmed by too many simultaneous requests.
package bulkhead

import (
	"context"
	"errors"
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

	name string
	rec  metric.Recorder
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
			return ErrBulkheadFull
		}
	} else {
		// Blocking with timeout: race between slot available, context done,
		// and the maxWait timer.
		timer := time.NewTimer(b.maxWait)
		defer timer.Stop()

		select {
		case b.sem <- struct{}{}:
			// Slot acquired.
		case <-ctx.Done():
			b.reject()
			return ctx.Err()
		case <-timer.C:
			b.reject()
			return ErrBulkheadFull
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
