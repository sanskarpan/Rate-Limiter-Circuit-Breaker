package bulkhead

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

// ErrQueueFull is returned by Submit when the worker queue is at capacity and
// the task cannot be accepted.
var ErrQueueFull = errors.New("thread pool: queue is full")

// ErrPoolClosed is returned by Submit when the pool has already been closed.
// A closed pool has no running workers, so accepting a task would leave the
// caller blocked forever waiting on a result that never arrives.
var ErrPoolClosed = errors.New("thread pool: pool is closed")

// task is an internal unit of work sent to a worker goroutine.
type task struct {
	ctx context.Context
	fn  func(ctx context.Context) error
	res chan error
}

// ThreadPool runs asynchronous tasks using a bounded pool of worker goroutines.
// Tasks that cannot be queued immediately because the queue is full are
// rejected with ErrQueueFull.
type ThreadPool struct {
	workers int
	queue   chan task
	done    chan struct{}
	wg      sync.WaitGroup
	closed  atomic.Bool
}

// NewThreadPool creates a new ThreadPool with the specified number of worker
// goroutines and task queue depth. Workers are started immediately and run
// until Close is called.
func NewThreadPool(workers, queueSize int) *ThreadPool {
	if workers <= 0 {
		panic("thread pool: workers must be greater than zero")
	}
	if queueSize < 0 {
		panic("thread pool: queueSize must be non-negative")
	}

	tp := &ThreadPool{
		workers: workers,
		queue:   make(chan task, queueSize),
		done:    make(chan struct{}),
	}

	for i := 0; i < workers; i++ {
		tp.wg.Add(1)
		go tp.run()
	}

	return tp
}

// run is the main loop executed by each worker goroutine. It processes tasks
// from the queue until the done channel is closed.
func (tp *ThreadPool) run() {
	defer tp.wg.Done()
	for {
		select {
		case t, ok := <-tp.queue:
			if !ok {
				return
			}
			// Execute the task and send the result back.
			t.res <- t.fn(t.ctx)
		case <-tp.done:
			// Drain any remaining tasks before exiting so callers whose
			// Submit returned a result channel still receive a value.
			for {
				select {
				case t, ok := <-tp.queue:
					if !ok {
						return
					}
					t.res <- t.fn(t.ctx)
				default:
					return
				}
			}
		}
	}
}

// Submit enqueues fn for asynchronous execution by the worker pool. It returns
// a receive-only channel that will carry exactly one value: the error (or nil)
// returned by fn. If the queue is full, Submit returns nil and ErrQueueFull
// without blocking.
func (tp *ThreadPool) Submit(ctx context.Context, fn func(context.Context) error) (<-chan error, error) {
	// Reject early if the pool is closed: workers are gone, so a queued task
	// would never be consumed and the caller would block on the result forever.
	if tp.closed.Load() {
		return nil, ErrPoolClosed
	}

	res := make(chan error, 1)
	t := task{ctx: ctx, fn: fn, res: res}

	select {
	case tp.queue <- t:
		// Re-check after enqueue: Close may have raced in and won the wg.Wait
		// before this task landed. If so, report closed so the caller doesn't
		// wait on a result no worker will produce.
		if tp.closed.Load() {
			return nil, ErrPoolClosed
		}
		return res, nil
	case <-tp.done:
		return nil, ErrPoolClosed
	default:
		return nil, ErrQueueFull
	}
}

// Close signals all worker goroutines to stop and waits for them to finish.
// After Close returns, no new tasks will be started. Submit must not be called
// after Close.
func (tp *ThreadPool) Close() {
	// Mark closed before signalling workers so any concurrent Submit that
	// observes the flag bails out instead of enqueuing an orphan task.
	if !tp.closed.CompareAndSwap(false, true) {
		// Already closed; closing done again would panic.
		return
	}
	close(tp.done)
	tp.wg.Wait()
}
