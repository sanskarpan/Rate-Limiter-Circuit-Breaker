// Package eventstream provides a generic, zero-dependency publish/subscribe
// broker for streaming resilience events (rate-limit decisions, breaker
// transitions, bulkhead rejections, or any consumer-defined payload) to many
// independent subscribers.
//
// It is the reusable core that an application's ad-hoc fan-out layer (for
// example a WebSocket hub in a demo server) can adopt: publish once, and every
// live subscriber receives the event on its own buffered channel.
//
// Design guarantees:
//
//   - Non-blocking fan-out. [Broker.Publish] never blocks on a slow or full
//     subscriber. If a subscriber's buffer is full, the event is dropped for
//     that subscriber only (its dropped counter is incremented) — one slow
//     consumer can never stall the publisher or the other subscribers.
//
//   - Per-subscriber buffered channels. Each [Subscription] owns a buffered
//     channel sized at subscribe time, decoupling consumers from one another.
//
//   - Safe lifecycle. Subscribe / Unsubscribe / Publish / Close are all safe
//     for concurrent use. Closing the broker closes every subscriber channel
//     exactly once and rejects further publishes and subscribes.
//
// The zero value is not usable; construct a broker with [New].
package eventstream

import (
	"sync"
	"sync/atomic"
)

// defaultBuffer is the per-subscriber channel capacity used when Subscribe is
// called with a non-positive size.
const defaultBuffer = 16

// Event is a generic envelope carrying a typed payload plus a monotonically
// increasing sequence number assigned by the broker at publish time. Consumers
// can use Seq to detect gaps caused by slow-consumer drops.
type Event[T any] struct {
	// Seq is the broker-assigned sequence number, starting at 1 for the first
	// published event and incrementing by one per Publish call (across all
	// subscribers). A gap in the sequence a subscriber observes indicates
	// dropped events for that subscriber.
	Seq uint64
	// Payload is the consumer-supplied value.
	Payload T
}

// Subscription is a single consumer's handle onto a Broker. Read events from
// [Subscription.C]; when the broker is closed (or the subscription is
// unsubscribed) the channel is closed and ranging over it terminates.
type Subscription[T any] struct {
	id      uint64
	ch      chan Event[T]
	dropped atomic.Uint64
	broker  *Broker[T]

	closeOnce sync.Once
}

// C returns the receive-only channel this subscriber reads events from. The
// channel is closed when the subscription is unsubscribed or the broker is
// closed.
func (s *Subscription[T]) C() <-chan Event[T] { return s.ch }

// Dropped returns the number of events dropped for this subscriber because its
// buffer was full at publish time. It is safe to call concurrently.
func (s *Subscription[T]) Dropped() uint64 { return s.dropped.Load() }

// Unsubscribe removes this subscription from its broker and closes its channel.
// It is idempotent and safe to call concurrently. After Unsubscribe returns,
// the subscriber receives no further events.
func (s *Subscription[T]) Unsubscribe() { s.broker.remove(s) }

// closeCh closes the subscriber channel exactly once.
func (s *Subscription[T]) closeCh() {
	s.closeOnce.Do(func() { close(s.ch) })
}

// Broker is a generic, concurrency-safe pub/sub hub with non-blocking fan-out
// and slow-consumer drop semantics. Create one with [New].
type Broker[T any] struct {
	mu     sync.RWMutex
	subs   map[uint64]*Subscription[T]
	nextID uint64
	seq    atomic.Uint64
	closed bool
}

// New creates an empty Broker ready to accept subscribers and publishes.
func New[T any]() *Broker[T] {
	return &Broker[T]{subs: make(map[uint64]*Subscription[T])}
}

// Subscribe registers a new subscriber and returns its Subscription. The buffer
// argument sets the subscriber's channel capacity; a non-positive value uses a
// default. Subscribing to a closed broker returns a Subscription whose channel
// is already closed.
func (b *Broker[T]) Subscribe(buffer int) *Subscription[T] {
	if buffer <= 0 {
		buffer = defaultBuffer
	}
	sub := &Subscription[T]{
		ch:     make(chan Event[T], buffer),
		broker: b,
	}

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		sub.closeCh()
		return sub
	}
	b.nextID++
	sub.id = b.nextID
	b.subs[sub.id] = sub
	b.mu.Unlock()

	return sub
}

// Unsubscribe removes a subscription previously returned by Subscribe. It is
// equivalent to calling sub.Unsubscribe() and is provided for symmetry with
// Subscribe. A nil subscription is ignored.
func (b *Broker[T]) Unsubscribe(sub *Subscription[T]) {
	if sub != nil {
		b.remove(sub)
	}
}

// remove detaches a subscription and closes its channel exactly once.
func (b *Broker[T]) remove(sub *Subscription[T]) {
	b.mu.Lock()
	if _, ok := b.subs[sub.id]; ok {
		delete(b.subs, sub.id)
		b.mu.Unlock()
		sub.closeCh()
		return
	}
	b.mu.Unlock()
}

// Publish delivers payload to every current subscriber and returns the number
// of subscribers the event was successfully queued to (i.e. not dropped). It
// never blocks: a subscriber whose buffer is full is skipped and its dropped
// counter is incremented. Publishing to a closed broker is a no-op that returns
// 0. The event is assigned a fresh monotonically increasing sequence number.
func (b *Broker[T]) Publish(payload T) int {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return 0
	}
	// Sequence is assigned per Publish call; incrementing under the read lock
	// while closed==false is fine because Close takes the write lock.
	seq := b.seq.Add(1)
	evt := Event[T]{Seq: seq, Payload: payload}

	delivered := 0
	for _, sub := range b.subs {
		select {
		case sub.ch <- evt:
			delivered++
		default:
			sub.dropped.Add(1)
		}
	}
	b.mu.RUnlock()
	return delivered
}

// Len returns the current number of active subscribers.
func (b *Broker[T]) Len() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subs)
}

// Close shuts down the broker: it closes every active subscriber channel
// exactly once, drops all subscriptions, and rejects further Publish and
// Subscribe calls. Close is idempotent and safe to call concurrently.
func (b *Broker[T]) Close() {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.closed = true
	subs := b.subs
	b.subs = make(map[uint64]*Subscription[T])
	b.mu.Unlock()

	for _, sub := range subs {
		sub.closeCh()
	}
}
