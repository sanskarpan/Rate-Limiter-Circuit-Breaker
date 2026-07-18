package eventstream_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/eventstream"
)

func TestPublish_FanOut(t *testing.T) {
	t.Parallel()
	b := eventstream.New[string]()
	defer b.Close()

	const nSubs = 3
	subs := make([]*eventstream.Subscription[string], nSubs)
	for i := range subs {
		subs[i] = b.Subscribe(4)
	}
	if got := b.Len(); got != nSubs {
		t.Fatalf("Len = %d, want %d", got, nSubs)
	}

	delivered := b.Publish("hello")
	if delivered != nSubs {
		t.Fatalf("Publish delivered=%d, want %d", delivered, nSubs)
	}

	for i, s := range subs {
		select {
		case evt := <-s.C():
			if evt.Payload != "hello" {
				t.Errorf("sub %d payload = %q", i, evt.Payload)
			}
			if evt.Seq != 1 {
				t.Errorf("sub %d seq = %d, want 1", i, evt.Seq)
			}
		default:
			t.Errorf("sub %d received nothing", i)
		}
	}
}

func TestPublish_SequenceMonotonic(t *testing.T) {
	t.Parallel()
	b := eventstream.New[int]()
	defer b.Close()
	s := b.Subscribe(10)

	for i := 0; i < 5; i++ {
		b.Publish(i)
	}
	for want := uint64(1); want <= 5; want++ {
		evt := <-s.C()
		if evt.Seq != want {
			t.Fatalf("seq = %d, want %d", evt.Seq, want)
		}
		if evt.Payload != int(want-1) {
			t.Fatalf("payload = %d, want %d", evt.Payload, want-1)
		}
	}
}

func TestPublish_SlowConsumerDrop(t *testing.T) {
	t.Parallel()
	b := eventstream.New[int]()
	defer b.Close()

	fast := b.Subscribe(100)
	slow := b.Subscribe(2) // tiny buffer, never drained

	const n = 20
	for i := 0; i < n; i++ {
		b.Publish(i)
	}

	// Slow consumer buffered only 2 and dropped the rest.
	if got := slow.Dropped(); got != n-2 {
		t.Errorf("slow.Dropped() = %d, want %d", got, n-2)
	}
	if got := fast.Dropped(); got != 0 {
		t.Errorf("fast.Dropped() = %d, want 0", got)
	}

	// Fast consumer got everything, in order.
	for i := 0; i < n; i++ {
		select {
		case evt := <-fast.C():
			if evt.Payload != i {
				t.Fatalf("fast payload = %d, want %d", evt.Payload, i)
			}
		default:
			t.Fatalf("fast missing event %d", i)
		}
	}
}

func TestUnsubscribe(t *testing.T) {
	t.Parallel()
	b := eventstream.New[int]()
	defer b.Close()

	s1 := b.Subscribe(4)
	s2 := b.Subscribe(4)

	s1.Unsubscribe()
	if got := b.Len(); got != 1 {
		t.Fatalf("Len after unsubscribe = %d, want 1", got)
	}

	// s1's channel is closed.
	if _, ok := <-s1.C(); ok {
		t.Error("s1 channel should be closed after Unsubscribe")
	}

	delivered := b.Publish(7)
	if delivered != 1 {
		t.Fatalf("delivered = %d, want 1 (only s2)", delivered)
	}
	if evt := <-s2.C(); evt.Payload != 7 {
		t.Errorf("s2 payload = %d, want 7", evt.Payload)
	}

	// Idempotent.
	s1.Unsubscribe()
	b.Unsubscribe(s1)
	b.Unsubscribe(nil)
}

func TestClose(t *testing.T) {
	t.Parallel()
	b := eventstream.New[int]()
	s := b.Subscribe(4)

	b.Close()

	// Channel closed.
	if _, ok := <-s.C(); ok {
		t.Error("subscriber channel should be closed after Close")
	}
	// Publish after close is a no-op.
	if got := b.Publish(1); got != 0 {
		t.Errorf("Publish after Close delivered %d, want 0", got)
	}
	if got := b.Len(); got != 0 {
		t.Errorf("Len after Close = %d, want 0", got)
	}
	// Subscribe after close returns an already-closed channel.
	s2 := b.Subscribe(4)
	if _, ok := <-s2.C(); ok {
		t.Error("subscribe-after-close channel should be closed")
	}
	// Close is idempotent.
	b.Close()
}

func TestSubscribe_DefaultBuffer(t *testing.T) {
	t.Parallel()
	b := eventstream.New[int]()
	defer b.Close()
	s := b.Subscribe(0) // non-positive -> default buffer
	// Should accept at least a handful without blocking/dropping.
	for i := 0; i < 8; i++ {
		b.Publish(i)
	}
	if got := s.Dropped(); got != 0 {
		t.Errorf("dropped = %d with default buffer, want 0", got)
	}
}

// TestConcurrentPublishSubscribe exercises the broker under -race with many
// goroutines publishing, subscribing, unsubscribing, and draining concurrently.
func TestConcurrentPublishSubscribe(t *testing.T) {
	t.Parallel()
	b := eventstream.New[int]()

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Publishers.
	var published atomic.Uint64
	for p := 0; p < 4; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					b.Publish(1)
					published.Add(1)
				}
			}
		}()
	}

	// Subscribers that drain then unsubscribe.
	for s := 0; s < 8; s++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				select {
				case <-stop:
					return
				default:
				}
				sub := b.Subscribe(8)
				// Drain a few.
				drained := 0
				for drained < 4 {
					select {
					case <-sub.C():
						drained++
					case <-time.After(time.Millisecond):
						drained = 4
					}
				}
				sub.Unsubscribe()
			}
		}()
	}

	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()

	if published.Load() == 0 {
		t.Fatal("expected some publishes")
	}
	b.Close()
}

// TestClose_RaceWithPublish ensures Close concurrent with Publish is safe.
func TestClose_RaceWithPublish(t *testing.T) {
	t.Parallel()
	b := eventstream.New[int]()
	for i := 0; i < 5; i++ {
		b.Subscribe(4)
	}
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				b.Publish(j)
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(time.Millisecond)
		b.Close()
	}()
	wg.Wait()
}
