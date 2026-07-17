package bulkhead_test

import (
	"context"
	"fmt"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/bulkhead"
)

// ExampleNew demonstrates creating a Bulkhead and executing requests through it.
func ExampleNew() {
	// Allow at most 2 concurrent requests; reject immediately if full.
	b := bulkhead.New(2, 0)

	err := b.Execute(context.Background(), func(ctx context.Context) error {
		fmt.Println("request executed")
		return nil
	})
	if err != nil {
		fmt.Println("rejected:", err)
	}
	// Output:
	// request executed
}

// ExampleBulkhead_Execute_rejected demonstrates rejection when the bulkhead
// is at capacity and maxWait is zero.
func ExampleBulkhead_Execute_rejected() {
	b := bulkhead.New(1, 0)

	// Occupy the single slot with a long-running task.
	hold := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = b.Execute(context.Background(), func(ctx context.Context) error {
			<-hold
			return nil
		})
	}()

	// Wait until the slot is held.
	for b.Inflight() == 0 {
		time.Sleep(time.Millisecond)
	}

	// Second request is rejected.
	err := b.Execute(context.Background(), func(ctx context.Context) error { return nil })
	fmt.Println(err)

	close(hold)
	<-done
	// Output:
	// bulkhead: concurrent request limit exceeded
}

// ExampleNewThreadPool demonstrates submitting a task to a ThreadPool and
// collecting the asynchronous result.
func ExampleNewThreadPool() {
	tp := bulkhead.NewThreadPool(2, 10)
	defer tp.Close()

	ch, err := tp.Submit(context.Background(), func(ctx context.Context) error {
		fmt.Println("async task executed")
		return nil
	})
	if err != nil {
		fmt.Println("submit error:", err)
		return
	}

	// Block until the result arrives.
	if taskErr := <-ch; taskErr != nil {
		fmt.Println("task error:", taskErr)
	}
	// Output:
	// async task executed
}
