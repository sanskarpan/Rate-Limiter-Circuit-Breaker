package debounce_test

import (
	"fmt"
	"sync"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/debounce"
)

// Example demonstrates trailing-edge debouncing: a burst of rapid calls
// collapses into a single invocation of the most recent function.
func Example() {
	var wg sync.WaitGroup
	wg.Add(1)

	d := debounce.New(20 * time.Millisecond)
	defer d.Stop()

	// Simulate a burst of five rapid events. Only the last runs.
	for i := 1; i <= 5; i++ {
		i := i
		d.Trigger(func() {
			fmt.Printf("saved revision %d\n", i)
			wg.Done()
		})
	}

	wg.Wait()
	// Output: saved revision 5
}

// ExampleThrottler shows leading-edge throttling: the first call runs
// immediately while calls within the interval are coalesced.
func ExampleThrottler() {
	var wg sync.WaitGroup
	wg.Add(1)

	th := debounce.NewThrottler(20*time.Millisecond, debounce.WithoutTrailing())
	defer th.Stop()

	th.Trigger(func() {
		fmt.Println("refresh")
		wg.Done()
	})
	// This call lands inside the interval and is dropped (trailing disabled).
	th.Trigger(func() { fmt.Println("should not run") })

	wg.Wait()
	// Output: refresh
}
