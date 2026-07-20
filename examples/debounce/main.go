// Package main demonstrates the debounce package's two coalescing primitives:
//
//   - Debouncer collapses a burst of rapid calls into a single trailing-edge
//     invocation of the most recent function, firing once the calls go quiet for
//     the configured delay. Classic use: "save on every keystroke, but only once
//     the user stops typing."
//
//   - Throttler runs at most once per interval. With leading edge enabled (the
//     default) the first call runs immediately and further calls within the
//     interval are coalesced into a single trailing invocation. Classic use:
//     "refresh on scroll, but no more than once every N ms."
//
// Both are safe for concurrent use and accept an injectable clock for
// deterministic tests. This demo uses the real clock and a WaitGroup to block
// until the deferred work has run.
package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/debounce"
)

func main() {
	debounceDemo()
	throttleDemo()
}

func debounceDemo() {
	fmt.Println("== Debouncer (trailing edge) ==")

	var wg sync.WaitGroup
	wg.Add(1)

	d := debounce.New(20 * time.Millisecond)
	defer d.Stop()

	// A burst of five rapid "keystrokes". Only the last one survives — the
	// debouncer keeps rescheduling until the burst goes quiet.
	for i := 1; i <= 5; i++ {
		rev := i
		d.Trigger(func() {
			fmt.Printf("  saved revision %d (only the last edit persists)\n", rev)
			wg.Done()
		})
	}

	wg.Wait()
}

func throttleDemo() {
	fmt.Println("== Throttler (leading edge + trailing) ==")

	var mu sync.Mutex
	var runs []int
	var wg sync.WaitGroup
	wg.Add(2) // expect a leading run and one coalesced trailing run

	th := debounce.NewThrottler(20 * time.Millisecond)
	defer th.Stop()

	record := func(id int) func() {
		return func() {
			mu.Lock()
			runs = append(runs, id)
			mu.Unlock()
			fmt.Printf("  refresh fired for event %d\n", id)
			wg.Done()
		}
	}

	// First call runs immediately (leading edge). The next two land inside the
	// interval and are coalesced into a single trailing invocation of the last.
	th.Trigger(record(1))
	th.Trigger(record(2))
	th.Trigger(record(3))

	wg.Wait()

	mu.Lock()
	fmt.Printf("  events that actually fired: %v (leading + one coalesced trailing)\n", runs)
	mu.Unlock()
}
