package testutil

import (
	"context"
	"sync"
	"testing"
)

// ChaosConfig configures a chaos test run.
type ChaosConfig struct {
	// Goroutines is the number of concurrent goroutines to use.
	Goroutines int
	// Operations is the total number of operations per goroutine.
	Operations int
}

// DefaultChaosConfig is the standard chaos configuration.
var DefaultChaosConfig = ChaosConfig{
	Goroutines: 500,
	Operations: 200,
}

// RunChaos runs fn concurrently from Goroutines goroutines, each calling fn
// Operations times. Any panic is caught and reported as a test failure.
// Returns the total number of successful operations.
func RunChaos(t testing.TB, cfg ChaosConfig, fn func(ctx context.Context)) {
	t.Helper()
	var wg sync.WaitGroup
	ctx := context.Background()
	for g := 0; g < cfg.Goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("chaos test panic: %v", r)
				}
			}()
			for op := 0; op < cfg.Operations; op++ {
				fn(ctx)
			}
		}()
	}
	wg.Wait()
}
