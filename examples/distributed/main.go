// Package main demonstrates distributed rate limiting backed by Redis.
// Run with: REDIS_ADDR=localhost:6379 go run ./examples/distributed/
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/store"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
)

func main() {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	// 1. Create Redis store with key prefix for this demo.
	s := store.NewRedis(store.RedisOptions{
		Addr:      redisAddr,
		KeyPrefix: "demo:dtb:",
	})
	defer s.Close()

	// 2. Test connectivity.
	ctx := context.Background()
	if err := s.Ping(ctx); err != nil {
		log.Fatalf("Cannot connect to Redis at %s: %v", redisAddr, err)
	}
	fmt.Printf("Connected to Redis at %s\n", redisAddr)

	// 3. Create 3 distributed token bucket limiters sharing the same Redis backend.
	//    All 3 share the same global limit of 20 requests.
	const limit = 20
	d1 := tokenbucket.NewDistributed(float64(limit), float64(limit), s, "demo")
	d2 := tokenbucket.NewDistributed(float64(limit), float64(limit), s, "demo")
	d3 := tokenbucket.NewDistributed(float64(limit), float64(limit), s, "demo")

	// 4. Send 30 concurrent requests across 3 limiters.
	//    Globally, only 20 should be allowed.
	var allowed, denied int32
	var wg sync.WaitGroup

	fmt.Printf("Sending 30 concurrent requests with global limit=%d...\n", limit)

	for i := 0; i < 10; i++ {
		for _, d := range []*tokenbucket.DistributedTokenBucket{d1, d2, d3} {
			d := d
			wg.Add(1)
			go func() {
				defer wg.Done()
				result := d.Allow(ctx, "global-key")
				if result.Allowed {
					atomic.AddInt32(&allowed, 1)
				} else {
					atomic.AddInt32(&denied, 1)
				}
			}()
		}
	}
	wg.Wait()

	fmt.Printf("Results: allowed=%d, denied=%d (limit=%d)\n", allowed, denied, limit)
	if int(allowed) > limit {
		fmt.Printf("ERROR: %d requests allowed, but limit is %d!\n", allowed, limit)
	} else {
		fmt.Println("PASS: global rate limit enforced across 3 distributed instances")
	}

	// 5. Demonstrate reset.
	fmt.Println("\nResetting rate limit state...")
	if err := d1.Reset(ctx, "global-key"); err != nil {
		log.Printf("Reset error: %v", err)
	} else {
		result := d1.Allow(ctx, "global-key")
		fmt.Printf("After reset: allowed=%v (expected true)\n", result.Allowed)
	}
}
