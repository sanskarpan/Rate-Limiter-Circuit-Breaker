package composite_test

import (
	"context"
	"fmt"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/composite"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/fixedwindow"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
)

func Example_andMode() {
	// AND mode: request must pass BOTH a per-second burst limit
	// AND a per-minute sustained limit.
	burstLimiter := tokenbucket.New(10, 10) // 10 req/s burst
	minuteLimiter := fixedwindow.New(100, time.Minute)

	comp := composite.New(composite.AND, []ratelimit.Limiter{burstLimiter, minuteLimiter})
	defer comp.Close()

	ctx := context.Background()
	result := comp.Allow(ctx, "user:123")
	if result.Allowed {
		fmt.Println("Request allowed by both limiters")
	}
	// Output: Request allowed by both limiters
}

func Example_orMode() {
	// OR mode: request passes if EITHER limiter allows it.
	// Useful for fallback: allow if premium tier OR basic tier quota available.
	premiumLimiter := tokenbucket.New(1000, 100) // premium: 1000 burst, 100/s
	basicLimiter := tokenbucket.New(10, 1)       // basic: 10 burst, 1/s

	comp := composite.New(composite.OR, []ratelimit.Limiter{premiumLimiter, basicLimiter})
	defer comp.Close()

	ctx := context.Background()
	result := comp.Allow(ctx, "user:456")
	if result.Allowed {
		fmt.Println("Request allowed")
	}
	// Output: Request allowed
}
