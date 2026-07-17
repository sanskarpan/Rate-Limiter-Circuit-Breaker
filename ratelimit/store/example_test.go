package store_test

import (
	"context"
	"fmt"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/store"
)

// ExampleNewMemory shows the in-memory Store used by non-distributed limiters
// and as a Redis fallback.
func ExampleNewMemory() {
	s := store.NewMemory()
	defer s.Close()

	ctx := context.Background()
	_ = s.Set(ctx, "greeting", "hello", time.Minute)
	v, _ := s.Get(ctx, "greeting")
	fmt.Println(v)
	// Output: hello
}
