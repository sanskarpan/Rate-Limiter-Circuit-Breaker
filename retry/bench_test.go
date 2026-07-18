package retry_test

import (
	"context"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/retry"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/retry/backoff"
)

// succeed always succeeds on the first attempt, so Do never sleeps and the
// benchmark measures the retry-loop overhead on the happy path.
func succeed(_ context.Context) error { return nil }

// BenchmarkRetry_Do_Success measures Do on the success path with no backoff
// configured: fn is called exactly once.
func BenchmarkRetry_Do_Success(b *testing.B) {
	p := &retry.Policy{MaxAttempts: 3}
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = p.Do(ctx, succeed)
	}
}

// BenchmarkRetry_Do_Backoff_NoRetry measures Do with a backoff strategy
// configured but a first-attempt success, so the backoff is never consulted and
// no sleep happens — isolating the per-call setup cost of a backoff-configured
// policy. A ManualClock keeps it deterministic (no wall-clock sleeps possible).
func BenchmarkRetry_Do_Backoff_NoRetry(b *testing.B) {
	p := &retry.Policy{
		MaxAttempts: 5,
		Backoff:     backoff.Exponential(100*time.Millisecond, 5*time.Second),
		Clock:       clock.NewManualClock(time.Unix(0, 0)),
	}
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = p.Do(ctx, succeed)
	}
}
