package backoff_test

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/retry/backoff"
)

// ---------------------------------------------------------------------------
// Constant backoff
// ---------------------------------------------------------------------------

func TestConstantBackoff_AlwaysReturnsFixedDuration(t *testing.T) {
	d := 250 * time.Millisecond
	b := backoff.Constant(d)
	for attempt := 0; attempt < 10; attempt++ {
		got := b.Next(attempt)
		if got != d {
			t.Errorf("attempt %d: expected %v, got %v", attempt, d, got)
		}
	}
}

// ---------------------------------------------------------------------------
// Exponential backoff
// ---------------------------------------------------------------------------

func TestExponentialBackoff_Formula(t *testing.T) {
	base := 100 * time.Millisecond
	max := 10 * time.Second
	b := backoff.Exponential(base, max)

	expected := []time.Duration{
		100 * time.Millisecond, // 2^0 * 100ms
		200 * time.Millisecond, // 2^1 * 100ms
		400 * time.Millisecond, // 2^2 * 100ms
		800 * time.Millisecond, // 2^3 * 100ms
		1600 * time.Millisecond, // 2^4 * 100ms
		3200 * time.Millisecond, // 2^5 * 100ms
		6400 * time.Millisecond, // 2^6 * 100ms
		10 * time.Second,        // capped
		10 * time.Second,        // capped
	}

	for i, want := range expected {
		got := b.Next(i)
		if got != want {
			t.Errorf("attempt %d: expected %v, got %v", i, want, got)
		}
	}
}

func TestExponentialBackoff_NeverExceedsMax(t *testing.T) {
	max := 5 * time.Second
	b := backoff.Exponential(100*time.Millisecond, max)
	for attempt := 0; attempt < 100; attempt++ {
		got := b.Next(attempt)
		if got > max {
			t.Errorf("attempt %d: got %v > max %v", attempt, got, max)
		}
	}
}

func TestExponentialBackoff_LargeAttemptDoesNotOverflow(t *testing.T) {
	max := time.Hour
	b := backoff.Exponential(time.Second, max)
	// Attempt 100 would overflow int64 — should just return max.
	got := b.Next(100)
	if got != max {
		t.Errorf("expected max %v at large attempt, got %v", max, got)
	}
}

// ---------------------------------------------------------------------------
// FullJitter
// ---------------------------------------------------------------------------

func TestFullJitter_AlwaysWithinBounds(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	base := 100 * time.Millisecond
	cap := 2 * time.Second
	b := backoff.FullJitter(base, cap, rng)

	for attempt := 0; attempt < 1000; attempt++ {
		got := b.Next(attempt % 10)
		if got < 0 || got >= cap {
			t.Errorf("attempt %d: got %v outside [0, %v)", attempt, got, cap)
		}
	}
}

func TestFullJitter_Distribution_MeanApproxHalfCap(t *testing.T) {
	// At a high enough attempt, exponentialCap saturates to cap.
	// The distribution then is uniform on [0, cap), mean ≈ cap/2.
	rng := rand.New(rand.NewSource(12345))
	base := 1 * time.Millisecond
	capDur := 1000 * time.Millisecond
	b := backoff.FullJitter(base, capDur, rng)

	const samples = 100_000
	var total int64
	for i := 0; i < samples; i++ {
		total += int64(b.Next(30)) // attempt 30 saturates cap
	}
	mean := time.Duration(total / samples)
	expected := capDur / 2
	// Allow ±5% deviation
	tolerance := capDur / 20
	if mean < expected-tolerance || mean > expected+tolerance {
		t.Errorf("FullJitter mean %v is not close to expected %v (±%v)", mean, expected, tolerance)
	}
}

// ---------------------------------------------------------------------------
// EqualJitter
// ---------------------------------------------------------------------------

func TestEqualJitter_AlwaysWithinBounds(t *testing.T) {
	rng := rand.New(rand.NewSource(99))
	base := 100 * time.Millisecond
	capDur := 2 * time.Second
	b := backoff.EqualJitter(base, capDur, rng)

	for attempt := 0; attempt < 1000; attempt++ {
		got := b.Next(attempt % 10)
		if got < 0 || got > capDur {
			t.Errorf("attempt %d: got %v outside [0, %v]", attempt, got, capDur)
		}
	}
}

func TestEqualJitter_MinimumIsHalfCap(t *testing.T) {
	// At saturated cap, min value is cap/2 (when rng always returns 0).
	// We cannot mock rng to return 0, but we can check that the min observed
	// over many samples is >= cap/2 - 1 (rounding).
	rng := rand.New(rand.NewSource(7))
	base := 1 * time.Millisecond
	capDur := 200 * time.Millisecond
	b := backoff.EqualJitter(base, capDur, rng)

	minSeen := capDur
	const samples = 10_000
	for i := 0; i < samples; i++ {
		got := b.Next(30) // saturated
		if got < minSeen {
			minSeen = got
		}
	}
	// Minimum should be >= cap/2 - 1ns (only 0 from rng.Int63n(cap/2) can give cap/2)
	halfCap := capDur / 2
	if minSeen < halfCap {
		t.Errorf("EqualJitter minimum %v is less than cap/2 %v", minSeen, halfCap)
	}
}

// ---------------------------------------------------------------------------
// DecorrelatedJitter
// ---------------------------------------------------------------------------

func TestDecorrelatedJitter_NeverExceedsCap(t *testing.T) {
	rng := rand.New(rand.NewSource(2024))
	base := 100 * time.Millisecond
	capDur := 3 * time.Second
	b := backoff.Decorrelated(base, capDur, rng)

	for attempt := 0; attempt < 1000; attempt++ {
		got := b.Next(attempt)
		if got > capDur {
			t.Errorf("attempt %d: got %v exceeds cap %v", attempt, got, capDur)
		}
		if got < base {
			t.Errorf("attempt %d: got %v is below base %v", attempt, got, base)
		}
	}
}

func TestDecorrelatedJitter_NoBoundExplosion(t *testing.T) {
	// Run thousands of attempts and confirm the cap is never exceeded.
	rng := rand.New(rand.NewSource(555))
	base := 50 * time.Millisecond
	capDur := 1 * time.Second
	b := backoff.Decorrelated(base, capDur, rng)

	for i := 0; i < 10_000; i++ {
		got := b.Next(i)
		if got > capDur {
			t.Fatalf("attempt %d: %v exceeds cap %v", i, got, capDur)
		}
	}
}

func TestDecorrelatedJitter_StateIsIndependentOfAttemptArg(t *testing.T) {
	// The decorrelated strategy is stateful: it ignores the attempt arg and
	// uses its internal prev state. Verify two separate strategies with the
	// same seed produce identical results when called with non-monotonic args.
	rng1 := rand.New(rand.NewSource(42))
	rng2 := rand.New(rand.NewSource(42))
	base := 100 * time.Millisecond
	capDur := 2 * time.Second
	b1 := backoff.Decorrelated(base, capDur, rng1)
	b2 := backoff.Decorrelated(base, capDur, rng2)

	args := []int{5, 0, 99, 3, 1, 7}
	for i, arg := range args {
		v1 := b1.Next(arg)
		v2 := b2.Next(arg)
		if v1 != v2 {
			t.Errorf("call %d with arg %d: b1=%v b2=%v differ", i, arg, v1, v2)
		}
	}
}

// ---------------------------------------------------------------------------
// Regression: H-13 / H-14 — concurrent Next() must be race-free
// ---------------------------------------------------------------------------

// concurrentNext hammers a single backoff instance from many goroutines. Under
// the -race detector this fails if the shared *rand.Rand (or, for decorrelated,
// the prev field) is accessed without synchronization.
func concurrentNext(t *testing.T, b backoff.BackoffStrategy) {
	t.Helper()
	const goroutines = 8
	const iters = 1000
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				_ = b.Next(i % 10)
			}
		}()
	}
	wg.Wait()
}

func TestFullJitter_ConcurrentNextRaceFree(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	concurrentNext(t, backoff.FullJitter(10*time.Millisecond, time.Second, rng))
}

func TestEqualJitter_ConcurrentNextRaceFree(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	concurrentNext(t, backoff.EqualJitter(10*time.Millisecond, time.Second, rng))
}

func TestDecorrelatedJitter_ConcurrentNextRaceFree(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	concurrentNext(t, backoff.Decorrelated(10*time.Millisecond, time.Second, rng))
}

// ---------------------------------------------------------------------------
// Regression: M-12 — base<=0 must return 0, distinct from real overflow -> max
// ---------------------------------------------------------------------------

func TestExponentialBackoff_ZeroBaseReturnsZero(t *testing.T) {
	b := backoff.Exponential(0, time.Hour)
	for attempt := 0; attempt < 5; attempt++ {
		if got := b.Next(attempt); got != 0 {
			t.Errorf("attempt %d: base 0 expected 0, got %v", attempt, got)
		}
	}
	// A genuinely overflowing attempt with a positive base still clamps to max.
	over := backoff.Exponential(time.Second, time.Hour)
	if got := over.Next(100); got != time.Hour {
		t.Errorf("overflow attempt: expected max %v, got %v", time.Hour, got)
	}
}

func TestJitter_ZeroBaseReturnsZero(t *testing.T) {
	rng := rand.New(rand.NewSource(4))
	full := backoff.FullJitter(0, time.Hour, rng)
	equal := backoff.EqualJitter(0, time.Hour, rng)
	for attempt := 0; attempt < 5; attempt++ {
		if got := full.Next(attempt); got != 0 {
			t.Errorf("FullJitter base 0 attempt %d: expected 0, got %v", attempt, got)
		}
		if got := equal.Next(attempt); got != 0 {
			t.Errorf("EqualJitter base 0 attempt %d: expected 0, got %v", attempt, got)
		}
	}
}
