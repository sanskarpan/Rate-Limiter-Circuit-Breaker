package retry

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
)

func newTestBudget(t *testing.T, cfg BudgetConfig) (*Budget, *clock.ManualClock) {
	t.Helper()
	clk := clock.NewManualClock(time.Unix(0, 0))
	b := NewBudget(cfg, WithBudgetClock(clk))
	return b, clk
}

// TestBudget_DeniesOnceExhausted verifies that a storm of retries drains the
// bucket and further retries are denied, and that no token is consumed on denial.
func TestBudget_DeniesOnceExhausted(t *testing.T) {
	// Burst 3, no time-based refill in this test (we don't advance the clock and
	// keep throughput at the floor). MinPerSecond 0 so nothing refills.
	b, _ := newTestBudget(t, BudgetConfig{Ratio: 0, MinPerSecond: 0, Burst: 3})

	// The bucket starts full with 3 tokens.
	for i := 0; i < 3; i++ {
		if !b.CanRetry() {
			t.Fatalf("CanRetry #%d: got false, want true (bucket should have tokens)", i)
		}
	}
	// Now exhausted: further retries denied, and denial must not go negative.
	for i := 0; i < 5; i++ {
		if b.CanRetry() {
			t.Fatalf("CanRetry after exhaustion #%d: got true, want false", i)
		}
	}
	if got := b.Tokens(); got < 0 {
		t.Fatalf("token count went negative: %v", got)
	}
	if got := b.Tokens(); got >= 1 {
		t.Fatalf("tokens=%v, want < 1 after exhaustion", got)
	}
}

// TestBudget_RecoversOverTime verifies the bucket refills at MinPerSecond even
// with no throughput, so retries become available again.
func TestBudget_RecoversOverTime(t *testing.T) {
	b, clk := newTestBudget(t, BudgetConfig{Ratio: 0, MinPerSecond: 2, Burst: 2})

	// Drain the initial 2 tokens (call both, no short-circuit).
	r1, r2 := b.CanRetry(), b.CanRetry()
	if !r1 || !r2 {
		t.Fatal("expected 2 initial tokens")
	}
	if b.CanRetry() {
		t.Fatal("expected exhaustion after draining burst")
	}

	// After 0.5s at 2 tokens/sec => 1 token refilled.
	clk.Advance(500 * time.Millisecond)
	if !b.CanRetry() {
		t.Fatal("expected 1 token to have refilled after 0.5s")
	}
	if b.CanRetry() {
		t.Fatal("expected only 1 token after 0.5s")
	}

	// After a full second => capped at burst (2).
	clk.Advance(1 * time.Second)
	if got := b.Tokens(); got < 2-1e-9 {
		t.Fatalf("tokens=%v, want ~2 (capped at burst)", got)
	}
}

// TestBudget_MinPerSecondFloor verifies the floor always guarantees a minimum
// refill even at zero throughput / zero ratio.
func TestBudget_MinPerSecondFloor(t *testing.T) {
	b, clk := newTestBudget(t, BudgetConfig{Ratio: 0.1, MinPerSecond: 5, Burst: 1})

	// Drain the single burst token.
	if !b.CanRetry() {
		t.Fatal("expected initial token")
	}
	if b.CanRetry() {
		t.Fatal("expected exhaustion")
	}

	// With no RecordAttempt calls the ratio term is 0, but the floor of 5/sec
	// still refills the bucket. After 0.2s => 1 token (capped at burst 1).
	clk.Advance(200 * time.Millisecond)
	if !b.CanRetry() {
		t.Fatal("MinPerSecond floor should have refilled a token in 0.2s")
	}
}

// TestBudget_RatioScaling verifies that at sustained high throughput the refill
// rate tracks Ratio × requestRate, permitting proportionally more retries than
// the MinPerSecond floor alone.
func TestBudget_RatioScaling(t *testing.T) {
	// Ratio 0.5, tiny floor, large burst so the bucket doesn't clip.
	b, clk := newTestBudget(t, BudgetConfig{Ratio: 0.5, MinPerSecond: 0.01, Burst: 1000})

	// Drive throughput at 100 req/sec for a while (10ms spacing) so the EWMA
	// settles near 100 req/sec.
	for i := 0; i < 500; i++ {
		clk.Advance(10 * time.Millisecond)
		b.RecordAttempt()
	}

	// Drain the bucket completely so we measure pure refill.
	for b.CanRetry() {
	}

	// Refill rate should be ~ Ratio × 100 = 50 tokens/sec. Continue driving
	// throughput to keep the estimate up, and measure tokens accrued over 1s.
	start := b.Tokens()
	for i := 0; i < 100; i++ {
		clk.Advance(10 * time.Millisecond) // 1s total, keeps rate at 100 req/s
		b.RecordAttempt()
	}
	gained := b.Tokens() - start
	// Expect close to 50 tokens over the second; allow generous tolerance for
	// EWMA smoothing.
	if gained < 35 || gained > 65 {
		t.Fatalf("ratio-scaled refill over 1s = %.2f tokens, want ~50 (Ratio×rps)", gained)
	}

	// Sanity: at this throughput we get far more than the MinPerSecond floor
	// (0.01/sec) would allow.
	if gained <= 1 {
		t.Fatalf("ratio scaling gave only %.2f tokens, no better than the floor", gained)
	}
}

// TestBudget_Deposit verifies Deposit returns tokens, capped at burst.
func TestBudget_Deposit(t *testing.T) {
	b, _ := newTestBudget(t, BudgetConfig{Ratio: 0, MinPerSecond: 0, Burst: 4})

	// Drain.
	for b.CanRetry() {
	}
	if b.CanRetry() {
		t.Fatal("expected exhaustion")
	}

	b.Deposit(2)
	d1, d2 := b.CanRetry(), b.CanRetry()
	if !d1 || !d2 {
		t.Fatal("Deposit(2) should have returned 2 tokens")
	}
	if b.CanRetry() {
		t.Fatal("only 2 tokens should have been deposited")
	}

	// Deposit beyond burst is capped.
	b.Deposit(1000)
	if got := b.Tokens(); got > 4+1e-9 {
		t.Fatalf("Deposit exceeded burst cap: tokens=%v, want <= 4", got)
	}

	// Non-positive deposits are no-ops.
	before := b.Tokens()
	b.Deposit(0)
	b.Deposit(-5)
	if after := b.Tokens(); after != before {
		t.Fatalf("non-positive Deposit changed tokens: %v -> %v", before, after)
	}
}

// TestBudget_DefaultBurst verifies the burst defaults sensibly when unset.
func TestBudget_DefaultBurst(t *testing.T) {
	b := NewBudget(BudgetConfig{Ratio: 0.1, MinPerSecond: 3}) // Burst unset
	if got := b.Tokens(); got < 3-1e-9 || got > 3+1e-9 {
		t.Fatalf("default burst tokens=%v, want 3 (=MinPerSecond)", got)
	}

	b2 := NewBudget(BudgetConfig{Ratio: 0.1, MinPerSecond: 0}) // both unset
	if got := b2.Tokens(); got < 1-1e-9 || got > 1+1e-9 {
		t.Fatalf("default burst tokens=%v, want 1 (floor)", got)
	}
}

// TestBudget_Concurrency exercises CanRetry/RecordAttempt/Deposit from many
// goroutines under -race, asserting the token count never goes negative.
func TestBudget_Concurrency(t *testing.T) {
	clk := clock.NewManualClock(time.Unix(0, 0))
	b := NewBudget(BudgetConfig{Ratio: 0.5, MinPerSecond: 10, Burst: 50}, WithBudgetClock(clk))

	const goroutines = 32
	const iters = 1000

	var granted int64
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				b.RecordAttempt()
				if b.CanRetry() {
					atomic.AddInt64(&granted, 1)
				}
				if i%100 == 0 {
					b.Deposit(1)
				}
			}
		}()
	}

	// Concurrently advance the clock to drive time-based refill.
	stop := make(chan struct{})
	var ticker sync.WaitGroup
	ticker.Add(1)
	go func() {
		defer ticker.Done()
		for {
			select {
			case <-stop:
				return
			default:
				clk.Advance(time.Millisecond)
			}
		}
	}()

	wg.Wait()
	close(stop)
	ticker.Wait()

	if got := b.Tokens(); got < 0 {
		t.Fatalf("token count went negative under concurrency: %v", got)
	}
	if granted < 0 {
		t.Fatalf("granted count negative: %d", granted)
	}
	t.Logf("granted %d retries across %d goroutines", granted, goroutines)
}
