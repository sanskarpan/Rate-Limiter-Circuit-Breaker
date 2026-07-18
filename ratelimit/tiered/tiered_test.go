package tiered

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/clock"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
)

// fakeBucket is a deterministic, in-memory token bucket used to assert exact
// accounting. It has no time dependence: tokens only change via AllowN/Credit/
// Reset, so a test can prove that a denied tiered call left counts untouched.
// It optionally implements Crediter (see credits field).
type fakeBucket struct {
	mu       sync.Mutex
	capacity int
	tokens   map[string]int
	credits  bool // whether Credit is honored (i.e. implements rollback)
	closed   bool
}

func newFakeBucket(capacity int, credits bool) *fakeBucket {
	return &fakeBucket{capacity: capacity, tokens: map[string]int{}, credits: credits}
}

func (f *fakeBucket) get(key string) int {
	if v, ok := f.tokens[key]; ok {
		return v
	}
	return f.capacity
}

func (f *fakeBucket) Allow(ctx context.Context, key string) ratelimit.Result {
	return f.AllowN(ctx, key, 1)
}

func (f *fakeBucket) AllowN(_ context.Context, key string, n int) ratelimit.Result {
	f.mu.Lock()
	defer f.mu.Unlock()
	cur := f.get(key)
	if cur < n {
		return ratelimit.Result{
			Allowed:    false,
			Limit:      f.capacity,
			Remaining:  cur,
			RetryAfter: 50 * time.Millisecond,
			Algorithm:  "fake",
		}
	}
	cur -= n
	f.tokens[key] = cur
	return ratelimit.Result{Allowed: true, Limit: f.capacity, Remaining: cur, Algorithm: "fake"}
}

func (f *fakeBucket) Wait(ctx context.Context, key string) error { return f.WaitN(ctx, key, 1) }

func (f *fakeBucket) WaitN(ctx context.Context, key string, n int) error {
	if f.AllowN(ctx, key, n).Allowed {
		return nil
	}
	return errors.New("would block")
}

func (f *fakeBucket) Peek(_ context.Context, key string) ratelimit.State {
	f.mu.Lock()
	defer f.mu.Unlock()
	return ratelimit.State{Key: key, Algorithm: "fake", Limit: f.capacity, Remaining: f.get(key)}
}

func (f *fakeBucket) Reset(_ context.Context, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.tokens, key)
	return nil
}

func (f *fakeBucket) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

// Credit implements Crediter only when credits is true. A fake without credit
// support still has the method but reports an error and does not restore tokens,
// letting us exercise the best-effort path — but see fakeNoCredit for a type
// that genuinely lacks the method.
func (f *fakeBucket) Credit(_ context.Context, key string, n int) error {
	if !f.credits {
		return errors.New("credit not supported")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	cur := f.get(key)
	cur += n
	if cur > f.capacity {
		cur = f.capacity
	}
	f.tokens[key] = cur
	return nil
}

// remaining is a test helper reading a fake's live token count for a key.
func (f *fakeBucket) remaining(key string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.get(key)
}

// ---- tests ----

func ctxT() context.Context { return context.Background() }

func TestAllow_AllTiersAllow(t *testing.T) {
	user := newFakeBucket(2, true)
	tenant := newFakeBucket(10, true)
	global := newFakeBucket(100, true)
	lim := New(
		Tier{Name: "user", KeyFunc: identity, Limiter: user},
		Tier{Name: "tenant", KeyFunc: Prefix(":"), Limiter: tenant},
		Tier{Name: "global", KeyFunc: Constant("all"), Limiter: global},
	)
	res := lim.Allow(ctxT(), "acme:alice")
	if !res.Allowed {
		t.Fatalf("expected allow, got deny: %+v", res)
	}
	if user.remaining("acme:alice") != 1 {
		t.Errorf("user tier: want 1 remaining, got %d", user.remaining("acme:alice"))
	}
	if tenant.remaining("acme") != 9 {
		t.Errorf("tenant tier: want 9 remaining, got %d", tenant.remaining("acme"))
	}
	if global.remaining("all") != 99 {
		t.Errorf("global tier: want 99 remaining, got %d", global.remaining("all"))
	}
	if res.Algorithm != algorithmName {
		t.Errorf("algorithm: want %q, got %q", algorithmName, res.Algorithm)
	}
}

func TestPerTierDenyAndMetadata(t *testing.T) {
	tests := []struct {
		name       string
		userCap    int
		tenantCap  int
		globalCap  int
		wantTier   string
		wantKey    string
		wantDenied bool
	}{
		{"user tier denies", 0, 10, 100, "user", "acme:alice", true},
		{"tenant tier denies", 5, 0, 100, "tenant", "acme", true},
		{"global tier denies", 5, 10, 0, "global", "all", true},
		{"all allow", 5, 10, 100, "", "", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			user := newFakeBucket(tc.userCap, true)
			tenant := newFakeBucket(tc.tenantCap, true)
			global := newFakeBucket(tc.globalCap, true)
			lim := New(
				Tier{Name: "user", KeyFunc: identity, Limiter: user},
				Tier{Name: "tenant", KeyFunc: Prefix(":"), Limiter: tenant},
				Tier{Name: "global", KeyFunc: Constant("all"), Limiter: global},
			)
			res := lim.Allow(ctxT(), "acme:alice")
			if res.Allowed == tc.wantDenied {
				t.Fatalf("allowed=%v, wantDenied=%v", res.Allowed, tc.wantDenied)
			}
			if !tc.wantDenied {
				return
			}
			if got := res.Metadata["denied_tier"]; got != tc.wantTier {
				t.Errorf("denied_tier: want %q, got %v", tc.wantTier, got)
			}
			if got := res.Metadata["denied_key"]; got != tc.wantKey {
				t.Errorf("denied_key: want %q, got %v", tc.wantKey, got)
			}
		})
	}
}

// TestRollback_LowerTierDeny verifies the core property: when a lower-priority
// (later) tier denies, the higher tiers are NOT left debited.
func TestRollback_LowerTierDeny(t *testing.T) {
	// user has plenty; global is exhausted. A phase-1 deny must never touch user.
	user := newFakeBucket(5, true)
	global := newFakeBucket(0, true) // no capacity => tier denies
	lim := New(
		Tier{Name: "user", KeyFunc: identity, Limiter: user},
		Tier{Name: "global", KeyFunc: Constant("all"), Limiter: global},
	)
	before := user.remaining("alice")
	res := lim.Allow(ctxT(), "alice")
	if res.Allowed {
		t.Fatal("expected deny (global exhausted)")
	}
	if got := user.remaining("alice"); got != before {
		t.Fatalf("higher (user) tier was debited on deny: before=%d after=%d", before, got)
	}
	if res.Metadata["denied_tier"] != "global" {
		t.Errorf("want global denial, got %v", res.Metadata["denied_tier"])
	}
}

// TestRollback_Phase2Anomaly forces a tier to deny during the COMMIT phase
// (after Peek reported capacity) by draining it between Peek and AllowN through
// an onDecision-free side channel: we pre-arm the global tier to allow the Peek
// but deny the AllowN by consuming it directly. We simulate this with a limiter
// whose Peek always reports capacity but whose AllowN denies, and assert the
// already-committed user tier is rolled back via Credit.
func TestRollback_Phase2Anomaly(t *testing.T) {
	user := newFakeBucket(5, true) // supports Credit => rollback restores it
	trap := &peekLiesLimiter{}     // Peek says OK, AllowN denies
	lim := New(
		Tier{Name: "user", KeyFunc: identity, Limiter: user},
		Tier{Name: "trap", KeyFunc: Constant("g"), Limiter: trap},
	)
	before := user.remaining("bob")
	res := lim.AllowN(ctxT(), "bob", 2)
	if res.Allowed {
		t.Fatal("expected deny from trap tier during commit")
	}
	if got := user.remaining("bob"); got != before {
		t.Fatalf("user tier not rolled back: before=%d after=%d", before, got)
	}
	if res.Metadata["denied_tier"] != "trap" {
		t.Errorf("want trap denial, got %v", res.Metadata["denied_tier"])
	}
}

// peekLiesLimiter reports capacity on Peek but always denies AllowN, to exercise
// the phase-2 rollback path.
type peekLiesLimiter struct{}

func (peekLiesLimiter) Allow(ctx context.Context, key string) ratelimit.Result {
	return peekLiesLimiter{}.AllowN(ctx, key, 1)
}
func (peekLiesLimiter) AllowN(context.Context, string, int) ratelimit.Result {
	return ratelimit.Result{Allowed: false, Limit: 10, Remaining: 0, RetryAfter: time.Second, Algorithm: "trap"}
}
func (peekLiesLimiter) Wait(context.Context, string) error       { return nil }
func (peekLiesLimiter) WaitN(context.Context, string, int) error { return nil }
func (peekLiesLimiter) Peek(_ context.Context, key string) ratelimit.State {
	return ratelimit.State{Key: key, Limit: 10, Remaining: 10, Algorithm: "trap"}
}
func (peekLiesLimiter) Reset(context.Context, string) error { return nil }
func (peekLiesLimiter) Close() error                        { return nil }

// TestOrdering verifies the FIRST failing tier in chain order is the one
// reported, even if multiple tiers would deny.
func TestOrdering(t *testing.T) {
	user := newFakeBucket(0, true)   // denies
	tenant := newFakeBucket(0, true) // also denies
	lim := New(
		Tier{Name: "user", KeyFunc: identity, Limiter: user},
		Tier{Name: "tenant", KeyFunc: Constant("t"), Limiter: tenant},
	)
	res := lim.Allow(ctxT(), "x")
	if res.Allowed {
		t.Fatal("expected deny")
	}
	if res.Metadata["denied_tier"] != "user" {
		t.Errorf("first tier should be reported first; got %v", res.Metadata["denied_tier"])
	}
}

func TestPeek(t *testing.T) {
	user := newFakeBucket(3, true)
	global := newFakeBucket(1, true) // most restrictive
	lim := New(
		Tier{Name: "user", KeyFunc: identity, Limiter: user},
		Tier{Name: "global", KeyFunc: Constant("all"), Limiter: global},
	)
	st := lim.Peek(ctxT(), "alice")
	if st.Remaining != 1 {
		t.Errorf("peek should return most restrictive remaining (1), got %d", st.Remaining)
	}
	if st.Key != "alice" {
		t.Errorf("peek key: want alice, got %q", st.Key)
	}
	// Peek must not consume.
	if user.remaining("alice") != 3 || global.remaining("all") != 1 {
		t.Errorf("peek consumed tokens: user=%d global=%d", user.remaining("alice"), global.remaining("all"))
	}
}

func TestReset(t *testing.T) {
	user := newFakeBucket(2, true)
	global := newFakeBucket(2, true)
	lim := New(
		Tier{Name: "user", KeyFunc: identity, Limiter: user},
		Tier{Name: "global", KeyFunc: Constant("all"), Limiter: global},
	)
	lim.Allow(ctxT(), "alice")
	lim.Allow(ctxT(), "alice")
	if user.remaining("alice") != 0 {
		t.Fatalf("setup: want user exhausted, got %d", user.remaining("alice"))
	}
	if err := lim.Reset(ctxT(), "alice"); err != nil {
		t.Fatalf("reset error: %v", err)
	}
	if user.remaining("alice") != 2 {
		t.Errorf("user not reset: got %d", user.remaining("alice"))
	}
	if global.remaining("all") != 2 {
		t.Errorf("global not reset: got %d", global.remaining("all"))
	}
	res := lim.Allow(ctxT(), "alice")
	if !res.Allowed {
		t.Error("expected allow after reset")
	}
}

func TestClose(t *testing.T) {
	user := newFakeBucket(2, true)
	global := newFakeBucket(2, true)
	lim := New(
		Tier{KeyFunc: identity, Limiter: user},
		Tier{KeyFunc: Constant("all"), Limiter: global},
	)
	if err := lim.Close(); err != nil {
		t.Fatalf("close error: %v", err)
	}
	if !user.closed || !global.closed {
		t.Errorf("underlying limiters not closed: user=%v global=%v", user.closed, global.closed)
	}
}

// TestWaitN_DeterministicClock exercises Wait against a real tokenbucket under a
// ManualClock: the first token is available, the second must wait for a refill.
func TestWaitN_DeterministicClock(t *testing.T) {
	mc := clock.NewManualClock(time.Unix(0, 0))
	// capacity 1, refill 1 token/sec.
	global := tokenbucket.New(1, 1, tokenbucket.WithClock(mc))
	defer global.Close()
	lim := New(
		Tier{Name: "global", KeyFunc: Constant("all"), Limiter: global},
		WithClock(mc),
	)
	// First allow consumes the only token.
	if !lim.Allow(ctxT(), "alice").Allowed {
		t.Fatal("first allow should succeed")
	}
	// Second must block; run Wait in a goroutine and release by advancing.
	done := make(chan error, 1)
	go func() { done <- lim.Wait(ctxT(), "alice") }()
	// Give the goroutine a moment to enter the wait loop, then advance the clock
	// enough to refill a token and fire the retry timer.
	waitUntilBlocked()
	mc.Advance(2 * time.Second)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("wait returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not return after clock advance")
	}
}

// waitUntilBlocked yields briefly so a spawned Wait goroutine can reach its
// timer registration before the test advances the manual clock. This is a
// scheduling hint, not a correctness dependency: even if the advance races
// ahead, the retry loop re-registers a timer and the next tick releases it.
func waitUntilBlocked() { time.Sleep(20 * time.Millisecond) }

func TestWaitN_ContextCancel(t *testing.T) {
	global := newFakeBucket(0, true) // always denies
	lim := New(
		Tier{Name: "global", KeyFunc: Constant("all"), Limiter: global},
	)
	ctx, cancel := context.WithCancel(ctxT())
	cancel()
	err := lim.Wait(ctx, "alice")
	if err == nil {
		t.Fatal("expected error on cancelled context")
	}
	var rlErr *ratelimit.RateLimitError
	if !errors.As(err, &rlErr) {
		t.Fatalf("want RateLimitError, got %T: %v", err, err)
	}
}

func TestEmptyTiers(t *testing.T) {
	lim := New()
	if lim.Allow(ctxT(), "x").Allowed {
		t.Error("empty tiered limiter should deny")
	}
	st := lim.Peek(ctxT(), "x")
	if st.Algorithm != algorithmName {
		t.Errorf("peek algorithm: %q", st.Algorithm)
	}
}

func TestInvalidInput(t *testing.T) {
	lim := New(Tier{KeyFunc: identity, Limiter: newFakeBucket(5, true)})
	if lim.Allow(ctxT(), "").Allowed {
		t.Error("empty key should deny")
	}
	if lim.AllowN(ctxT(), "k", 0).Allowed {
		t.Error("n=0 should deny")
	}
}

func TestNilKeyFuncIsIdentity(t *testing.T) {
	user := newFakeBucket(1, true)
	lim := New(Tier{Name: "user", Limiter: user}) // nil KeyFunc
	res := lim.Allow(ctxT(), "raw-key")
	if !res.Allowed {
		t.Fatal("expected allow")
	}
	if user.remaining("raw-key") != 0 {
		t.Errorf("nil KeyFunc should key by request key; remaining=%d", user.remaining("raw-key"))
	}
}

func TestNilLimiterPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("expected panic on nil Limiter")
		}
	}()
	New(Tier{Name: "bad", KeyFunc: identity, Limiter: nil})
}

func TestCostMetadata(t *testing.T) {
	user := newFakeBucket(5, true)
	lim := New(Tier{Name: "user", KeyFunc: identity, Limiter: user})
	res := lim.AllowN(ctxT(), "k", 3)
	if !res.Allowed {
		t.Fatal("expected allow")
	}
	if res.Metadata["cost"] != 3 {
		t.Errorf("cost metadata: want 3, got %v", res.Metadata["cost"])
	}
}

// TestConcurrency_NoLeak stresses the limiter with many concurrent callers and
// asserts the all-or-nothing invariant: the number of successful allows equals
// exactly the tokens debited from the bottleneck tier, and no tier is debited
// more than its allows. Run with -race.
func TestConcurrency_NoLeak(t *testing.T) {
	const users = 8
	const perUser = 50
	// Global bottleneck: capacity chosen so many requests are denied.
	const globalCap = 100
	user := newFakeBucket(1000, true) // effectively unlimited per user here
	global := newFakeBucket(globalCap, true)
	lim := New(
		Tier{Name: "user", KeyFunc: identity, Limiter: user},
		Tier{Name: "global", KeyFunc: Constant("all"), Limiter: global},
	)

	var wg sync.WaitGroup
	var allowed int64
	var mu sync.Mutex
	perUserAllowed := map[string]int{}

	for u := 0; u < users; u++ {
		wg.Add(1)
		key := "user-" + string(rune('A'+u))
		go func(k string) {
			defer wg.Done()
			local := 0
			for i := 0; i < perUser; i++ {
				if lim.Allow(ctxT(), k).Allowed {
					local++
				}
			}
			mu.Lock()
			allowed += int64(local)
			perUserAllowed[k] = local
			mu.Unlock()
		}(key)
	}
	wg.Wait()

	// Global tier should have been debited exactly `allowed` tokens.
	globalDebited := globalCap - global.remaining("all")
	if int64(globalDebited) != allowed {
		t.Fatalf("global debited=%d but allowed=%d: token leak/loss", globalDebited, allowed)
	}
	// Total allows cannot exceed global capacity.
	if allowed > globalCap {
		t.Fatalf("allowed=%d exceeds global capacity=%d", allowed, globalCap)
	}
	// Each user tier debited exactly that user's allow count.
	for k, cnt := range perUserAllowed {
		if debited := 1000 - user.remaining(k); debited != cnt {
			t.Fatalf("user %q debited=%d but allowed=%d", k, debited, cnt)
		}
	}
}
