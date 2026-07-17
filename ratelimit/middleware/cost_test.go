package middleware_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/middleware"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
)

// spyLimiter records the last n passed to AllowN and delegates to an inner
// limiter so behavior (allow/deny, remaining) stays realistic.
type spyLimiter struct {
	inner ratelimit.Limiter

	mu       sync.Mutex
	lastN    int
	allowNHt int
	allowHit int
}

func (s *spyLimiter) Allow(ctx context.Context, key string) ratelimit.Result {
	s.mu.Lock()
	s.allowHit++
	s.lastN = 1
	s.mu.Unlock()
	return s.inner.Allow(ctx, key)
}

func (s *spyLimiter) AllowN(ctx context.Context, key string, n int) ratelimit.Result {
	s.mu.Lock()
	s.allowNHt++
	s.lastN = n
	s.mu.Unlock()
	return s.inner.AllowN(ctx, key, n)
}

func (s *spyLimiter) Wait(ctx context.Context, key string) error { return s.inner.Wait(ctx, key) }
func (s *spyLimiter) WaitN(c context.Context, k string, n int) error {
	return s.inner.WaitN(c, k, n)
}
func (s *spyLimiter) Peek(ctx context.Context, key string) ratelimit.State {
	return s.inner.Peek(ctx, key)
}
func (s *spyLimiter) Reset(ctx context.Context, key string) error { return s.inner.Reset(ctx, key) }
func (s *spyLimiter) Close() error                                { return s.inner.Close() }

// TestHTTP_WithCost_ConsumesN verifies WithCost(5) makes each request consume 5
// tokens (limiter exhausts 5x faster), sets the X-RateLimit-Cost header, and
// surfaces Result.Metadata["cost"]==5 on the underlying limiter.
func TestHTTP_WithCost_ConsumesN(t *testing.T) {
	// Capacity 10, refill effectively frozen for the test window: with cost 5
	// exactly two requests fit, the third is denied.
	tb := tokenbucket.New(10, 1)
	defer tb.Close()
	spy := &spyLimiter{inner: tb}

	mw := middleware.RateLimit(spy,
		middleware.WithKeyFunc(func(*http.Request) string { return "k" }),
		middleware.WithCost(func(*http.Request) int { return 5 }),
	)
	handler := mw(okHandler)

	do := func() *httptest.ResponseRecorder {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		handler.ServeHTTP(rr, req)
		return rr
	}

	// Request 1: allowed, cost header set to 5.
	rr1 := do()
	if rr1.Code != http.StatusOK {
		t.Fatalf("req1: expected 200, got %d", rr1.Code)
	}
	if got := rr1.Header().Get("X-RateLimit-Cost"); got != "5" {
		t.Fatalf("req1: expected X-RateLimit-Cost=5, got %q", got)
	}

	// Request 2: allowed (10 total consumed).
	if rr2 := do(); rr2.Code != http.StatusOK {
		t.Fatalf("req2: expected 200, got %d", rr2.Code)
	}

	// Request 3: denied (bucket exhausted after 2x cost-5), cost header still set.
	rr3 := do()
	if rr3.Code != http.StatusTooManyRequests {
		t.Fatalf("req3: expected 429 (5x-faster exhaustion), got %d", rr3.Code)
	}
	if got := rr3.Header().Get("X-RateLimit-Cost"); got != "5" {
		t.Fatalf("req3: expected X-RateLimit-Cost=5 on deny, got %q", got)
	}

	// The middleware routed through AllowN with n=5, never the single-token path.
	spy.mu.Lock()
	defer spy.mu.Unlock()
	if spy.lastN != 5 {
		t.Fatalf("expected AllowN called with n=5, last n=%d", spy.lastN)
	}
	if spy.allowHit != 0 {
		t.Fatalf("expected single-token Allow never called, hits=%d", spy.allowHit)
	}

	// Underlying limiter surfaces cost in Metadata for an n>1 consume.
	res := tokenbucket.New(10, 1)
	defer res.Close()
	r := res.AllowN(context.Background(), "x", 5)
	if r.Metadata == nil || r.Metadata["cost"] != 5 {
		t.Fatalf("expected Metadata[cost]==5, got %v", r.Metadata)
	}
}

// TestHTTP_WithCost_DefaultsToOne verifies the absence of WithCost keeps the
// single-token fast path (cost header = 1, Allow used).
func TestHTTP_WithCost_DefaultsToOne(t *testing.T) {
	tb := tokenbucket.New(10, 1)
	defer tb.Close()
	spy := &spyLimiter{inner: tb}

	mw := middleware.RateLimit(spy,
		middleware.WithKeyFunc(func(*http.Request) string { return "k" }),
	)
	handler := mw(okHandler)

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if got := rr.Header().Get("X-RateLimit-Cost"); got != "1" {
		t.Fatalf("expected default X-RateLimit-Cost=1, got %q", got)
	}

	spy.mu.Lock()
	defer spy.mu.Unlock()
	if spy.allowHit != 1 || spy.allowNHt != 0 {
		t.Fatalf("expected single-token Allow path, allowHit=%d allowNHt=%d", spy.allowHit, spy.allowNHt)
	}
}
