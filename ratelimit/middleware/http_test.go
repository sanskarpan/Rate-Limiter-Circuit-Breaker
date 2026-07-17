package middleware_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/middleware"
)

// mockLimiter is a test double for ratelimit.Limiter.
type mockLimiter struct {
	result ratelimit.Result
}

func (m *mockLimiter) Allow(_ context.Context, _ string) ratelimit.Result  { return m.result }
func (m *mockLimiter) AllowN(_ context.Context, _ string, _ int) ratelimit.Result {
	return m.result
}
func (m *mockLimiter) Wait(_ context.Context, _ string) error         { return nil }
func (m *mockLimiter) WaitN(_ context.Context, _ string, _ int) error { return nil }
func (m *mockLimiter) Peek(_ context.Context, _ string) ratelimit.State {
	return ratelimit.State{}
}
func (m *mockLimiter) Reset(_ context.Context, _ string) error { return nil }
func (m *mockLimiter) Close() error                            { return nil }

// okHandler is a simple HTTP handler that writes 200 OK.
var okHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
})

func newRequest(method, target string) *http.Request {
	req := httptest.NewRequest(method, target, nil)
	req.RemoteAddr = "192.0.2.1:1234"
	return req
}

// --- Tests for RateLimit middleware ---

func TestRateLimit_Allowed(t *testing.T) {
	limiter := &mockLimiter{result: ratelimit.Result{
		Allowed:    true,
		Limit:      100,
		Remaining:  99,
		ResetAfter: 60 * time.Second,
		Algorithm:  "token_bucket",
	}}

	handler := middleware.RateLimit(limiter)(okHandler)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, newRequest(http.MethodGet, "/"))

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
	if got := rec.Header().Get("X-RateLimit-Limit"); got != "100" {
		t.Errorf("X-RateLimit-Limit: expected 100, got %q", got)
	}
	if got := rec.Header().Get("X-RateLimit-Remaining"); got != "99" {
		t.Errorf("X-RateLimit-Remaining: expected 99, got %q", got)
	}
	reset := rec.Header().Get("X-RateLimit-Reset")
	if reset == "" {
		t.Error("X-RateLimit-Reset should be set")
	}
	ts, err := strconv.ParseInt(reset, 10, 64)
	if err != nil {
		t.Errorf("X-RateLimit-Reset is not a unix timestamp: %v", err)
	}
	// Reset timestamp should be in the future.
	if ts <= time.Now().Unix() {
		t.Errorf("X-RateLimit-Reset %d should be in the future", ts)
	}
}

func TestRateLimit_Denied_DefaultResponse(t *testing.T) {
	limiter := &mockLimiter{result: ratelimit.Result{
		Allowed:    false,
		Limit:      10,
		Remaining:  0,
		RetryAfter: 5 * time.Second,
		Algorithm:  "token_bucket",
	}}

	handler := middleware.RateLimit(limiter)(okHandler)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, newRequest(http.MethodGet, "/"))

	if rec.Code != http.StatusTooManyRequests {
		t.Errorf("expected 429, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type: expected application/json, got %q", ct)
	}
	if got := rec.Header().Get("X-RateLimit-Limit"); got != "10" {
		t.Errorf("X-RateLimit-Limit: expected 10, got %q", got)
	}
	if got := rec.Header().Get("X-RateLimit-Remaining"); got != "0" {
		t.Errorf("X-RateLimit-Remaining: expected 0, got %q", got)
	}
	if got := rec.Header().Get("Retry-After"); got != "5" {
		t.Errorf("Retry-After: expected 5, got %q", got)
	}

	var body map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode body: %v", err)
	}
	if body["error"] != "rate_limit_exceeded" {
		t.Errorf("body.error: expected rate_limit_exceeded, got %v", body["error"])
	}
	// retry_after should be present as a number.
	if _, ok := body["retry_after"]; !ok {
		t.Error("body.retry_after should be present")
	}
}

func TestRateLimit_Denied_NoRetryAfter(t *testing.T) {
	limiter := &mockLimiter{result: ratelimit.Result{
		Allowed:   false,
		Limit:     10,
		Remaining: 0,
		// RetryAfter is zero — Retry-After header must NOT be set.
	}}

	handler := middleware.RateLimit(limiter)(okHandler)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, newRequest(http.MethodGet, "/"))

	if rec.Code != http.StatusTooManyRequests {
		t.Errorf("expected 429, got %d", rec.Code)
	}
	if got := rec.Header().Get("Retry-After"); got != "" {
		t.Errorf("Retry-After should not be set when RetryAfter==0, got %q", got)
	}

	var body map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode body: %v", err)
	}
	if _, ok := body["retry_after"]; ok {
		t.Error("body.retry_after should not be present when RetryAfter==0")
	}
}

func TestRateLimit_HeaderNotSetWhenResetAfterZero(t *testing.T) {
	limiter := &mockLimiter{result: ratelimit.Result{
		Allowed:    true,
		Limit:      10,
		Remaining:  5,
		ResetAfter: 0, // zero — X-RateLimit-Reset should not be set
	}}

	handler := middleware.RateLimit(limiter)(okHandler)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, newRequest(http.MethodGet, "/"))

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
	if got := rec.Header().Get("X-RateLimit-Reset"); got != "" {
		t.Errorf("X-RateLimit-Reset should not be set when ResetAfter==0, got %q", got)
	}
}

func TestRateLimit_DownstreamNotCalledWhenDenied(t *testing.T) {
	called := false
	downstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	})
	limiter := &mockLimiter{result: ratelimit.Result{Allowed: false, Limit: 1}}
	handler := middleware.RateLimit(limiter)(downstream)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, newRequest(http.MethodGet, "/"))

	if called {
		t.Error("downstream handler must not be called when request is denied")
	}
}

// --- Tests for KeyFunc options ---

func TestKeyByIP_XForwardedFor(t *testing.T) {
	var capturedKey string
	limiter := &mockLimiter{result: ratelimit.Result{Allowed: true, Limit: 10, Remaining: 9}}

	handler := middleware.RateLimit(limiter, middleware.WithKeyFunc(func(r *http.Request) string {
		key := middleware.KeyByIP()(r)
		capturedKey = key
		return key
	}))(okHandler)

	req := newRequest(http.MethodGet, "/")
	req.Header.Set("X-Forwarded-For", "10.0.0.1, 10.0.0.2")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if capturedKey != "10.0.0.1" {
		t.Errorf("KeyByIP with X-Forwarded-For: expected 10.0.0.1, got %q", capturedKey)
	}
}

func TestKeyByIP_XRealIP(t *testing.T) {
	var capturedKey string
	limiter := &mockLimiter{result: ratelimit.Result{Allowed: true, Limit: 10, Remaining: 9}}

	handler := middleware.RateLimit(limiter, middleware.WithKeyFunc(func(r *http.Request) string {
		key := middleware.KeyByIP()(r)
		capturedKey = key
		return key
	}))(okHandler)

	req := newRequest(http.MethodGet, "/")
	req.Header.Set("X-Real-IP", "10.0.0.5")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if capturedKey != "10.0.0.5" {
		t.Errorf("KeyByIP with X-Real-IP: expected 10.0.0.5, got %q", capturedKey)
	}
}

func TestKeyByIP_RemoteAddr(t *testing.T) {
	var capturedKey string
	limiter := &mockLimiter{result: ratelimit.Result{Allowed: true, Limit: 10, Remaining: 9}}

	handler := middleware.RateLimit(limiter, middleware.WithKeyFunc(func(r *http.Request) string {
		key := middleware.KeyByIP()(r)
		capturedKey = key
		return key
	}))(okHandler)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "203.0.113.7:54321"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if capturedKey != "203.0.113.7" {
		t.Errorf("KeyByIP from RemoteAddr: expected 203.0.113.7, got %q", capturedKey)
	}
}

func TestKeyByHeader(t *testing.T) {
	var capturedKey string
	limiter := &mockLimiter{result: ratelimit.Result{Allowed: true, Limit: 10, Remaining: 9}}

	keyFn := middleware.KeyByHeader("X-API-Key")
	handler := middleware.RateLimit(limiter, middleware.WithKeyFunc(func(r *http.Request) string {
		key := keyFn(r)
		capturedKey = key
		return key
	}))(okHandler)

	req := newRequest(http.MethodGet, "/")
	req.Header.Set("X-API-Key", "my-api-token")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if capturedKey != "my-api-token" {
		t.Errorf("KeyByHeader: expected my-api-token, got %q", capturedKey)
	}
}

func TestKeyByParam(t *testing.T) {
	var capturedKey string
	limiter := &mockLimiter{result: ratelimit.Result{Allowed: true, Limit: 10, Remaining: 9}}

	keyFn := middleware.KeyByParam("tenant")
	handler := middleware.RateLimit(limiter, middleware.WithKeyFunc(func(r *http.Request) string {
		key := keyFn(r)
		capturedKey = key
		return key
	}))(okHandler)

	req := newRequest(http.MethodGet, "/?tenant=acme")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if capturedKey != "acme" {
		t.Errorf("KeyByParam: expected acme, got %q", capturedKey)
	}
}

// --- Tests for Option functions ---

func TestWithSkipFunc_Skips(t *testing.T) {
	// Limiter always denies, but skipFunc returns true → should pass through.
	limiter := &mockLimiter{result: ratelimit.Result{Allowed: false, Limit: 0}}

	handler := middleware.RateLimit(
		limiter,
		middleware.WithSkipFunc(func(r *http.Request) bool {
			return r.Header.Get("X-Internal") == "true"
		}),
	)(okHandler)

	req := newRequest(http.MethodGet, "/")
	req.Header.Set("X-Internal", "true")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("skipped request should get 200, got %d", rec.Code)
	}
}

func TestWithSkipFunc_DoesNotSkip(t *testing.T) {
	// Limiter always denies; skipFunc returns false → should be rate limited.
	limiter := &mockLimiter{result: ratelimit.Result{Allowed: false, Limit: 1}}

	handler := middleware.RateLimit(
		limiter,
		middleware.WithSkipFunc(func(r *http.Request) bool {
			return false
		}),
	)(okHandler)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, newRequest(http.MethodGet, "/"))

	if rec.Code != http.StatusTooManyRequests {
		t.Errorf("non-skipped denied request should get 429, got %d", rec.Code)
	}
}

func TestWithOnLimited_CustomHandler(t *testing.T) {
	limiter := &mockLimiter{result: ratelimit.Result{Allowed: false, Limit: 5}}

	customCalled := false
	handler := middleware.RateLimit(
		limiter,
		middleware.WithOnLimited(func(w http.ResponseWriter, r *http.Request, result ratelimit.Result) {
			customCalled = true
			w.WriteHeader(http.StatusForbidden) // use 403 instead of 429 in custom handler
		}),
	)(okHandler)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, newRequest(http.MethodGet, "/"))

	if !customCalled {
		t.Error("custom onLimited handler was not called")
	}
	if rec.Code != http.StatusForbidden {
		t.Errorf("expected custom status 403, got %d", rec.Code)
	}
}

func TestWithKeyFunc_Custom(t *testing.T) {
	var gotKey string
	limiter := &mockLimiter{result: ratelimit.Result{Allowed: true, Limit: 10, Remaining: 9}}

	handler := middleware.RateLimit(
		limiter,
		middleware.WithKeyFunc(func(r *http.Request) string {
			gotKey = "custom-key"
			return gotKey
		}),
	)(okHandler)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, newRequest(http.MethodGet, "/"))

	if gotKey != "custom-key" {
		t.Errorf("expected custom-key, got %q", gotKey)
	}
}

// --- Test that headers from the default onLimited don't get doubled ---

func TestRateLimit_HeadersSetOnce(t *testing.T) {
	limiter := &mockLimiter{result: ratelimit.Result{
		Allowed:   false,
		Limit:     5,
		Remaining: 0,
	}}

	handler := middleware.RateLimit(limiter)(okHandler)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, newRequest(http.MethodGet, "/"))

	// Should be set once.
	if vals := rec.Result().Header["X-Ratelimit-Limit"]; len(vals) != 1 {
		t.Errorf("X-RateLimit-Limit should appear exactly once, got %v", vals)
	}
}
