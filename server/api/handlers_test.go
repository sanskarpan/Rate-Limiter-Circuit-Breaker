package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/fixedwindow"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
)

// ── Test helpers ────────────────────────────────────────────────────────────

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func testHandlers(t *testing.T) *Handlers {
	t.Helper()
	limiters := map[string]ratelimit.Limiter{
		"token_bucket": tokenbucket.New(10, 100), // large cap, fast refill for tests
		"fixed_window": fixedwindow.New(10, time.Second),
	}
	t.Cleanup(func() {
		for _, l := range limiters {
			l.Close() //nolint:errcheck
		}
	})

	registry := circuitbreaker.NewRegistry()
	cb := registry.GetOrCreate("primary", circuitbreaker.Config{
		Name:             "primary",
		WindowType:       circuitbreaker.CountBased,
		WindowSize:       10,
		FailureThreshold: 5,
		OpenTimeout:      time.Second,
	})
	cbs := map[string]*circuitbreaker.CircuitBreaker{"primary": cb}

	hub := newHub(testLogger())
	go hub.Run()
	t.Cleanup(hub.Stop)

	var ready atomic.Bool
	ready.Store(true)

	return NewHandlers(limiters, cbs, registry, hub, testLogger(), &ready)
}


// ── HandleAllow tests ────────────────────────────────────────────────────────

func TestHandleAllow_Allowed(t *testing.T) {
	h := testHandlers(t)
	body := `{"key":"test-user","n":1}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/limiters/token_bucket/allow", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(context.Background())
	req.SetPathValue("algorithm", "token_bucket")
	w := httptest.NewRecorder()

	h.HandleAllow(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body=%s", w.Code, w.Body.String())
	}
	var resp rateLimitResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !resp.Allowed {
		t.Fatal("expected Allowed=true")
	}
	if resp.Algorithm == "" {
		t.Fatal("expected non-empty Algorithm")
	}
}

func TestHandleAllow_UnknownAlgorithm(t *testing.T) {
	h := testHandlers(t)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/limiters/nonexistent/allow", nil)
	req.SetPathValue("algorithm", "nonexistent")
	w := httptest.NewRecorder()

	h.HandleAllow(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestHandleAllow_InvalidKey_Rejected(t *testing.T) {
	h := testHandlers(t)
	// Key with newline — header injection attempt; validateKey rejects \n
	// Note: JSON encodes \n as \\n so the Go decoder returns a string containing '\n'
	body := `{"key":"bad\nkey","n":1}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/limiters/token_bucket/allow", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("algorithm", "token_bucket")
	w := httptest.NewRecorder()

	h.HandleAllow(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid key, got %d; body=%s", w.Code, w.Body.String())
	}
}

func TestHandleAllow_EmptyBody_UsesDefaults(t *testing.T) {
	h := testHandlers(t)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/limiters/token_bucket/allow", nil)
	req.SetPathValue("algorithm", "token_bucket")
	w := httptest.NewRecorder()

	h.HandleAllow(w, req)

	// Should use key="default", n=1 — must not 500
	if w.Code == http.StatusInternalServerError {
		t.Fatalf("unexpected 500; body=%s", w.Body.String())
	}
}

func TestHandleAllow_Denied_Returns429(t *testing.T) {
	// Use a tiny fixed window limiter that's already exhausted
	limiters := map[string]ratelimit.Limiter{
		"fixed_window": fixedwindow.New(1, 10*time.Second),
	}
	defer limiters["fixed_window"].Close()

	registry := circuitbreaker.NewRegistry()
	hub := newHub(testLogger())
	go hub.Run()
	defer hub.Stop()

	var ready atomic.Bool
	ready.Store(true)
	h := NewHandlers(limiters, nil, registry, hub, testLogger(), &ready)

	// First request consumes the only slot
	req1 := httptest.NewRequest(http.MethodPost, "/api/v1/limiters/fixed_window/allow",
		strings.NewReader(`{"key":"usr","n":1}`))
	req1.SetPathValue("algorithm", "fixed_window")
	w1 := httptest.NewRecorder()
	h.HandleAllow(w1, req1)

	// Second request should be denied
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/limiters/fixed_window/allow",
		strings.NewReader(`{"key":"usr","n":1}`))
	req2.SetPathValue("algorithm", "fixed_window")
	w2 := httptest.NewRecorder()
	h.HandleAllow(w2, req2)

	if w2.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d; body=%s", w2.Code, w2.Body.String())
	}

	var resp rateLimitResponse
	if err := json.NewDecoder(w2.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Allowed {
		t.Fatal("expected Allowed=false in 429 response")
	}
}

// ── HandleState tests ────────────────────────────────────────────────────────

func TestHandleState_ReturnsState(t *testing.T) {
	h := testHandlers(t)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/limiters/token_bucket/state?key=mykey", nil)
	req.SetPathValue("algorithm", "token_bucket")
	w := httptest.NewRecorder()

	h.HandleState(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body=%s", w.Code, w.Body.String())
	}

	var body map[string]any
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["algorithm"] == nil {
		t.Fatal("expected 'algorithm' field in state response")
	}
}

func TestHandleState_InvalidKey_Rejected(t *testing.T) {
	h := testHandlers(t)
	// Key longer than 512 bytes — exceeds max length
	longKey := strings.Repeat("a", 513)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/limiters/token_bucket/state?key="+longKey, nil)
	req.SetPathValue("algorithm", "token_bucket")
	w := httptest.NewRecorder()

	h.HandleState(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid key (too long), got %d", w.Code)
	}
}

// ── HandleCBSnapshot tests ───────────────────────────────────────────────────

func TestHandleCBSnapshot_KnownCB(t *testing.T) {
	h := testHandlers(t)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/cb/primary/snapshot", nil)
	req.SetPathValue("name", "primary")
	w := httptest.NewRecorder()

	h.HandleCBSnapshot(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body=%s", w.Code, w.Body.String())
	}

	var snap snapshotJSON
	if err := json.NewDecoder(w.Body).Decode(&snap); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if snap.Name != "primary" {
		t.Fatalf("expected name='primary', got %q", snap.Name)
	}
	if snap.State == "" {
		t.Fatal("expected non-empty State in snapshot")
	}
}

func TestHandleCBSnapshot_UnknownCB(t *testing.T) {
	h := testHandlers(t)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/cb/unknown/snapshot", nil)
	req.SetPathValue("name", "unknown")
	w := httptest.NewRecorder()

	h.HandleCBSnapshot(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

// ── HandleCBAll tests ────────────────────────────────────────────────────────

func TestHandleCBAll_ReturnsAll(t *testing.T) {
	h := testHandlers(t)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/cb/all", nil)
	w := httptest.NewRecorder()

	h.HandleCBAll(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body=%s", w.Code, w.Body.String())
	}

	var body map[string]*snapshotJSON
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, ok := body["primary"]; !ok {
		t.Fatal("expected 'primary' circuit breaker in /cb/all response")
	}
}

// ── HandleCBExecute tests ────────────────────────────────────────────────────

func TestHandleCBExecute_Success(t *testing.T) {
	h := testHandlers(t)
	body := `{"simulate_failure":false,"latency_ms":0}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/cb/primary/execute", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("name", "primary")
	w := httptest.NewRecorder()

	h.HandleCBExecute(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body=%s", w.Code, w.Body.String())
	}

	var resp cbResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error != "" {
		t.Fatalf("unexpected error: %s", resp.Error)
	}
}

func TestHandleCBExecute_SimulateFailure(t *testing.T) {
	h := testHandlers(t)
	body := `{"simulate_failure":true}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/cb/primary/execute", strings.NewReader(body))
	req.SetPathValue("name", "primary")
	w := httptest.NewRecorder()

	h.HandleCBExecute(w, req)

	// Should be 200 (CB still closed, just reported error)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 on simulated failure (CB still closed), got %d", w.Code)
	}

	var resp cbResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error == "" {
		t.Fatal("expected Error to be set on simulated failure")
	}
}

func TestHandleCBExecute_LatencyCapped(t *testing.T) {
	h := testHandlers(t)
	// Latency of 999999ms should be capped to 5000ms (and we time out fast)
	body := `{"simulate_failure":false,"latency_ms":999999}`
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/cb/primary/execute", strings.NewReader(body))
	req.SetPathValue("name", "primary")
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()

	// Should return before 999999ms — context cancels it
	done := make(chan struct{})
	go func() {
		h.HandleCBExecute(w, req)
		close(done)
	}()

	select {
	case <-done:
		// Good — returned within context timeout
	case <-time.After(200 * time.Millisecond):
		t.Fatal("HandleCBExecute did not respect context cancellation")
	}
}

// ── HandleSimulate tests ─────────────────────────────────────────────────────

func TestHandleSimulate_Basic(t *testing.T) {
	h := testHandlers(t)
	body := `{"algorithm":"token_bucket","pattern":"constant","duration_ms":100,"requests_per_second":10,"concurrency":1}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/simulate", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(context.Background())
	w := httptest.NewRecorder()

	h.HandleSimulate(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body=%s", w.Code, w.Body.String())
	}
}

func TestHandleSimulate_UnknownAlgorithm(t *testing.T) {
	h := testHandlers(t)
	body := `{"algorithm":"nonexistent"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/simulate", strings.NewReader(body))
	w := httptest.NewRecorder()

	h.HandleSimulate(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestHandleSimulate_ConcurrencyCapped(t *testing.T) {
	h := testHandlers(t)
	// concurrency=999999 should be capped to 500
	body := `{"algorithm":"token_bucket","duration_ms":50,"concurrency":999999}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/simulate", strings.NewReader(body))
	req = req.WithContext(context.Background())
	w := httptest.NewRecorder()

	// Should not spawn 999999 goroutines — just verify it completes
	done := make(chan struct{})
	go func() {
		h.HandleSimulate(w, req)
		close(done)
	}()

	select {
	case <-done:
		// Good
	case <-time.After(5 * time.Second):
		t.Fatal("HandleSimulate did not complete within 5s (may have spawned too many goroutines)")
	}
}

func TestHandleSimulate_DurationCapped(t *testing.T) {
	h := testHandlers(t)
	// duration=999999000ms (999999s) should be capped to 60000ms
	body := `{"algorithm":"token_bucket","duration_ms":999999000,"concurrency":1}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/simulate", strings.NewReader(body))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()

	// With cap at 60s, context timeout (2s) should cancel it
	done := make(chan struct{})
	go func() {
		h.HandleSimulate(w, req)
		close(done)
	}()

	select {
	case <-done:
		// Good — context cancelled it
	case <-time.After(3 * time.Second):
		t.Fatal("HandleSimulate did not respect context cancellation with capped duration")
	}
}

func TestHandleSimulate_InvalidKey_Rejected(t *testing.T) {
	h := testHandlers(t)
	// Newline in key — header injection attempt
	body := `{"algorithm":"token_bucket","key":"bad\nkey"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/simulate", strings.NewReader(body))
	w := httptest.NewRecorder()

	h.HandleSimulate(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid key, got %d; body=%s", w.Code, w.Body.String())
	}
}

// ── Middleware tests ─────────────────────────────────────────────────────────

func TestLimitRequestBody_RejectsOversizedBody(t *testing.T) {
	// Build a handler that would just return 200
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := LimitRequestBody(inner)

	// Build a request with Content-Length > maxRequestBodyBytes
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("x"))
	req.ContentLength = maxRequestBodyBytes + 1
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", w.Code)
	}
}

func TestSecurityHeaders_Set(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := SecurityHeaders(inner)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	checks := map[string]string{
		"X-Content-Type-Options": "nosniff",
		"X-Frame-Options":        "DENY",
		"Referrer-Policy":        "strict-origin-when-cross-origin",
	}
	for header, expected := range checks {
		if got := w.Header().Get(header); got != expected {
			t.Errorf("header %s: expected %q, got %q", header, expected, got)
		}
	}
	if w.Header().Get("Content-Security-Policy") == "" {
		t.Error("expected Content-Security-Policy header to be set")
	}
}

func TestCORS_ExactOrigin(t *testing.T) {
	handler := CORS([]string{"http://localhost:3000"})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Origin", "http://localhost:3000")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if got := w.Header().Get("Access-Control-Allow-Origin"); got != "http://localhost:3000" {
		t.Fatalf("expected exact origin, got %q", got)
	}
}

func TestCORS_UnknownOriginNotReflected(t *testing.T) {
	handler := CORS([]string{"http://localhost:3000"})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Origin", "http://evil.example.com")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if got := w.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("unknown origin must not be reflected, got %q", got)
	}
}

func TestCORS_WildcardReturnsLiteral(t *testing.T) {
	handler := CORS([]string{"*"})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Origin", "http://any.example.com")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if got := w.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("wildcard config must return literal '*', got %q", got)
	}
}

func TestCORS_Preflight(t *testing.T) {
	handler := CORS([]string{"http://localhost:3000"})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("inner handler must not be called on OPTIONS preflight")
	}))

	req := httptest.NewRequest(http.MethodOptions, "/", nil)
	req.Header.Set("Origin", "http://localhost:3000")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204 on OPTIONS preflight, got %d", w.Code)
	}
}

func TestRecovery_CatchesPanic(t *testing.T) {
	handler := Recovery(testLogger())(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("deliberate test panic")
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()

	// Should not re-panic
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 after panic, got %d", w.Code)
	}
}

func TestRequestID_GeneratesID(t *testing.T) {
	var captured string
	handler := RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured = requestIDFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if captured == "" {
		t.Fatal("expected request ID to be generated")
	}
	if w.Header().Get("X-Request-ID") != captured {
		t.Fatal("X-Request-ID header must match context value")
	}
}

func TestRequestID_PropagatesExisting(t *testing.T) {
	var captured string
	handler := RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured = requestIDFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Request-ID", "my-trace-id")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if captured != "my-trace-id" {
		t.Fatalf("expected propagated request ID 'my-trace-id', got %q", captured)
	}
}

// ── Concurrency safety tests ─────────────────────────────────────────────────

func TestHandleAllow_Concurrent_NoRace(t *testing.T) {
	h := testHandlers(t)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("user-%d", i%10)
			body := fmt.Sprintf(`{"key":%q,"n":1}`, key)
			req := httptest.NewRequest(http.MethodPost, "/api/v1/limiters/token_bucket/allow", strings.NewReader(body))
			req.SetPathValue("algorithm", "token_bucket")
			w := httptest.NewRecorder()
			h.HandleAllow(w, req)
		}(i)
	}
	wg.Wait()
}

func TestHandleCBAll_Concurrent_NoRace(t *testing.T) {
	h := testHandlers(t)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodGet, "/api/v1/cb/all", nil)
			w := httptest.NewRecorder()
			h.HandleCBAll(w, req)
		}()
	}
	wg.Wait()
}

// ── validateKey tests (via handlers) ────────────────────────────────────────

func TestValidateKey_AllowsNormal(t *testing.T) {
	cases := []string{"user-123", "api-key-abc", "tenant.prod", "x" + strings.Repeat("y", 100)}
	for _, key := range cases {
		if err := validateKey(key); err != nil {
			t.Errorf("validateKey(%q) returned unexpected error: %v", key, err)
		}
	}
}

func TestValidateKey_RejectsInvalid(t *testing.T) {
	cases := []struct {
		key    string
		reason string
	}{
		{"", "empty key"},
		{"\x00null", "null byte"},
		{"key\ninjection", "newline"},
		{"key\rinjection", "carriage return"},
		{strings.Repeat("x", 513), "too long"},
	}
	for _, tc := range cases {
		if err := validateKey(tc.key); err == nil {
			t.Errorf("validateKey(%q) expected error for %s, got nil", tc.key, tc.reason)
		}
	}
}
