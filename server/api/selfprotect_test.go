package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
)

// newProtectedTestServer builds the full router with an explicit
// SelfProtectConfig so the §7.4 self-protection layer can be exercised
// end-to-end through the real middleware chain.
func newProtectedTestServer(t *testing.T, sp SelfProtectConfig) *httptest.Server {
	t.Helper()
	limiters := map[string]ratelimit.Limiter{
		"token_bucket": tokenbucket.New(1_000_000, 1_000_000), // effectively unlimited
	}
	t.Cleanup(func() {
		for _, l := range limiters {
			l.Close() //nolint:errcheck
		}
	})

	registry := circuitbreaker.NewRegistry()
	registry.GetOrCreate("primary", circuitbreaker.Config{
		Name:             "primary",
		WindowType:       circuitbreaker.CountBased,
		WindowSize:       10,
		FailureThreshold: 5,
		OpenTimeout:      time.Second,
	})
	cbs := map[string]*circuitbreaker.CircuitBreaker{"primary": registry.Get("primary")}

	var ready atomic.Bool
	ready.Store(true)

	handler, _, closer := NewRouterWithHubAndProtection(
		limiters, cbs, registry, testLogger(), &ready,
		[]string{"http://localhost:3000"}, "", sp)
	t.Cleanup(closer)

	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return srv
}

// TestSelfProtect_OversizedBodyRejected proves §7.4(1): oversized bodies are
// rejected with 413 using the configurable body-size cap.
func TestSelfProtect_OversizedBodyRejected(t *testing.T) {
	sp := DefaultSelfProtectConfig()
	sp.MaxRequestBytes = 64 // tiny cap for the test
	srv := newProtectedTestServer(t, sp)

	big := `{"key":"` + strings.Repeat("a", 200) + `"}`
	resp := do(t, http.MethodPost, srv.URL+"/api/v1/limiters/token_bucket/allow",
		big, map[string]string{"Content-Type": "application/json"})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413 for oversized body, got %d", resp.StatusCode)
	}
}

// TestSelfProtect_PerIPRateLimitFloods proves §7.4(3): the dogfooded per-IP rate
// limiter rejects a flood with 429 without affecting a slow/normal client.
func TestSelfProtect_PerIPRateLimitFloods(t *testing.T) {
	sp := SelfProtectConfig{
		MaxRequestBytes: 1 << 20,
		RatePerIP:       1, // 1 req/s sustained
		Burst:           3, // small burst
		MaxInflight:     128,
	}
	srv := newProtectedTestServer(t, sp)

	body := `{"key":"flood","n":1}`
	hdr := map[string]string{"Content-Type": "application/json"}

	var got429 bool
	var okCount int
	// Fire a burst well beyond the bucket capacity; the token bucket must start
	// denying with 429.
	for i := 0; i < 20; i++ {
		resp := do(t, http.MethodPost, srv.URL+"/api/v1/limiters/token_bucket/allow", body, hdr)
		switch resp.StatusCode {
		case http.StatusTooManyRequests:
			got429 = true
		case http.StatusOK:
			okCount++
		}
		resp.Body.Close()
	}
	if !got429 {
		t.Fatalf("expected at least one 429 from the per-IP self rate limit, got none (ok=%d)", okCount)
	}
	if okCount == 0 {
		t.Fatalf("expected some requests to pass (burst), got none")
	}
	if okCount > 6 {
		t.Fatalf("per-IP limiter too permissive: %d requests passed with burst=3", okCount)
	}
}

// TestSelfProtect_NormalTrafficUnaffected proves normal traffic still succeeds:
// a read-only endpoint (health) is never throttled by self-protection.
func TestSelfProtect_NormalTrafficUnaffected(t *testing.T) {
	sp := SelfProtectConfig{
		MaxRequestBytes: 1 << 20,
		RatePerIP:       1,
		Burst:           1,
		MaxInflight:     1,
	}
	srv := newProtectedTestServer(t, sp)

	// Health endpoint is outside the protected control plane; hammer it and it
	// must always return 200 regardless of the strict self-protection limits.
	for i := 0; i < 50; i++ {
		resp := do(t, http.MethodGet, srv.URL+"/health/live", "", nil)
		code := resp.StatusCode
		resp.Body.Close()
		if code != http.StatusOK {
			t.Fatalf("health request %d: expected 200 (unaffected by self-protection), got %d", i, code)
		}
	}
}

// TestSelfProtect_InflightGuardShedsLoad proves §7.4(4): the global concurrency
// guard sheds load with 503 when the in-flight cap is exceeded. It uses a
// slow-latency CB execute so requests overlap.
func TestSelfProtect_InflightGuardShedsLoad(t *testing.T) {
	sp := SelfProtectConfig{
		MaxRequestBytes: 1 << 20,
		RatePerIP:       0, // disable per-IP limit to isolate the inflight guard
		MaxInflight:     2, // only 2 concurrent control-plane requests
	}
	srv := newProtectedTestServer(t, sp)

	// LatencyMs makes HandleCBExecute hold an inflight slot long enough that
	// concurrent requests contend for the 2 available slots.
	body := `{"simulate_failure":false,"latency_ms":300}`
	hdr := map[string]string{"Content-Type": "application/json"}

	const n = 12
	var wg sync.WaitGroup
	var got503 atomic.Bool
	var okCount atomic.Int64
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp := do(t, http.MethodPost, srv.URL+"/api/v1/cb/primary/execute", body, hdr)
			switch resp.StatusCode {
			case http.StatusServiceUnavailable:
				got503.Store(true)
			case http.StatusOK:
				okCount.Add(1)
			}
			resp.Body.Close()
		}()
	}
	wg.Wait()

	if !got503.Load() {
		t.Fatalf("expected at least one 503 from the inflight concurrency guard, got none (ok=%d)", okCount.Load())
	}
}

// TestSelfProtect_DisabledGuards verifies that zero/negative config disables the
// guards so the middleware is a safe no-op.
func TestSelfProtect_DisabledGuards(t *testing.T) {
	sp := SelfProtectConfig{
		MaxRequestBytes: 0, // fall back to default
		RatePerIP:       0, // disabled
		MaxInflight:     0, // disabled
	}
	spro, closer := newSelfProtector(sp)
	defer closer()
	if spro.limiter != nil {
		t.Fatalf("expected nil limiter when RatePerIP<=0")
	}
	if spro.inflight != nil {
		t.Fatalf("expected nil inflight guard when MaxInflight<=0")
	}
	if got := sp.effectiveMaxRequestBytes(); got != maxRequestBodyBytes {
		t.Fatalf("expected default body cap, got %d", got)
	}
}

// TestClientIP verifies per-IP key extraction (used as the rate-limit key).
func TestClientIP(t *testing.T) {
	tests := []struct {
		name       string
		remoteAddr string
		xff        string
		want       string
	}{
		{"remote host:port", "203.0.113.7:54321", "", "203.0.113.7"},
		{"xff single", "10.0.0.1:5000", "198.51.100.9", "198.51.100.9"},
		{"xff list", "10.0.0.1:5000", "198.51.100.9, 10.0.0.1", "198.51.100.9"},
		{"no port", "bareaddr", "", "bareaddr"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			r.RemoteAddr = tc.remoteAddr
			if tc.xff != "" {
				r.Header.Set("X-Forwarded-For", tc.xff)
			}
			if got := clientIP(r); got != tc.want {
				t.Fatalf("clientIP=%q want %q", got, tc.want)
			}
		})
	}
}
