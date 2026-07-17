package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskarpan/resilience/circuitbreaker"
	"github.com/sanskarpan/resilience/ratelimit"
	"github.com/sanskarpan/resilience/ratelimit/tokenbucket"
)

// newTestServer assembles the full router via NewRouter (exercising the real
// middleware chain) and returns an httptest.Server (TQ-3). Passing apiKey=""
// runs in demo mode; a non-empty key enforces authentication.
func newTestServer(t *testing.T, apiKey string) *httptest.Server {
	t.Helper()
	limiters := map[string]ratelimit.Limiter{
		"token_bucket": tokenbucket.New(100, 100),
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

	handler := NewRouter(limiters, cbs, registry, testLogger(), &ready,
		[]string{"http://localhost:3000"}, apiKey)
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return srv
}

func do(t *testing.T, method, url, body string, headers map[string]string) *http.Response {
	t.Helper()
	var r *strings.Reader
	if body != "" {
		r = strings.NewReader(body)
	} else {
		r = strings.NewReader("")
	}
	req, err := http.NewRequest(method, url, r)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request %s %s: %v", method, url, err)
	}
	return resp
}

// ── C-1: API-key auth on mutating routes ─────────────────────────────────────

func TestRouter_Auth_MutatingRouteRequiresKey(t *testing.T) {
	srv := newTestServer(t, "topsecret")

	// Without a key → 401.
	resp := do(t, http.MethodPost, srv.URL+"/api/v1/cb/primary/execute",
		`{"simulate_failure":false}`, map[string]string{"Content-Type": "application/json"})
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no key: expected 401, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// Wrong key → 401.
	resp = do(t, http.MethodPost, srv.URL+"/api/v1/cb/primary/execute",
		`{"simulate_failure":false}`, map[string]string{
			"Content-Type":  "application/json",
			"Authorization": "Bearer wrong",
		})
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong key: expected 401, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// Correct Bearer key → 2xx.
	resp = do(t, http.MethodPost, srv.URL+"/api/v1/cb/primary/execute",
		`{"simulate_failure":false}`, map[string]string{
			"Content-Type":  "application/json",
			"Authorization": "Bearer topsecret",
		})
	if resp.StatusCode/100 != 2 {
		t.Fatalf("correct bearer key: expected 2xx, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// Correct X-API-Key header → 2xx.
	resp = do(t, http.MethodPost, srv.URL+"/api/v1/cb/primary/execute",
		`{"simulate_failure":false}`, map[string]string{
			"Content-Type": "application/json",
			"X-API-Key":    "topsecret",
		})
	if resp.StatusCode/100 != 2 {
		t.Fatalf("correct X-API-Key: expected 2xx, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestRouter_Auth_ReadOnlyRoutesUnaffected(t *testing.T) {
	srv := newTestServer(t, "topsecret")

	// Read-only routes stay open even with a key configured.
	for _, path := range []string{
		"/health/live",
		"/api/v1/cb/all",
		"/api/v1/cb/primary/snapshot",
		"/api/v1/limiters/token_bucket/state",
	} {
		resp := do(t, http.MethodGet, srv.URL+path, "", nil)
		if resp.StatusCode == http.StatusUnauthorized {
			t.Fatalf("read-only route %s returned 401; must be open", path)
		}
		resp.Body.Close()
	}
}

func TestRouter_Auth_DemoModeAllowsMutating(t *testing.T) {
	srv := newTestServer(t, "") // demo mode: no key

	resp := do(t, http.MethodPost, srv.URL+"/api/v1/cb/primary/execute",
		`{"simulate_failure":false}`, map[string]string{"Content-Type": "application/json"})
	if resp.StatusCode == http.StatusUnauthorized {
		t.Fatalf("demo mode must not require auth, got 401")
	}
	resp.Body.Close()
}

// ── C-2: /metrics gated ──────────────────────────────────────────────────────

func TestRouter_Metrics_RequiresKey(t *testing.T) {
	srv := newTestServer(t, "topsecret")

	resp := do(t, http.MethodGet, srv.URL+"/metrics", "", nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("/metrics without key: expected 401, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp = do(t, http.MethodGet, srv.URL+"/metrics", "",
		map[string]string{"X-API-Key": "topsecret"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/metrics with key: expected 200, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

// ── M-21: malformed JSON → 400 (through the full chain) ──────────────────────

func TestRouter_MalformedJSON_Returns400(t *testing.T) {
	srv := newTestServer(t, "") // demo mode so auth doesn't intercept

	cases := []struct {
		method, path, body string
	}{
		{http.MethodPost, "/api/v1/limiters/token_bucket/allow", `{bad json`},
		{http.MethodPost, "/api/v1/cb/primary/execute", `{bad json`},
		{http.MethodPost, "/api/v1/simulate", `{bad json`},
	}
	for _, tc := range cases {
		resp := do(t, tc.method, srv.URL+tc.path, tc.body,
			map[string]string{"Content-Type": "application/json"})
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s %s: expected 400 for malformed JSON, got %d", tc.method, tc.path, resp.StatusCode)
		}
		resp.Body.Close()
	}
}

// TestRouter_EmptyBody_UsesDefaults verifies M-21 doesn't break genuinely empty
// bodies where a default is intended.
func TestRouter_EmptyBody_UsesDefaults(t *testing.T) {
	srv := newTestServer(t, "")

	resp := do(t, http.MethodPost, srv.URL+"/api/v1/limiters/token_bucket/allow", "",
		map[string]string{"Content-Type": "application/json"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("empty allow body: expected 200 (defaults), got %d", resp.StatusCode)
	}
	resp.Body.Close()
}
