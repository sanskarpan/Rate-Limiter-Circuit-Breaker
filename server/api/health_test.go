package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestHealthLive(t *testing.T) {
	var ready atomic.Bool
	h := newHealthHandler(&ready)

	req := httptest.NewRequest(http.MethodGet, "/health/live", nil)
	w := httptest.NewRecorder()

	h.Live(w, req)

	res := w.Result()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.StatusCode)
	}

	var body map[string]string
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("expected status=ok, got %q", body["status"])
	}
}

func TestHealthReady_NotReady(t *testing.T) {
	var ready atomic.Bool // false by default
	h := newHealthHandler(&ready)

	req := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	w := httptest.NewRecorder()

	h.Ready(w, req)

	res := w.Result()
	if res.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", res.StatusCode)
	}

	var body map[string]string
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if body["status"] != "not_ready" {
		t.Errorf("expected status=not_ready, got %q", body["status"])
	}
}

func TestHealthReady_Ready(t *testing.T) {
	var ready atomic.Bool
	ready.Store(true)
	h := newHealthHandler(&ready)

	req := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	w := httptest.NewRecorder()

	h.Ready(w, req)

	res := w.Result()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.StatusCode)
	}

	var body map[string]string
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if body["status"] != "ready" {
		t.Errorf("expected status=ready, got %q", body["status"])
	}
}
