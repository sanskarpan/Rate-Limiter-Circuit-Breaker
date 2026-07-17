package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/sanskarpan/resilience/circuitbreaker"
	"github.com/sanskarpan/resilience/ratelimit"
	"github.com/sanskarpan/resilience/ratelimit/tokenbucket"
)

// ── M-18: force-half-open must not lie ───────────────────────────────────────

// TestForceHalfOpen_DoesNotLie verifies that force-half-open returns an honest
// 501 rather than reporting a state it didn't set (previously it opened the
// breaker and reported OPEN).
func TestForceHalfOpen_HonestResponse(t *testing.T) {
	h := testHandlers(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/cb/primary/force-half-open", nil)
	req.SetPathValue("name", "primary")
	w := httptest.NewRecorder()

	// Capture the state before the call.
	before := h.registry.Get("primary").State()

	h.HandleCBForceHalfOpen(w, req)

	if w.Code != http.StatusNotImplemented {
		t.Fatalf("expected 501 Not Implemented, got %d; body=%s", w.Code, w.Body.String())
	}

	// The breaker must not have been mutated (no lie, no side effect).
	after := h.registry.Get("primary").State()
	if before != after {
		t.Fatalf("force-half-open must not mutate state: before=%s after=%s", before, after)
	}
}

// ── M-19: global simulation cap ──────────────────────────────────────────────

// TestSimulate_ConcurrentCapRejects verifies that concurrent /simulate calls
// beyond maxConcurrentSimulations are rejected with 429.
func TestSimulate_ConcurrentCapRejects(t *testing.T) {
	limiters := map[string]ratelimit.Limiter{"token_bucket": tokenbucket.New(100, 100)}
	t.Cleanup(func() { limiters["token_bucket"].Close() }) //nolint:errcheck

	registry := circuitbreaker.NewRegistry()
	hub := newHub(testLogger())
	go hub.Run()
	t.Cleanup(hub.Stop)
	var ready atomic.Bool
	ready.Store(true)
	h := NewHandlers(limiters, nil, registry, hub, testLogger(), &ready)

	// Fill every simulation slot with long-running simulations so the cap is
	// exhausted, then confirm an additional call is rejected with 429.
	body := `{"algorithm":"token_bucket","duration_ms":1000,"requests_per_second":5,"concurrency":1}`

	var wg sync.WaitGroup
	var rejected atomic.Int64
	var accepted atomic.Int64

	total := maxConcurrentSimulations + 8
	start := make(chan struct{})
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			req := httptest.NewRequest(http.MethodPost, "/api/v1/simulate", strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			h.HandleSimulate(w, req)
			switch w.Code {
			case http.StatusTooManyRequests:
				rejected.Add(1)
			case http.StatusOK:
				accepted.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	if rejected.Load() == 0 {
		t.Fatalf("expected some simulations to be rejected with 429 (cap=%d, total=%d); accepted=%d",
			maxConcurrentSimulations, total, accepted.Load())
	}
	if accepted.Load() > int64(maxConcurrentSimulations) {
		t.Fatalf("more simulations accepted (%d) than the cap allows (%d)",
			accepted.Load(), maxConcurrentSimulations)
	}
}

// ── M-21: force-open still works and broadcasts (sanity) ─────────────────────

func TestForceOpen_TransitionsOpen(t *testing.T) {
	h := testHandlers(t)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/cb/primary/force-open", nil)
	req.SetPathValue("name", "primary")
	w := httptest.NewRecorder()

	h.HandleCBForceOpen(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp cbResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.State != circuitbreaker.StateOpen.String() {
		t.Fatalf("expected state OPEN, got %s", resp.State)
	}
	// The reported state must match the actual breaker state.
	if h.registry.Get("primary").State() != circuitbreaker.StateOpen {
		t.Fatalf("breaker state does not match reported OPEN")
	}
}
