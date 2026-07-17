package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/internal/testutil"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
)

// newWSTestServer builds a router-backed WS server whose upgrader accepts the
// test server's own origin.
func newWSTestServer(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	limiters := map[string]ratelimit.Limiter{"token_bucket": tokenbucket.New(100, 100)}
	t.Cleanup(func() {
		for _, l := range limiters {
			l.Close() //nolint:errcheck
		}
	})
	registry := circuitbreaker.NewRegistry()
	registry.GetOrCreate("primary", circuitbreaker.Config{
		Name: "primary", WindowType: circuitbreaker.CountBased,
		WindowSize: 10, FailureThreshold: 5, OpenTimeout: time.Second,
	})
	cbs := map[string]*circuitbreaker.CircuitBreaker{"primary": registry.Get("primary")}

	var ready atomic.Bool
	ready.Store(true)

	srv := httptest.NewUnstartedServer(nil)
	srv.Start()
	// The allowed origin must equal the server's own URL so the upgrader accepts it.
	handler := NewRouter(limiters, cbs, registry, testLogger(), &ready, []string{srv.URL}, "")
	srv.Config.Handler = handler
	t.Cleanup(srv.Close)
	return srv, srv.URL
}

func wsURL(httpURL, path string) string {
	return "ws" + strings.TrimPrefix(httpURL, "http") + path
}

// ── H-17: no writePump goroutine leak on hub Stop ────────────────────────────

func TestHub_NoGoroutineLeakOnStop(t *testing.T) {
	lc := testutil.NewLeakChecker(t)
	defer lc.Check()

	hub := newHub(testLogger())
	go hub.Run()

	// Register several clients, each with a stub writePump that ranges over the
	// send channel (mirroring the real writePump's `for msg := range c.send`).
	// Before the H-17 fix, Stop() never closed c.send, so these goroutines would
	// block forever and the LeakChecker would flag them.
	const n = 6
	for i := 0; i < n; i++ {
		c := &Client{hub: hub, send: make(chan []byte, 8)}
		hub.register <- c
		go c.stubWritePump()
	}

	time.Sleep(30 * time.Millisecond) // let registrations + pumps settle
	hub.Stop()
	time.Sleep(50 * time.Millisecond) // allow pumps to observe closed channels
}

// stubWritePump mimics writePump's range-over-send loop without a real conn, so
// the leak test can assert that Stop() closes send and unblocks the loop.
func (c *Client) stubWritePump() {
	for range c.send {
	}
}

// ── H-19: Broadcast delivers events to subscribed clients ────────────────────

func TestHub_BroadcastDeliversEvent(t *testing.T) {
	_, base := newWSTestServer(t)

	dialer := websocket.Dialer{}
	hdr := http.Header{"Origin": {base}}
	conn, _, err := dialer.Dial(wsURL(base, "/ws/v1/events"), hdr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// First message is the "connected" welcome.
	var welcome Event
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if err := conn.ReadJSON(&welcome); err != nil {
		t.Fatalf("read welcome: %v", err)
	}
	if welcome.Type != "connected" {
		t.Fatalf("expected connected welcome, got %q", welcome.Type)
	}

	// Trigger activity: a rate-limit allow should broadcast a rate_limit_result.
	resp := do(t, http.MethodPost, base+"/api/v1/limiters/token_bucket/allow",
		`{"key":"ws-test"}`, map[string]string{"Content-Type": "application/json"})
	resp.Body.Close()

	var ev Event
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if err := conn.ReadJSON(&ev); err != nil {
		t.Fatalf("read broadcast event: %v", err)
	}
	if ev.Type != "rate_limit_result" {
		t.Fatalf("expected rate_limit_result event, got %q", ev.Type)
	}
	if ev.Name != "token_bucket" {
		t.Fatalf("expected name=token_bucket, got %q", ev.Name)
	}
	if ev.TS == 0 {
		t.Fatalf("expected non-zero ts")
	}
}

// TestHub_BroadcastEventEnvelope verifies the documented envelope directly.
func TestHub_BroadcastEventEnvelope(t *testing.T) {
	hub := newHub(testLogger())
	go hub.Run()
	defer hub.Stop()

	c := &Client{hub: hub, send: make(chan []byte, 4), filter: "primary"}
	hub.register <- c
	time.Sleep(10 * time.Millisecond)

	// The hub sends a "connected" welcome on register (F-3); drain it first.
	select {
	case msg := <-c.send:
		var ev Event
		if err := json.Unmarshal(msg, &ev); err != nil || ev.Type != "connected" {
			t.Fatalf("expected connected welcome first, got %s (err=%v)", msg, err)
		}
	case <-time.After(time.Second):
		t.Fatal("expected connected welcome on register")
	}

	// Matching name is delivered.
	hub.BroadcastEvent("cb_state_change", "primary", map[string]string{"state": "open"})
	select {
	case msg := <-c.send:
		var ev Event
		if err := json.Unmarshal(msg, &ev); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if ev.Type != "cb_state_change" || ev.Name != "primary" || ev.TS == 0 {
			t.Fatalf("bad envelope: %+v", ev)
		}
	case <-time.After(time.Second):
		t.Fatal("expected matching event to be delivered")
	}

	// Non-matching name is filtered out.
	hub.BroadcastEvent("cb_state_change", "secondary", nil)
	select {
	case <-c.send:
		t.Fatal("event for non-subscribed topic must be filtered")
	case <-time.After(100 * time.Millisecond):
		// expected
	}
}

// ── M-20: WS origin allow-list (CSWSH) ───────────────────────────────────────

func TestWS_RejectsDisallowedOrigin(t *testing.T) {
	_, base := newWSTestServer(t)

	dialer := websocket.Dialer{}
	hdr := http.Header{"Origin": {"http://evil.example.com"}}
	_, resp, err := dialer.Dial(wsURL(base, "/ws/v1/events"), hdr)
	if err == nil {
		t.Fatal("expected upgrade to be rejected for disallowed origin")
	}
	if resp == nil || resp.StatusCode != http.StatusForbidden {
		got := 0
		if resp != nil {
			got = resp.StatusCode
		}
		t.Fatalf("expected 403 for disallowed origin, got %d", got)
	}
}

func TestWS_RejectsEmptyOrigin(t *testing.T) {
	_, base := newWSTestServer(t)

	dialer := websocket.Dialer{}
	// No Origin header at all.
	_, resp, err := dialer.Dial(wsURL(base, "/ws/v1/events"), http.Header{})
	if err == nil {
		t.Fatal("expected upgrade to be rejected for empty origin")
	}
	if resp == nil || resp.StatusCode != http.StatusForbidden {
		got := 0
		if resp != nil {
			got = resp.StatusCode
		}
		t.Fatalf("expected 403 for empty origin, got %d", got)
	}
}

// ── L-12: WS path param validation ───────────────────────────────────────────

func TestWS_UnknownAlgorithmParam_NoUpgrade(t *testing.T) {
	_, base := newWSTestServer(t)

	dialer := websocket.Dialer{}
	hdr := http.Header{"Origin": {base}}
	_, resp, err := dialer.Dial(wsURL(base, "/ws/v1/limiters/nonexistent"), hdr)
	if err == nil {
		t.Fatal("expected upgrade to fail for unknown algorithm")
	}
	if resp == nil || resp.StatusCode != http.StatusNotFound {
		got := 0
		if resp != nil {
			got = resp.StatusCode
		}
		t.Fatalf("expected 404 for unknown algorithm param, got %d", got)
	}
}

func TestWS_OversizedCBParam_NoUpgrade(t *testing.T) {
	_, base := newWSTestServer(t)

	dialer := websocket.Dialer{}
	hdr := http.Header{"Origin": {base}}
	big := strings.Repeat("a", 600) // > maxKeyLen (512)
	_, resp, err := dialer.Dial(wsURL(base, "/ws/v1/cb/"+big), hdr)
	if err == nil {
		t.Fatal("expected upgrade to fail for oversized cb param")
	}
	if resp == nil || resp.StatusCode != http.StatusBadRequest {
		got := 0
		if resp != nil {
			got = resp.StatusCode
		}
		t.Fatalf("expected 400 for oversized cb param, got %d", got)
	}
}
