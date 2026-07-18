package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
)

// newFuzzHandlers builds a Handlers wired with a token_bucket limiter and a
// "primary" circuit breaker for use in fuzz targets. It registers cleanup on f.
func newFuzzHandlers(f *testing.F) *Handlers {
	f.Helper()
	limiters := map[string]ratelimit.Limiter{
		"token_bucket": tokenbucket.New(1_000_000, 1_000_000),
	}
	f.Cleanup(func() {
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

	hub := newHub(testLogger())
	go hub.Run()
	f.Cleanup(hub.Stop)

	var ready atomic.Bool
	ready.Store(true)
	return NewHandlers(limiters, cbs, registry, hub, testLogger(), &ready)
}

// ENHANCEMENTS §7.5 — Fuzz the server's JSON decoders & the simulator.
//
// These native Go fuzz targets feed arbitrary bytes to the server's request
// decoders (allow / execute / simulate) and assert two invariants:
//  1. no handler ever panics on adversarial input (the Recovery middleware is
//     bypassed here, so a panic fails the fuzz run directly), and
//  2. malformed JSON is surfaced as a 400 Bad Request, never a 5xx or 2xx.
//
// They run fast on the seed corpus via `go test -run=Fuzz` and can be driven
// longer with -fuzztime in CI/Makefile.

// seedJSONCorpus adds a realistic mix of valid, malformed and adversarial
// JSON bodies to a fuzz target.
func seedJSONCorpus(f *testing.F) {
	f.Helper()
	seeds := []string{
		``,
		`{}`,
		`   `,
		`null`,
		`[]`,
		`true`,
		`{"key":"user:1","n":1}`,
		`{"key":"user:1","n":-5}`,
		`{"key":"","n":0}`,
		`{"simulate_failure":true,"latency_ms":100}`,
		`{"latency_ms":-1}`,
		`{"latency_ms":99999999999999}`,
		`{"algorithm":"token_bucket","pattern":"burst","duration_ms":1000,` +
			`"requests_per_second":50,"concurrency":4,"key":"sim"}`,
		`{"duration_ms":-1,"requests_per_second":-1,"concurrency":-1}`,
		`{"requests_per_second":1e308,"concurrency":2147483647}`,
		`{"key":"` + strings.Repeat("x", 1000) + `"}`,
		"{\"key\":\"bad\x00null\"}", // embedded NUL in key
		`{"n":`,                     // truncated
		`{"n":1}{"n":2}`,            // trailing tokens
		`{"n":1.5}`,                 // wrong type for int
		`{"latency_ms":"x"}`,        // wrong type
		"\x00\x01\x02",              // binary garbage
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}
}

// callHandler invokes a handler with the given JSON body and path values,
// returning the recorded status. It fails the test if the handler panics.
func callHandler(
	t *testing.T,
	h *Handlers,
	fn http.HandlerFunc,
	path string,
	body []byte,
	pathValues map[string]string,
) int {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range pathValues {
		req.SetPathValue(k, v)
	}
	w := httptest.NewRecorder()
	fn(w, req) // a panic here fails the fuzz iteration directly
	return w.Code
}

// FuzzHandleAllow fuzzes the /allow JSON decoder.
func FuzzHandleAllow(f *testing.F) {
	seedJSONCorpus(f)
	h := newFuzzHandlers(f)
	f.Fuzz(func(t *testing.T, body []byte) {
		code := callHandler(t, h, h.HandleAllow, "/api/v1/limiters/token_bucket/allow",
			body, map[string]string{"algorithm": "token_bucket"})
		// Never a server error: decode/validation failures must be 4xx.
		if code >= 500 {
			t.Fatalf("HandleAllow returned %d for body %q", code, body)
		}
	})
}

// FuzzHandleCBExecute fuzzes the /execute JSON decoder.
func FuzzHandleCBExecute(f *testing.F) {
	seedJSONCorpus(f)
	h := newFuzzHandlers(f)
	f.Fuzz(func(t *testing.T, body []byte) {
		code := callHandler(t, h, h.HandleCBExecute, "/api/v1/cb/primary/execute",
			body, map[string]string{"name": "primary"})
		if code >= 500 && code != http.StatusServiceUnavailable {
			// 503 is a legitimate outcome (circuit open / bulkhead full).
			t.Fatalf("HandleCBExecute returned %d for body %q", code, body)
		}
	})
}

// FuzzHandleSimulate fuzzes the /simulate JSON decoder AND the simulator input
// validation (decodeSimulateRequest + clampSimulateRequest) without running the
// engine, so adversarial numeric params (huge N, negative/NaN/Inf rates) are
// exercised while runs stay fast. Invariants: no panic; on success the clamped
// bounds are always sane.
func FuzzHandleSimulate(f *testing.F) {
	seedJSONCorpus(f)
	f.Fuzz(func(t *testing.T, body []byte) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/simulate", strings.NewReader(string(body)))
		got, serr := decodeSimulateRequest(req) // a panic here fails the iteration
		if serr != nil {
			// Only 400s are produced by this path.
			if serr.status != http.StatusBadRequest {
				t.Fatalf("decodeSimulateRequest returned status %d, want 400, body %q", serr.status, body)
			}
			return
		}
		// On success, all bounds must be clamped into safe ranges.
		if got.DurationMs <= 0 || got.DurationMs > 60_000 {
			t.Fatalf("DurationMs out of bounds: %d (body %q)", got.DurationMs, body)
		}
		if got.RequestsPerSecond <= 0 || got.RequestsPerSecond > 10_000 {
			t.Fatalf("RequestsPerSecond out of bounds: %v (body %q)", got.RequestsPerSecond, body)
		}
		if got.Concurrency <= 0 || got.Concurrency > 500 {
			t.Fatalf("Concurrency out of bounds: %d (body %q)", got.Concurrency, body)
		}
		if err := validateKey(got.Key); err != nil {
			t.Fatalf("clamped key invalid: %v (body %q)", err, body)
		}
	})
}

// FuzzClampSimulateRequest fuzzes the numeric clamp directly with structured
// inputs (bypassing JSON) to hammer the boundary arithmetic, including NaN/Inf.
func FuzzClampSimulateRequest(f *testing.F) {
	f.Add(int64(1000), 50.0, 4, "sim")
	f.Add(int64(-1), -1.0, -1, "")
	f.Add(int64(1<<62), 1e308, 1<<30, "k")
	f.Fuzz(func(t *testing.T, durationMs int64, rps float64, conc int, key string) {
		req := simulateRequest{DurationMs: durationMs, RequestsPerSecond: rps, Concurrency: conc, Key: key}
		clampSimulateRequest(&req) // must not panic
		if req.DurationMs <= 0 || req.DurationMs > 60_000 {
			t.Fatalf("DurationMs out of bounds: %d", req.DurationMs)
		}
		if req.RequestsPerSecond <= 0 || req.RequestsPerSecond > 10_000 {
			t.Fatalf("RequestsPerSecond out of bounds: %v", req.RequestsPerSecond)
		}
		if req.Concurrency <= 0 || req.Concurrency > 500 {
			t.Fatalf("Concurrency out of bounds: %d", req.Concurrency)
		}
	})
}
