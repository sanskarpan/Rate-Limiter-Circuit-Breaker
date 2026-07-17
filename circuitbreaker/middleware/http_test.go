package middleware_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
	cbmiddleware "github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker/middleware"
)

// newCB creates a circuit breaker configured for easy tripping in tests.
func newCB(name string) *circuitbreaker.CircuitBreaker {
	return circuitbreaker.New(circuitbreaker.Config{
		Name:             name,
		WindowSize:       5,
		FailureThreshold: 3,
		OpenTimeout:      30 * time.Second,
	})
}

// --- Tests ---

func TestCircuitBreaker_ClosedPassesThrough(t *testing.T) {
	cb := newCB("test-closed")
	backend := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "hello")
	})

	handler := cbmiddleware.CircuitBreaker(cb)(backend)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
	if rec.Body.String() != "hello" {
		t.Errorf("expected body 'hello', got %q", rec.Body.String())
	}
}

func TestCircuitBreaker_5xxCountsAsFailure(t *testing.T) {
	cb := circuitbreaker.New(circuitbreaker.Config{
		Name:             "test-5xx",
		WindowSize:       10,
		FailureThreshold: 1, // open after just 1 failure
		OpenTimeout:      30 * time.Second,
	})
	backend := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})

	handler := cbmiddleware.CircuitBreaker(cb)(backend)

	// First request: 5xx — circuit records failure and may open.
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("first request: expected 500 pass-through, got %d", rec.Code)
	}

	// Circuit should now be open.
	if cb.State() != circuitbreaker.StateOpen {
		t.Errorf("circuit should be open after failure threshold, state=%s", cb.State())
	}
}

func TestCircuitBreaker_OpenReturns503(t *testing.T) {
	cb := circuitbreaker.New(circuitbreaker.Config{
		Name:             "test-open",
		WindowSize:       10,
		FailureThreshold: 1,
		OpenTimeout:      30 * time.Second,
	})
	backend := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})

	handler := cbmiddleware.CircuitBreaker(cb)(backend)

	// Trip the circuit.
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, httptest.NewRequest(http.MethodGet, "/", nil))
	// Now circuit is open.

	// Next request should get 503 from the middleware.
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, httptest.NewRequest(http.MethodGet, "/", nil))

	if rec2.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 when circuit open, got %d", rec2.Code)
	}
	if ct := rec2.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type: expected application/json, got %q", ct)
	}
}

func TestCircuitBreaker_OpenResponse_JSONBody(t *testing.T) {
	cb := circuitbreaker.New(circuitbreaker.Config{
		Name:             "test-open-json",
		WindowSize:       10,
		FailureThreshold: 1,
		OpenTimeout:      30 * time.Second,
	})
	backend := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})

	handler := cbmiddleware.CircuitBreaker(cb)(backend)

	// Trip the circuit.
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	// Open circuit request.
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, httptest.NewRequest(http.MethodGet, "/", nil))

	body := rec2.Body.String()
	if body == "" {
		t.Fatal("expected JSON body for open circuit response, got empty")
	}
	// Should contain circuit_open reason.
	if !contains(body, "circuit_open") {
		t.Errorf("expected body to contain 'circuit_open', got: %q", body)
	}
}

func TestCircuitBreaker_4xxDoesNotCountAsFailure(t *testing.T) {
	cb := circuitbreaker.New(circuitbreaker.Config{
		Name:             "test-4xx",
		WindowSize:       10,
		FailureThreshold: 1,
		OpenTimeout:      30 * time.Second,
	})
	backend := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest) // 4xx — not a CB failure
	})

	handler := cbmiddleware.CircuitBreaker(cb)(backend)

	for i := 0; i < 5; i++ {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		if rec.Code != http.StatusBadRequest {
			t.Errorf("request %d: expected 400 pass-through, got %d", i, rec.Code)
		}
	}

	// Circuit should still be closed since 4xx is not a failure.
	if cb.State() != circuitbreaker.StateClosed {
		t.Errorf("circuit should remain closed after 4xx responses, state=%s", cb.State())
	}
}

func TestCircuitBreaker_2xxDoesNotCountAsFailure(t *testing.T) {
	cb := circuitbreaker.New(circuitbreaker.Config{
		Name:             "test-2xx",
		WindowSize:       10,
		FailureThreshold: 1,
		OpenTimeout:      30 * time.Second,
	})
	backend := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated) // 201
	})

	handler := cbmiddleware.CircuitBreaker(cb)(backend)

	for i := 0; i < 5; i++ {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	}

	if cb.State() != circuitbreaker.StateClosed {
		t.Errorf("circuit should remain closed after 2xx responses, state=%s", cb.State())
	}
}

func TestCircuitBreaker_BackendWritesBodyOnError(t *testing.T) {
	// When backend writes a 5xx response, the body should be passed through.
	cb := circuitbreaker.New(circuitbreaker.Config{
		Name:             "test-body-passthrough",
		WindowSize:       10,
		FailureThreshold: 5, // high threshold so it doesn't open in this test
		OpenTimeout:      30 * time.Second,
	})
	backend := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, "internal server error detail")
	})

	handler := cbmiddleware.CircuitBreaker(cb)(backend)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
	if rec.Body.String() != "internal server error detail" {
		t.Errorf("expected backend body to be passed through, got %q", rec.Body.String())
	}
}

func TestCircuitBreaker_DefaultStatusCode200WhenNoWriteHeader(t *testing.T) {
	// Backend writes body without calling WriteHeader explicitly.
	cb := circuitbreaker.New(circuitbreaker.Config{
		Name:             "test-implicit-200",
		WindowSize:       10,
		FailureThreshold: 1,
		OpenTimeout:      30 * time.Second,
	})
	backend := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Write body without WriteHeader — Go net/http defaults to 200.
		fmt.Fprint(w, "implicit 200")
	})

	handler := cbmiddleware.CircuitBreaker(cb)(backend)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	// Circuit should remain closed since implicit 200 is a success.
	if cb.State() != circuitbreaker.StateClosed {
		t.Errorf("circuit should remain closed after implicit 200, state=%s", cb.State())
	}
	if rec.Body.String() != "implicit 200" {
		t.Errorf("expected 'implicit 200', got %q", rec.Body.String())
	}
}

func TestCircuitBreaker_MultipleMiddlewareChaining(t *testing.T) {
	cb1 := newCB("chain-cb1")
	cb2 := newCB("chain-cb2")
	backend := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "chained")
	})

	// Chain two circuit breakers.
	handler := cbmiddleware.CircuitBreaker(cb1)(cbmiddleware.CircuitBreaker(cb2)(backend))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 through chained CBs, got %d", rec.Code)
	}
	if rec.Body.String() != "chained" {
		t.Errorf("expected body 'chained', got %q", rec.Body.String())
	}
}

// contains is a simple substring check without importing strings in test file.
func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
