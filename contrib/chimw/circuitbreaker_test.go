package chimw_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/contrib/chimw"
)

func newTestBreaker(name string) *circuitbreaker.CircuitBreaker {
	return circuitbreaker.New(circuitbreaker.Config{
		Name:             name,
		FailureThreshold: 2,
		MinimumRequests:  1,
		OpenTimeout:      time.Minute,
	})
}

func TestCircuitBreaker_OpensAndShortCircuits(t *testing.T) {
	cb := newTestBreaker("chi-cb")

	var handlerCalls int
	r := chi.NewRouter()
	r.Use(chimw.CircuitBreaker(cb))
	r.Get("/", func(w http.ResponseWriter, _ *http.Request) {
		handlerCalls++
		w.WriteHeader(http.StatusInternalServerError)
	})

	for i := 0; i < 2; i++ {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
		if w.Code != http.StatusInternalServerError {
			t.Fatalf("request %d: want 500, got %d", i, w.Code)
		}
	}
	if cb.State() != circuitbreaker.StateOpen {
		t.Fatalf("breaker state = %v, want open", cb.State())
	}

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("open-circuit status = %d, want 503", w.Code)
	}
	if handlerCalls != 2 {
		t.Fatalf("handler ran while circuit open: handlerCalls = %d, want 2", handlerCalls)
	}
}

func TestCircuitBreaker_SuccessPassesThrough(t *testing.T) {
	cb := newTestBreaker("chi-cb-ok")

	r := chi.NewRouter()
	r.Use(chimw.CircuitBreaker(cb))
	r.Get("/", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})

	for i := 0; i < 5; i++ {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("request %d: want 200, got %d", i, w.Code)
		}
	}
	if cb.State() != circuitbreaker.StateClosed {
		t.Fatalf("breaker state = %v, want closed", cb.State())
	}
}

func ExampleCircuitBreaker() {
	cb := circuitbreaker.New(circuitbreaker.Config{Name: "chi"})

	r := chi.NewRouter()
	r.Use(chimw.CircuitBreaker(cb))
	r.Get("/api", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })

	fmt.Println("mounted")
	// Output: mounted
}
