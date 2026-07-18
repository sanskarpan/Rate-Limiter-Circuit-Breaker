package echomw_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/labstack/echo/v4"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/contrib/echomw"
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
	cb := newTestBreaker("echo-cb")

	var handlerCalls int
	e := echo.New()
	e.Use(echomw.CircuitBreaker(cb))
	e.GET("/", func(c echo.Context) error {
		handlerCalls++
		return c.String(http.StatusInternalServerError, "boom")
	})

	for i := 0; i < 2; i++ {
		w := httptest.NewRecorder()
		e.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
		if w.Code != http.StatusInternalServerError {
			t.Fatalf("request %d: want 500, got %d", i, w.Code)
		}
	}
	if cb.State() != circuitbreaker.StateOpen {
		t.Fatalf("breaker state = %v, want open", cb.State())
	}
	if handlerCalls != 2 {
		t.Fatalf("handlerCalls = %d, want 2", handlerCalls)
	}

	w := httptest.NewRecorder()
	e.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("open-circuit status = %d, want 503", w.Code)
	}
	if handlerCalls != 2 {
		t.Fatalf("handler ran while circuit open: handlerCalls = %d, want 2", handlerCalls)
	}
}

func TestCircuitBreaker_HandlerErrorTripsAndPropagates(t *testing.T) {
	cb := newTestBreaker("echo-cb-err")

	e := echo.New()
	e.Use(echomw.CircuitBreaker(cb))
	e.GET("/", func(c echo.Context) error {
		return echo.NewHTTPError(http.StatusInternalServerError, "boom")
	})

	// Two handler errors trip the breaker; echo's error handler still renders 500.
	for i := 0; i < 2; i++ {
		w := httptest.NewRecorder()
		e.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
		if w.Code != http.StatusInternalServerError {
			t.Fatalf("request %d: want 500, got %d", i, w.Code)
		}
	}
	if cb.State() != circuitbreaker.StateOpen {
		t.Fatalf("breaker state = %v, want open", cb.State())
	}

	w := httptest.NewRecorder()
	e.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("open-circuit status = %d, want 503", w.Code)
	}
}

func TestCircuitBreaker_SuccessPassesThrough(t *testing.T) {
	cb := newTestBreaker("echo-cb-ok")

	e := echo.New()
	e.Use(echomw.CircuitBreaker(cb))
	e.GET("/", func(c echo.Context) error { return c.String(http.StatusOK, "ok") })

	for i := 0; i < 5; i++ {
		w := httptest.NewRecorder()
		e.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("request %d: want 200, got %d", i, w.Code)
		}
	}
	if cb.State() != circuitbreaker.StateClosed {
		t.Fatalf("breaker state = %v, want closed", cb.State())
	}
}

func ExampleCircuitBreaker() {
	cb := circuitbreaker.New(circuitbreaker.Config{Name: "echo"})

	e := echo.New()
	e.Use(echomw.CircuitBreaker(cb))
	e.GET("/api", func(c echo.Context) error { return c.String(http.StatusOK, "ok") })

	fmt.Println("mounted")
	// Output: mounted
}
