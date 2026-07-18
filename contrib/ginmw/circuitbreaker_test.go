package ginmw_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/contrib/ginmw"
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
	gin.SetMode(gin.TestMode)
	cb := newTestBreaker("gin-cb")

	var handlerCalls int
	router := gin.New()
	router.Use(ginmw.CircuitBreaker(cb))
	router.GET("/", func(c *gin.Context) {
		handlerCalls++
		c.String(http.StatusInternalServerError, "boom")
	})

	// Two 5xx responses trip the breaker (FailureThreshold=2).
	for i := 0; i < 2; i++ {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
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

	// Next request must short-circuit: 503 and the handler is NOT invoked.
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("open-circuit status = %d, want 503", w.Code)
	}
	if handlerCalls != 2 {
		t.Fatalf("handler ran while circuit open: handlerCalls = %d, want 2", handlerCalls)
	}
}

func TestCircuitBreaker_SuccessPassesThrough(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cb := newTestBreaker("gin-cb-ok")

	router := gin.New()
	router.Use(ginmw.CircuitBreaker(cb))
	router.GET("/", func(c *gin.Context) { c.String(http.StatusOK, "ok") })

	for i := 0; i < 5; i++ {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("request %d: want 200, got %d", i, w.Code)
		}
	}
	if cb.State() != circuitbreaker.StateClosed {
		t.Fatalf("breaker state = %v, want closed", cb.State())
	}
}

func TestCircuitBreaker_SkipFunc(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cb := newTestBreaker("gin-cb-skip")

	router := gin.New()
	router.Use(ginmw.CircuitBreaker(cb, ginmw.WithCBSkipFunc(func(c *gin.Context) bool {
		return c.GetHeader("X-Skip") == "1"
	})))
	router.GET("/", func(c *gin.Context) { c.String(http.StatusInternalServerError, "boom") })

	// Skipped requests never touch the breaker, so it stays closed.
	for i := 0; i < 5; i++ {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("X-Skip", "1")
		router.ServeHTTP(w, req)
	}
	if cb.State() != circuitbreaker.StateClosed {
		t.Fatalf("breaker state = %v, want closed (all requests skipped)", cb.State())
	}
}

func ExampleCircuitBreaker() {
	gin.SetMode(gin.ReleaseMode)
	cb := circuitbreaker.New(circuitbreaker.Config{Name: "gin"})

	router := gin.New()
	router.Use(ginmw.CircuitBreaker(cb))
	router.GET("/api", func(c *gin.Context) { c.String(http.StatusOK, "ok") })

	fmt.Println("mounted")
	// Output: mounted
}
