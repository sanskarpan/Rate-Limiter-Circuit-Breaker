package fibermw_test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/contrib/fibermw"
)

func newTestBreaker(name string) *circuitbreaker.CircuitBreaker {
	return circuitbreaker.New(circuitbreaker.Config{
		Name:             name,
		FailureThreshold: 2,
		MinimumRequests:  1,
		OpenTimeout:      time.Minute,
	})
}

func doReq(t *testing.T, app *fiber.App) int {
	t.Helper()
	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/", nil))
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp.StatusCode
}

func TestCircuitBreaker_OpensAndShortCircuits(t *testing.T) {
	cb := newTestBreaker("fiber-cb")

	var handlerCalls int
	app := fiber.New()
	app.Use(fibermw.CircuitBreaker(cb))
	app.Get("/", func(c *fiber.Ctx) error {
		handlerCalls++
		return c.Status(http.StatusInternalServerError).SendString("boom")
	})

	for i := 0; i < 2; i++ {
		if code := doReq(t, app); code != http.StatusInternalServerError {
			t.Fatalf("request %d: want 500, got %d", i, code)
		}
	}
	if cb.State() != circuitbreaker.StateOpen {
		t.Fatalf("breaker state = %v, want open", cb.State())
	}
	if handlerCalls != 2 {
		t.Fatalf("handlerCalls = %d, want 2", handlerCalls)
	}

	if code := doReq(t, app); code != http.StatusServiceUnavailable {
		t.Fatalf("open-circuit status = %d, want 503", code)
	}
	if handlerCalls != 2 {
		t.Fatalf("handler ran while circuit open: handlerCalls = %d, want 2", handlerCalls)
	}
}

func TestCircuitBreaker_SuccessPassesThrough(t *testing.T) {
	cb := newTestBreaker("fiber-cb-ok")

	app := fiber.New()
	app.Use(fibermw.CircuitBreaker(cb))
	app.Get("/", func(c *fiber.Ctx) error { return c.SendString("ok") })

	for i := 0; i < 5; i++ {
		if code := doReq(t, app); code != http.StatusOK {
			t.Fatalf("request %d: want 200, got %d", i, code)
		}
	}
	if cb.State() != circuitbreaker.StateClosed {
		t.Fatalf("breaker state = %v, want closed", cb.State())
	}
}

func ExampleCircuitBreaker() {
	cb := circuitbreaker.New(circuitbreaker.Config{Name: "fiber"})

	app := fiber.New()
	app.Use(fibermw.CircuitBreaker(cb))
	app.Get("/api", func(c *fiber.Ctx) error { return c.SendString("ok") })

	fmt.Println("mounted")
	// Output: mounted
}
