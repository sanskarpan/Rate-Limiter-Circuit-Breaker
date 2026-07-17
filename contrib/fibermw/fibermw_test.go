package fibermw_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/contrib/fibermw"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
)

func TestRateLimit_AllowsThenDenies(t *testing.T) {
	lim := tokenbucket.New(2, 0.001)
	defer lim.Close()

	app := fiber.New()
	app.Use(fibermw.RateLimit(lim, fibermw.WithKeyFunc(func(_ *fiber.Ctx) string { return "k" })))
	app.Get("/", func(c *fiber.Ctx) error { return c.SendString("ok") })

	statuses := make([]int, 0, 4)
	var lastResetHeader, lastRemaining string
	for i := 0; i < 4; i++ {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		statuses = append(statuses, resp.StatusCode)
		if i == 3 {
			lastResetHeader = resp.Header.Get("X-RateLimit-Limit")
			lastRemaining = resp.Header.Get("X-RateLimit-Remaining")
		}
		resp.Body.Close()
	}

	if statuses[0] != http.StatusOK || statuses[1] != http.StatusOK {
		t.Fatalf("want first two 200, got %v", statuses)
	}
	if statuses[2] != http.StatusTooManyRequests || statuses[3] != http.StatusTooManyRequests {
		t.Fatalf("want requests 3,4 = 429, got %v", statuses)
	}
	if lastResetHeader == "" {
		t.Error("missing X-RateLimit-Limit header on 429")
	}
	if lastRemaining != "0" {
		t.Errorf("X-RateLimit-Remaining = %q, want 0", lastRemaining)
	}
}

func ExampleRateLimit() {
	lim := tokenbucket.New(100, 10)
	defer lim.Close()

	app := fiber.New()
	app.Use(fibermw.RateLimit(lim, fibermw.WithKeyFunc(fibermw.KeyByHeader("X-API-Key"))))
	app.Get("/api", func(c *fiber.Ctx) error { return c.SendString("ok") })

	fmt.Println("mounted")
	// Output: mounted
}
