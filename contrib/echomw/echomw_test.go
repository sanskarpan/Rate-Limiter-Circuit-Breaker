package echomw_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v4"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/contrib/echomw"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
)

func TestRateLimit_AllowsThenDenies(t *testing.T) {
	lim := tokenbucket.New(2, 0.001)
	defer lim.Close()

	e := echo.New()
	e.Use(echomw.RateLimit(lim, echomw.WithKeyFunc(func(_ echo.Context) string { return "k" })))
	e.GET("/", func(c echo.Context) error { return c.String(http.StatusOK, "ok") })

	statuses := make([]int, 0, 4)
	var last *httptest.ResponseRecorder
	for i := 0; i < 4; i++ {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		e.ServeHTTP(w, req)
		statuses = append(statuses, w.Code)
		last = w
	}

	if statuses[0] != http.StatusOK || statuses[1] != http.StatusOK {
		t.Fatalf("want first two 200, got %v", statuses)
	}
	if statuses[2] != http.StatusTooManyRequests || statuses[3] != http.StatusTooManyRequests {
		t.Fatalf("want requests 3,4 = 429, got %v", statuses)
	}
	if last.Header().Get("X-RateLimit-Limit") == "" {
		t.Error("missing X-RateLimit-Limit header on 429")
	}
	if got := last.Header().Get("X-RateLimit-Remaining"); got != "0" {
		t.Errorf("X-RateLimit-Remaining = %q, want 0", got)
	}
}

func ExampleRateLimit() {
	lim := tokenbucket.New(100, 10)
	defer lim.Close()

	e := echo.New()
	e.Use(echomw.RateLimit(lim, echomw.WithKeyFunc(echomw.KeyByHeader("X-API-Key"))))
	e.GET("/api", func(c echo.Context) error { return c.String(http.StatusOK, "ok") })

	fmt.Println("mounted")
	// Output: mounted
}
