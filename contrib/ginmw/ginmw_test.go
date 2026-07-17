package ginmw_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/contrib/ginmw"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
)

func TestRateLimit_AllowsThenDenies(t *testing.T) {
	gin.SetMode(gin.TestMode)
	lim := tokenbucket.New(2, 0.001)
	defer lim.Close()

	router := gin.New()
	router.Use(ginmw.RateLimit(lim, ginmw.WithKeyFunc(func(_ *gin.Context) string { return "k" })))
	router.GET("/", func(c *gin.Context) { c.String(http.StatusOK, "ok") })

	statuses := make([]int, 0, 4)
	var last *httptest.ResponseRecorder
	for i := 0; i < 4; i++ {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		router.ServeHTTP(w, req)
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
	gin.SetMode(gin.ReleaseMode)
	lim := tokenbucket.New(100, 10)
	defer lim.Close()

	router := gin.New()
	router.Use(ginmw.RateLimit(lim, ginmw.WithKeyFunc(ginmw.KeyByHeader("X-API-Key"))))
	router.GET("/api", func(c *gin.Context) { c.String(http.StatusOK, "ok") })

	fmt.Println("mounted")
	// Output: mounted
}
