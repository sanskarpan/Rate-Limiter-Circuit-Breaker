package middleware_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/middleware"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
)

func ExampleRateLimit() {
	// Create a token bucket: capacity 100, refill 10 tokens/second.
	limiter := tokenbucket.New(100, 10)
	defer limiter.Close()

	// Wrap your handler with the rate limit middleware.
	mux := http.NewServeMux()
	mux.HandleFunc("/api/data", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, `{"data":"hello"}`)
	})

	// Apply rate limiting keyed by client IP.
	handler := middleware.RateLimit(limiter)(mux)

	// Simulate a request.
	req := httptest.NewRequest(http.MethodGet, "/api/data", nil)
	req.RemoteAddr = "192.0.2.1:4321"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	fmt.Println(rec.Code)
	// Output:
	// 200
}

func ExampleRateLimit_keyByHeader() {
	limiter := tokenbucket.New(10, 1)
	defer limiter.Close()

	handler := middleware.RateLimit(
		limiter,
		// Rate limit by API key header instead of IP.
		middleware.WithKeyFunc(middleware.KeyByHeader("X-API-Key")),
	)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-API-Key", "token-abc")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	fmt.Println(rec.Code)
	// Output:
	// 200
}

func ExampleRateLimit_skipHealthCheck() {
	limiter := tokenbucket.New(10, 1)
	defer limiter.Close()

	handler := middleware.RateLimit(
		limiter,
		// Skip rate limiting for health check endpoints.
		middleware.WithSkipFunc(func(r *http.Request) bool {
			return r.URL.Path == "/healthz"
		}),
	)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	fmt.Println(rec.Code)
	// Output:
	// 200
}

func ExampleRateLimit_customLimitedHandler() {
	// Limiter that always denies (0 capacity for demo).
	limiter := &mockLimiter{result: ratelimit.Result{
		Allowed:   false,
		Limit:     0,
		Remaining: 0,
	}}

	handler := middleware.RateLimit(
		limiter,
		middleware.WithOnLimited(func(w http.ResponseWriter, r *http.Request, result ratelimit.Result) {
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusTooManyRequests)
			fmt.Fprintf(w, "slow down! limit=%d\n", result.Limit)
		}),
	)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	fmt.Println(rec.Code)
	// Output:
	// 429
}

func ExampleKeyByIP() {
	fn := middleware.KeyByIP()

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Forwarded-For", "10.0.0.1, 10.0.0.2")

	key := fn(req)
	fmt.Println(key)
	// Output:
	// 10.0.0.1
}

func ExampleKeyByHeader() {
	fn := middleware.KeyByHeader("X-API-Key")

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-API-Key", "my-secret-token")

	key := fn(req)
	fmt.Println(key)
	// Output:
	// my-secret-token
}

func ExampleKeyByParam() {
	fn := middleware.KeyByParam("tenant")

	req := httptest.NewRequest(http.MethodGet, "/?tenant=acme", nil)

	key := fn(req)
	fmt.Println(key)
	// Output:
	// acme
}
