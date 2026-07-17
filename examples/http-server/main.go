// Package main demonstrates how to use the resilience library with an HTTP server.
// It shows rate limiting, circuit breaking, and middleware integration.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/sanskarpan/resilience/circuitbreaker"
	cbmw "github.com/sanskarpan/resilience/circuitbreaker/middleware"
	"github.com/sanskarpan/resilience/internal/clock"
	ratelimitmw "github.com/sanskarpan/resilience/ratelimit/middleware"
	"github.com/sanskarpan/resilience/ratelimit/tokenbucket"
)

func main() {
	// 1. Create a token bucket rate limiter: 10 requests/second, burst of 20.
	limiter := tokenbucket.New(10, 20, tokenbucket.WithClock(clock.RealClock{}))
	defer limiter.Close()

	// 2. Create a circuit breaker with count-based window.
	cb := circuitbreaker.New(circuitbreaker.Config{
		Name:             "backend",
		WindowType:       circuitbreaker.CountBased,
		WindowSize:       10,
		FailureThreshold: 5,
		OpenTimeout:      10 * time.Second,
		Clock:            clock.RealClock{},
	})

	// 3. Build handler chain: rate limit → CB → actual handler
	mux := http.NewServeMux()

	apiHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Wrap downstream call in circuit breaker
		err := cb.Execute(r.Context(), func(ctx context.Context) error {
			// Simulate backend call
			if r.URL.Query().Get("fail") == "true" {
				return errors.New("backend error")
			}
			fmt.Fprintf(w, `{"status":"ok","path":"%s"}`, r.URL.Path)
			return nil
		})
		if err != nil {
			if errors.Is(err, circuitbreaker.ErrCircuitOpen) {
				http.Error(w, `{"error":"service_unavailable"}`, http.StatusServiceUnavailable)
				return
			}
			http.Error(w, `{"error":"backend_error"}`, http.StatusInternalServerError)
		}
	})

	// Apply rate limiting middleware (key by IP)
	rateLimitedHandler := ratelimitmw.RateLimit(
		limiter,
		ratelimitmw.WithKeyFunc(ratelimitmw.KeyByIP()),
	)(apiHandler)

	mux.Handle("/api/", rateLimitedHandler)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"status":"ok"}`)
	})

	// 4. Apply CB HTTP middleware to the entire mux (for 5xx tracking)
	handler := cbmw.CircuitBreaker(cb)(mux)

	log.Println("Server running on :8080")
	log.Println("Try: curl http://localhost:8080/api/test")
	log.Println("     curl http://localhost:8080/api/test?fail=true  (to trip CB)")
	if err := http.ListenAndServe(":8080", handler); err != nil {
		log.Fatal(err)
	}
}
