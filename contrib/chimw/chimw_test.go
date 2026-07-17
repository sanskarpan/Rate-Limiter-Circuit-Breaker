package chimw_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/contrib/chimw"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
)

// newLimiter returns a token bucket with capacity 2 and a slow refill so the
// third request within the test window is denied.
func newLimiter() *tokenbucket.TokenBucket {
	// capacity 2, refill 0.001 tokens/sec (effectively no refill during the test).
	return tokenbucket.New(2, 0.001)
}

func TestRateLimit_AllowsThenDenies(t *testing.T) {
	lim := newLimiter()
	defer lim.Close()

	r := chi.NewRouter()
	r.Use(chimw.RateLimit(lim, chimw.WithKeyFunc(func(_ *http.Request) string { return "k" })))
	r.Get("/", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	srv := httptest.NewServer(r)
	defer srv.Close()

	statuses := make([]int, 0, 4)
	var last *http.Response
	for i := 0; i < 4; i++ {
		resp, err := http.Get(srv.URL + "/")
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		statuses = append(statuses, resp.StatusCode)
		if i == 3 {
			last = resp
		} else {
			resp.Body.Close()
		}
	}

	if statuses[0] != http.StatusOK || statuses[1] != http.StatusOK {
		t.Fatalf("want first two 200, got %v", statuses)
	}
	if statuses[2] != http.StatusTooManyRequests || statuses[3] != http.StatusTooManyRequests {
		t.Fatalf("want requests 3,4 = 429, got %v", statuses)
	}

	defer last.Body.Close()
	if got := last.Header.Get("X-RateLimit-Limit"); got == "" {
		t.Error("missing X-RateLimit-Limit header on 429")
	}
	if got := last.Header.Get("X-RateLimit-Remaining"); got != "0" {
		t.Errorf("X-RateLimit-Remaining = %q, want 0", got)
	}
}

func ExampleRateLimit() {
	lim := tokenbucket.New(100, 10)
	defer lim.Close()

	r := chi.NewRouter()
	r.Use(chimw.RateLimit(lim, chimw.WithKeyFunc(chimw.KeyByHeader("X-API-Key"))))
	r.Get("/api", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, "ok")
	})

	// The core middleware already returns func(http.Handler) http.Handler, so
	// chi mounts it directly via r.Use.
	fmt.Println("mounted")
	// Output: mounted
}
