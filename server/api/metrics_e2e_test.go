package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
	metricprom "github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/metric/prometheus"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
)

// TestMetricsEndpoint_ExposesRealSeries is an integration-style test that
// assembles the real router with limiters + circuit breakers wired to the
// Prometheus adapter over the DEFAULT registry (the one /metrics serves),
// drives allow + deny + CB execute traffic, then scrapes /metrics and asserts
// the new resilience_* series appear with non-zero values. No network/Redis.
func TestMetricsEndpoint_ExposesRealSeries(t *testing.T) {
	// The demo /metrics handler serves the default registry, so the recorder
	// must register there. New() tolerates duplicate registration across tests.
	rec := metricprom.New(prometheus.DefaultRegisterer)

	// capacity 1, refill 1/s → first Allow succeeds, subsequent ones deny.
	tb := tokenbucket.New(1, 1, tokenbucket.WithRecorder(rec))
	t.Cleanup(func() { _ = tb.Close() })
	limiters := map[string]ratelimit.Limiter{"token_bucket": tb}

	registry := circuitbreaker.NewRegistry()
	registry.GetOrCreate("primary", circuitbreaker.Config{
		Name:             "primary",
		WindowType:       circuitbreaker.CountBased,
		WindowSize:       10,
		FailureThreshold: 5,
		OpenTimeout:      time.Second,
		Recorder:         rec,
	})
	cbs := map[string]*circuitbreaker.CircuitBreaker{"primary": registry.Get("primary")}

	var ready atomic.Bool
	ready.Store(true)

	handler := NewRouter(limiters, cbs, registry, testLogger(), &ready,
		[]string{"http://localhost:3000"}, "") // demo mode: no auth
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	// Drive one allowed (200) + several denied (429) rate-limit decisions.
	post(t, srv.URL+"/api/v1/limiters/token_bucket/allow", `{"key":"e2e","n":1}`, http.StatusOK)
	for i := 0; i < 3; i++ {
		post(t, srv.URL+"/api/v1/limiters/token_bucket/allow", `{"key":"e2e","n":1}`, http.StatusTooManyRequests)
	}

	// Drive a CB success and a CB failure (the handler returns 200 with an
	// "executed" flag in both cases).
	post(t, srv.URL+"/api/v1/cb/primary/execute", `{"simulate_failure":false}`, http.StatusOK)
	post(t, srv.URL+"/api/v1/cb/primary/execute", `{"simulate_failure":true}`, http.StatusOK)

	// Scrape /metrics.
	resp := do(t, http.MethodGet, srv.URL+"/metrics", "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/metrics status = %d, want 200", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("read /metrics body: %v", err)
	}
	metrics := string(body)

	// Assert the new series appear with the expected labels. These substrings
	// must be present in the Prometheus text exposition.
	wantSubstrings := []string{
		`resilience_ratelimit_requests_total{algorithm="token_bucket",result="allowed"}`,
		`resilience_ratelimit_requests_total{algorithm="token_bucket",result="denied"}`,
		`resilience_ratelimit_decision_duration_seconds_bucket{algorithm="token_bucket"`,
		`resilience_circuitbreaker_state{name="primary"}`,
		`resilience_circuitbreaker_requests_total{name="primary",result="success"}`,
		`resilience_circuitbreaker_requests_total{name="primary",result="failure"}`,
		`resilience_circuitbreaker_execution_duration_seconds_bucket{name="primary"`,
		// HTTP middleware metrics must still be present.
		`resilience_http_requests_total{`,
	}
	for _, want := range wantSubstrings {
		if !strings.Contains(metrics, want) {
			t.Errorf("/metrics output missing series: %s", want)
		}
	}

	// Assert the allowed counter is non-zero (value column after the label set).
	if !hasNonZeroSample(metrics,
		`resilience_ratelimit_requests_total{algorithm="token_bucket",result="allowed"}`) {
		t.Error("allowed rate-limit counter is zero; expected >0")
	}
	if !hasNonZeroSample(metrics,
		`resilience_ratelimit_requests_total{algorithm="token_bucket",result="denied"}`) {
		t.Error("denied rate-limit counter is zero; expected >0")
	}
	if !hasNonZeroSample(metrics,
		`resilience_circuitbreaker_requests_total{name="primary",result="success"}`) {
		t.Error("CB success counter is zero; expected >0")
	}
}

func post(t *testing.T, url, body string, wantStatus int) {
	t.Helper()
	resp := do(t, http.MethodPost, url, body, map[string]string{"Content-Type": "application/json"})
	if resp.StatusCode != wantStatus {
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("POST %s = %d (want %d): %s", url, resp.StatusCode, wantStatus, string(b))
	}
	resp.Body.Close()
}

// hasNonZeroSample finds the exposition line beginning with prefix and checks
// its trailing value is > 0.
func hasNonZeroSample(exposition, prefix string) bool {
	for _, line := range strings.Split(exposition, "\n") {
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		if fields[len(fields)-1] != "0" {
			return true
		}
	}
	return false
}
