// Package main is the entrypoint for the resilience demo HTTP server.
// It wires together all rate limiter algorithms and circuit breakers and
// serves the REST + WebSocket API defined in server/api.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
	metricprom "github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/metric/prometheus"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/observability/otel"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/fixedwindow"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/gcra"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/leakybucket"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/slidingwindow"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/ratelimit/tokenbucket"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/server/api"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/server/config"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/server/version"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	logger.Info("starting server",
		"name", version.Name,
		"version", version.Version,
	)

	cfg, err := config.Load()
	if err != nil {
		logger.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	// ── Tracing (OpenTelemetry) ──────────────────────────────────────────────
	// Real OTel path (replaces the old no-op stub). Enabled via OTEL_ENABLED=true;
	// exporter selectable via OTEL_EXPORTER (otlp|stdout|none) and OTEL_ENDPOINT.
	// When disabled, no provider is installed and the otelhttp middleware falls
	// back to the global no-op tracer (zero overhead).
	if os.Getenv("OTEL_ENABLED") == "true" {
		exporter := os.Getenv("OTEL_EXPORTER")
		if exporter == "" {
			exporter = "stdout"
		}
		tp, terr := otel.New(context.Background(), otel.Config{
			ServiceName:    version.Name,
			ServiceVersion: version.Version,
			Exporter:       exporter,
			Endpoint:       os.Getenv("OTEL_ENDPOINT"),
			SampleRatio:    1.0,
		})
		if terr != nil {
			logger.Error("failed to init tracing", "error", terr)
		} else {
			logger.Info("tracing enabled", "exporter", exporter)
			defer func() {
				sctx, scancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer scancel()
				_ = tp.Shutdown(sctx)
			}()
		}
	}

	// ── Metrics recorder ─────────────────────────────────────────────────────
	// Register the library-core metrics on the default Prometheus registry so
	// they are exposed by the same promhttp handler that serves /metrics. This
	// wires REAL rate-limit / circuit-breaker / bulkhead series (the ones the
	// Grafana dashboard queries) instead of leaving them empty.
	rec := metricprom.New(prometheus.DefaultRegisterer)

	// ── Rate limiters ────────────────────────────────────────────────────────
	limiters := map[string]ratelimit.Limiter{
		// token_bucket: 10 req/s, burst=20
		"token_bucket": tokenbucket.New(20, 10, tokenbucket.WithRecorder(rec)),

		// fixed_window: 10 req / 10s
		"fixed_window": fixedwindow.New(10, 10*time.Second, fixedwindow.WithRecorder(rec)),

		// sliding_window_log: 10 req / 10s
		"sliding_window_log": slidingwindow.NewLog(10, 10*time.Second, slidingwindow.WithLogRecorder(rec)),

		// sliding_window_counter: 10 req / 10s
		"sliding_window_counter": slidingwindow.NewCounter(10, 10*time.Second, slidingwindow.WithCounterRecorder(rec)),

		// gcra: 10 req/s, burst=5
		"gcra": gcra.New(10, 5, time.Second, gcra.WithRecorder(rec)),

		// leaky_bucket: leak rate 10 req/s, queue capacity 20
		"leaky_bucket": leakybucket.New(20, 10, leakybucket.WithRecorder(rec)),
	}

	// ── Circuit breakers ─────────────────────────────────────────────────────
	registry := circuitbreaker.NewRegistry()

	cbNames := []string{"primary", "secondary"}
	cbs := make(map[string]*circuitbreaker.CircuitBreaker, len(cbNames))
	for _, name := range cbNames {
		cb := registry.GetOrCreate(name, circuitbreaker.Config{
			Name:             name,
			WindowSize:       10,
			FailureThreshold: 5,
			OpenTimeout:      30 * time.Second,
			Recorder:         rec,
		})
		cbs[name] = cb
	}

	// ── Readiness flag ───────────────────────────────────────────────────────
	var ready atomic.Bool

	// ── HTTP server ──────────────────────────────────────────────────────────
	handler, hub := api.NewRouterWithHub(limiters, cbs, registry, logger, &ready, cfg.CORSOrigins, cfg.APIKey)

	srv := &http.Server{
		Addr:              cfg.Addr(),
		Handler:           handler,
		ReadTimeout:       cfg.ReadTimeout,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout, // M-22: mitigate Slowloris
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
		MaxHeaderBytes:    1 << 16, // 64 KiB header limit
	}

	// Start listening before marking ready so health checks pass
	serverErr := make(chan error, 1)
	go func() {
		logger.Info("server listening", "addr", cfg.Addr())
		ready.Store(true)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serverErr <- err
		}
		close(serverErr)
	}()

	// ── Graceful shutdown ────────────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)

	select {
	case sig := <-quit:
		logger.Info("received signal, shutting down", "signal", sig.String())
	case err := <-serverErr:
		if err != nil {
			logger.Error("server error", "error", err)
			os.Exit(1)
		}
	}

	ready.Store(false)

	ctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("shutdown error", "error", err)
		os.Exit(1)
	}

	// Stop the WebSocket hub so its Run goroutine exits and every client's
	// writePump is unblocked (closes each c.send) rather than leaking (H-17/F-2).
	hub.Stop()

	// Close all rate limiters
	for name, l := range limiters {
		if err := l.Close(); err != nil {
			logger.Warn("failed to close limiter", "name", name, "error", err)
		}
	}

	logger.Info("server stopped")
}
