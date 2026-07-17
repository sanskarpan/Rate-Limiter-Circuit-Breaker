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

	"github.com/sanskarpan/resilience/circuitbreaker"
	"github.com/sanskarpan/resilience/ratelimit"
	"github.com/sanskarpan/resilience/ratelimit/fixedwindow"
	"github.com/sanskarpan/resilience/ratelimit/gcra"
	"github.com/sanskarpan/resilience/ratelimit/leakybucket"
	"github.com/sanskarpan/resilience/ratelimit/slidingwindow"
	"github.com/sanskarpan/resilience/ratelimit/tokenbucket"
	"github.com/sanskarpan/resilience/server/api"
	"github.com/sanskarpan/resilience/server/config"
	"github.com/sanskarpan/resilience/server/version"
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

	// ── Rate limiters ────────────────────────────────────────────────────────
	limiters := map[string]ratelimit.Limiter{
		// token_bucket: 10 req/s, burst=20
		"token_bucket": tokenbucket.New(20, 10),

		// fixed_window: 10 req / 10s
		"fixed_window": fixedwindow.New(10, 10*time.Second),

		// sliding_window_log: 10 req / 10s
		"sliding_window_log": slidingwindow.NewLog(10, 10*time.Second),

		// sliding_window_counter: 10 req / 10s
		"sliding_window_counter": slidingwindow.NewCounter(10, 10*time.Second),

		// gcra: 10 req/s, burst=5
		"gcra": gcra.New(10, 5, time.Second),

		// leaky_bucket: leak rate 10 req/s, queue capacity 20
		"leaky_bucket": leakybucket.New(20, 10),
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
		})
		cbs[name] = cb
	}

	// ── Readiness flag ───────────────────────────────────────────────────────
	var ready atomic.Bool

	// ── HTTP server ──────────────────────────────────────────────────────────
	handler := api.NewRouter(limiters, cbs, registry, logger, &ready, cfg.CORSOrigins, cfg.APIKey)

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

	// Close all rate limiters
	for name, l := range limiters {
		if err := l.Close(); err != nil {
			logger.Warn("failed to close limiter", "name", name, "error", err)
		}
	}

	logger.Info("server stopped")
}
