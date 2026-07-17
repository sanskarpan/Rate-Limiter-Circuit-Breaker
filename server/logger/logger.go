// Package logger provides structured logging with slog for the demo server.
// Every request-scoped log line includes mandatory fields: request_id, method,
// path, status, duration_ms, client_ip, and user_agent.
package logger

import (
	"context"
	"log/slog"
	"os"
)

type contextKey string

const loggerKey contextKey = "logger"

// New creates a slog.Logger based on the environment.
// When format="json" (production), uses JSON handler.
// Otherwise uses text handler (development).
func New(format string) *slog.Logger {
	var h slog.Handler
	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}
	if format == "json" {
		h = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		h = slog.NewTextHandler(os.Stdout, opts)
	}
	return slog.New(h)
}

// WithRequestID returns a child logger with the request_id field pre-attached.
func WithRequestID(logger *slog.Logger, requestID string) *slog.Logger {
	return logger.With("request_id", requestID)
}

// ToCtx stores a logger in the context.
func ToCtx(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey, logger)
}

// FromCtx retrieves the request-scoped logger from the context.
// Falls back to the default logger if none is stored.
func FromCtx(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(loggerKey).(*slog.Logger); ok && l != nil {
		return l
	}
	return slog.Default()
}
