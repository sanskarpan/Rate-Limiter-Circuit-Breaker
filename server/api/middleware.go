package api

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/sanskarpan/resilience/server/metrics"
)

// maxRequestBodyBytes is the maximum allowed request body size (1 MiB).
const maxRequestBodyBytes = 1 << 20

type contextKey string

const requestIDKey contextKey = "request_id"

// responseRecorder wraps http.ResponseWriter to capture the status code.
type responseRecorder struct {
	http.ResponseWriter
	statusCode int
	written    bool
}

func (rr *responseRecorder) WriteHeader(code int) {
	if !rr.written {
		rr.statusCode = code
		rr.written = true
		rr.ResponseWriter.WriteHeader(code)
	}
}

func (rr *responseRecorder) Write(b []byte) (int, error) {
	if !rr.written {
		rr.statusCode = http.StatusOK
		rr.written = true
	}
	return rr.ResponseWriter.Write(b)
}

// Hijack implements http.Hijacker so wrapped handlers (e.g. the WebSocket
// upgrader) can take over the connection. Without this, wrapping the
// ResponseWriter in the Logger/Metrics middleware breaks WS upgrades with a 500.
func (rr *responseRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hj, ok := rr.ResponseWriter.(http.Hijacker); ok {
		return hj.Hijack()
	}
	return nil, nil, fmt.Errorf("underlying ResponseWriter does not support hijacking")
}

// RequestID middleware attaches a unique request ID to each request context
// and sets the X-Request-ID response header.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" {
			b := make([]byte, 8)
			rand.Read(b) //nolint:errcheck
			id = hex.EncodeToString(b)
		}
		w.Header().Set("X-Request-ID", id)
		ctx := context.WithValue(r.Context(), requestIDKey, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// requestIDFromContext retrieves the request ID from the context.
func requestIDFromContext(ctx context.Context) string {
	if id, ok := ctx.Value(requestIDKey).(string); ok {
		return id
	}
	return ""
}

// Logger middleware logs each request with method, path, status and duration.
func Logger(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rr := &responseRecorder{ResponseWriter: w, statusCode: http.StatusOK}
			next.ServeHTTP(rr, r)
			logger.Info("request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", rr.statusCode,
				"duration_ms", time.Since(start).Milliseconds(),
				"remote_addr", r.RemoteAddr,
				"request_id", requestIDFromContext(r.Context()),
			)
		})
	}
}

// Metrics middleware records bounded HTTP request metrics (L-13).
//
// The "path" label uses the MATCHED ROUTE PATTERN (r.Pattern, e.g.
// "POST /api/v1/cb/{name}/execute"), NOT the raw URL path. This keeps
// cardinality bounded to the finite set of registered routes rather than
// exploding on user-supplied {name}/{algorithm} values (cardinality DoS).
// Unmatched requests are bucketed under "other".
func Metrics(m *metrics.Metrics) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rr := &responseRecorder{ResponseWriter: w, statusCode: http.StatusOK}
			next.ServeHTTP(rr, r)

			route := r.Pattern
			if route == "" {
				route = "other"
			}
			m.HTTPRequestsTotal.WithLabelValues(
				r.Method, route, strconv.Itoa(rr.statusCode)).Inc()
			m.HTTPDurationSecs.WithLabelValues(
				r.Method, route).Observe(time.Since(start).Seconds())
		})
	}
}

// CORS middleware adds Cross-Origin Resource Sharing headers.
// If a wildcard "*" is present in origins, it is returned literally (not
// reflecting the client Origin), which is incompatible with credentials.
func CORS(origins []string) func(http.Handler) http.Handler {
	originSet := make(map[string]bool, len(origins))
	hasWildcard := false
	for _, o := range origins {
		trimmed := strings.TrimSpace(o)
		if trimmed == "*" {
			hasWildcard = true
		}
		originSet[trimmed] = true
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")
			if origin != "" {
				if originSet[origin] {
					// Exact match — reflect the specific origin so credentials work.
					w.Header().Set("Access-Control-Allow-Origin", origin)
					w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
					w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Request-ID")
					w.Header().Set("Access-Control-Expose-Headers", "X-Request-ID")
					w.Header().Set("Access-Control-Max-Age", "86400")
				} else if hasWildcard {
					// Wildcard — return literal "*" (no credentials allowed).
					w.Header().Set("Access-Control-Allow-Origin", "*")
					w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
					w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Request-ID")
					w.Header().Set("Access-Control-Expose-Headers", "X-Request-ID")
					w.Header().Set("Access-Control-Max-Age", "86400")
				}
			}

			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// Recovery middleware catches panics and returns a 500 Internal Server Error.
func Recovery(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					buf := make([]byte, 4096)
					n := runtime.Stack(buf, false)
					logger.Error("panic recovered",
						"panic", fmt.Sprintf("%v", rec),
						"stack", string(buf[:n]),
						"method", r.Method,
						"path", r.URL.Path,
						"request_id", requestIDFromContext(r.Context()),
					)
					writeError(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// LimitRequestBody middleware restricts incoming request bodies to maxRequestBodyBytes.
func LimitRequestBody(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ContentLength > maxRequestBodyBytes {
			writeError(w, r, http.StatusRequestEntityTooLarge, "payload_too_large",
				fmt.Sprintf("request body must not exceed %d bytes", maxRequestBodyBytes))
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
		next.ServeHTTP(w, r)
	})
}

// APIKeyAuth returns a middleware that enforces API-key authentication when a
// key is configured. Clients must supply the key via either the
// "Authorization: Bearer <key>" header or the "X-API-Key" header; the value is
// compared using crypto/subtle.ConstantTimeCompare to avoid timing leaks.
//
// When apiKey is empty (default demo mode) the middleware is a no-op — every
// request is allowed. Callers should log a startup warning in that case; see
// NewRouter.
func APIKeyAuth(apiKey string) func(http.Handler) http.Handler {
	keyBytes := []byte(apiKey)
	return func(next http.Handler) http.Handler {
		if apiKey == "" {
			// Unauthenticated demo mode — pass through untouched.
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !validAPIKey(r, keyBytes) {
				writeError(w, r, http.StatusUnauthorized, "unauthorized",
					"missing or invalid API key")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// validAPIKey reports whether the request carries the configured key in either
// the Authorization: Bearer header or the X-API-Key header.
func validAPIKey(r *http.Request, keyBytes []byte) bool {
	if h := r.Header.Get("Authorization"); h != "" {
		const prefix = "Bearer "
		if strings.HasPrefix(h, prefix) {
			presented := []byte(strings.TrimSpace(h[len(prefix):]))
			if subtle.ConstantTimeCompare(presented, keyBytes) == 1 {
				return true
			}
		}
	}
	if h := r.Header.Get("X-API-Key"); h != "" {
		if subtle.ConstantTimeCompare([]byte(h), keyBytes) == 1 {
			return true
		}
	}
	return false
}

// RequireJSON middleware validates that requests to mutating endpoints
// have Content-Type: application/json.
func RequireJSON(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost || r.Method == http.MethodPut || r.Method == http.MethodPatch {
			ct := r.Header.Get("Content-Type")
			if !strings.HasPrefix(ct, "application/json") {
				writeError(w, r, http.StatusUnsupportedMediaType, "unsupported_media_type", "Content-Type must be application/json")
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// writeError writes a standardised JSON error response.
func writeError(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{ //nolint:errcheck
		"error":      message,
		"code":       code,
		"request_id": requestIDFromContext(r.Context()),
	})
}

// writeJSON writes a JSON response with the given status code.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v) //nolint:errcheck
}
