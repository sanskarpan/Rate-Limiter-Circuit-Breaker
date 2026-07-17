package api

import (
	"errors"
	"net/http"
	"strings"
	"unicode/utf8"
)

// SecurityHeaders middleware adds security headers to every response.
// These headers mitigate common web vulnerabilities such as XSS and clickjacking.
func SecurityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Content-Security-Policy", "default-src 'self'")
		h.Set("Referrer-Policy", "strict-origin-when-cross-origin")
		next.ServeHTTP(w, r)
	})
}

const maxKeyLen = 512

// validateKey validates a rate-limit key parameter.
// Rules: non-empty, max 512 bytes, no null bytes, no newlines (header injection prevention).
func validateKey(key string) error {
	if key == "" {
		return errors.New("key must not be empty")
	}
	if len(key) > maxKeyLen {
		return errors.New("key exceeds 512 bytes")
	}
	if !utf8.ValidString(key) {
		return errors.New("key must be valid UTF-8")
	}
	if strings.ContainsAny(key, "\x00\r\n") {
		return errors.New("key must not contain null bytes or newlines")
	}
	return nil
}
