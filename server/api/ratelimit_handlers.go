package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

// rateLimitAllowRequest is the body for POST /api/v1/limiters/{algorithm}/allow.
type rateLimitAllowRequest struct {
	Key string `json:"key"`
	N   int    `json:"n"` // tokens to consume; defaults to 1
}

// rateLimitResponse is returned by allow and state endpoints.
type rateLimitResponse struct {
	Allowed      bool   `json:"allowed"`
	Limit        int    `json:"limit"`
	Remaining    int    `json:"remaining"`
	RetryAfterMs int64  `json:"retry_after_ms"`
	Algorithm    string `json:"algorithm"`
}

// HandleAllow handles POST /api/v1/limiters/{algorithm}/allow
func (h *Handlers) HandleAllow(w http.ResponseWriter, r *http.Request) {
	algorithm := r.PathValue("algorithm")
	limiter, ok := h.limiters[algorithm]
	if !ok {
		writeError(w, r, http.StatusNotFound, "not_found", "unknown algorithm: "+algorithm)
		return
	}

	var req rateLimitAllowRequest
	// M-21: empty body (io.EOF) is a valid "use defaults" request, but malformed
	// JSON must be rejected with 400 rather than silently defaulting.
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		writeError(w, r, http.StatusBadRequest, "bad_request", "invalid JSON: "+err.Error())
		return
	}
	if req.Key == "" {
		req.Key = "default"
	}
	if req.N <= 0 {
		req.N = 1
	}
	if err := validateKey(req.Key); err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_key", err.Error())
		return
	}

	rl := limiter.AllowN(r.Context(), req.Key, req.N)

	resp := rateLimitResponse{
		Allowed:      rl.Allowed,
		Limit:        rl.Limit,
		Remaining:    rl.Remaining,
		RetryAfterMs: rl.RetryAfter.Milliseconds(),
		Algorithm:    rl.Algorithm,
	}

	// H-19: stream the result to subscribed WebSocket clients.
	if h.hub != nil {
		h.hub.BroadcastEvent("rate_limit_result", algorithm, resp)
	}

	status := http.StatusOK
	if !rl.Allowed {
		status = http.StatusTooManyRequests
	}
	writeJSON(w, status, resp)
}

// HandleConfigure handles POST /api/v1/limiters/{algorithm}/configure
// Currently returns 501 Not Implemented as runtime reconfiguration is complex.
func (h *Handlers) HandleConfigure(w http.ResponseWriter, r *http.Request) {
	algorithm := r.PathValue("algorithm")
	if _, ok := h.limiters[algorithm]; !ok {
		writeError(w, r, http.StatusNotFound, "not_found", "unknown algorithm: "+algorithm)
		return
	}
	writeError(w, r, http.StatusNotImplemented, "not_implemented",
		"runtime reconfiguration is not yet supported; restart with updated env vars")
}

// HandleState handles GET /api/v1/limiters/{algorithm}/state
func (h *Handlers) HandleState(w http.ResponseWriter, r *http.Request) {
	algorithm := r.PathValue("algorithm")
	limiter, ok := h.limiters[algorithm]
	if !ok {
		writeError(w, r, http.StatusNotFound, "not_found", "unknown algorithm: "+algorithm)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		key = "default"
	}
	if err := validateKey(key); err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_key", err.Error())
		return
	}

	state := limiter.Peek(r.Context(), key)
	writeJSON(w, http.StatusOK, map[string]any{
		"algorithm": state.Algorithm,
		"key":       state.Key,
		"limit":     state.Limit,
		"remaining": state.Remaining,
		"reset_at":  state.ResetAt,
		"extra":     state.Extra,
	})
}
