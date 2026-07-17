package api

import (
	"net/http"
	"sync/atomic"
)

// HealthHandler handles liveness and readiness probe endpoints.
type HealthHandler struct {
	ready *atomic.Bool
}

// newHealthHandler creates a new HealthHandler backed by the given ready flag.
func newHealthHandler(ready *atomic.Bool) *HealthHandler {
	return &HealthHandler{ready: ready}
}

// Live handles GET /health/live.
// Always returns 200 OK as long as the process is running.
func (h *HealthHandler) Live(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// Ready handles GET /health/ready.
// Returns 200 when the server is ready to serve traffic, 503 otherwise.
func (h *HealthHandler) Ready(w http.ResponseWriter, r *http.Request) {
	if h.ready.Load() {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
		return
	}
	writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready"})
}
