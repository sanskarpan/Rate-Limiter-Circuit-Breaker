package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/bulkhead"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
)

// cbExecuteRequest is the body for POST /api/v1/cb/{name}/execute.
type cbExecuteRequest struct {
	// SimulateFailure causes the executed function to return an error.
	SimulateFailure bool `json:"simulate_failure"`
	// LatencyMs is the artificial latency added before returning.
	LatencyMs int `json:"latency_ms"`
}

// cbResponse is the response for circuit breaker operations.
type cbResponse struct {
	State    string        `json:"state"`
	Executed bool          `json:"executed"`
	Snapshot *snapshotJSON `json:"snapshot"`
	Error    string        `json:"error,omitempty"`
}

// snapshotJSON is a JSON-serialisable version of circuitbreaker.Snapshot.
type snapshotJSON struct {
	Name              string        `json:"name"`
	State             string        `json:"state"`
	Failures          int           `json:"failures"`
	Successes         int           `json:"successes"`
	Requests          int           `json:"requests"`
	FailureRate       float64       `json:"failure_rate"`
	OpenedAt          *time.Time    `json:"opened_at,omitempty"`
	TimeUntilHalfOpen time.Duration `json:"time_until_half_open_ms"`
}

func toSnapshotJSON(s circuitbreaker.Snapshot) *snapshotJSON {
	js := &snapshotJSON{
		Name:              s.Name,
		State:             s.State.String(),
		Failures:          s.Failures,
		Successes:         s.Successes,
		Requests:          s.Requests,
		FailureRate:       s.FailureRate,
		TimeUntilHalfOpen: s.TimeUntilHalfOpen,
	}
	if !s.OpenedAt.IsZero() {
		js.OpenedAt = &s.OpenedAt
	}
	return js
}

func (h *Handlers) lookupCB(w http.ResponseWriter, r *http.Request) *circuitbreaker.CircuitBreaker {
	name := r.PathValue("name")
	cb := h.registry.Get(name)
	if cb == nil {
		writeError(w, r, http.StatusNotFound, "not_found", "unknown circuit breaker: "+name)
	}
	return cb
}

// HandleCBExecute handles POST /api/v1/cb/{name}/execute
func (h *Handlers) HandleCBExecute(w http.ResponseWriter, r *http.Request) {
	cb := h.lookupCB(w, r)
	if cb == nil {
		return
	}

	var req cbExecuteRequest
	// M-21: an empty body is a valid "use defaults" request (io.EOF), but a
	// malformed JSON body must be rejected with 400 rather than silently
	// treated as defaults.
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		writeError(w, r, http.StatusBadRequest, "bad_request", "invalid JSON: "+err.Error())
		return
	}
	// Cap artificial latency to prevent resource exhaustion.
	if req.LatencyMs < 0 {
		req.LatencyMs = 0
	}
	if req.LatencyMs > 5000 {
		req.LatencyMs = 5000
	}

	// Bound concurrent executions through a bulkhead (populates the bulkhead
	// saturation metrics the Grafana dashboard queries).
	var cbErr error
	bhErr := h.execBulkhead.Execute(r.Context(), func(ctx context.Context) error {
		cbErr = cb.Execute(ctx, func(ctx context.Context) error {
			if req.LatencyMs > 0 {
				select {
				case <-time.After(time.Duration(req.LatencyMs) * time.Millisecond):
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			if req.SimulateFailure {
				return errors.New("simulated failure")
			}
			return nil
		})
		return nil
	})
	if errors.Is(bhErr, bulkhead.ErrBulkheadFull) {
		writeError(w, r, http.StatusServiceUnavailable, "bulkhead_full", "concurrency limit exceeded")
		return
	}
	execErr := cbErr

	snap := toSnapshotJSON(cb.Snapshot())
	resp := cbResponse{
		State:    cb.State().String(),
		Executed: cbErr == nil || !errors.Is(cbErr, circuitbreaker.ErrCircuitOpen),
		Snapshot: snap,
	}
	if execErr != nil {
		resp.Error = execErr.Error()
	}

	h.broadcastCBState(r.PathValue("name"), snap)

	status := http.StatusOK
	if errors.Is(cbErr, circuitbreaker.ErrCircuitOpen) {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, resp)
}

// broadcastCBState streams a circuit-breaker snapshot to subscribed WS clients
// under the "cb_state_change" event type (H-19).
func (h *Handlers) broadcastCBState(name string, snap *snapshotJSON) {
	if h.hub != nil {
		h.hub.BroadcastEvent("cb_state_change", name, snap)
	}
}

// HandleCBForceOpen handles POST /api/v1/cb/{name}/force-open
func (h *Handlers) HandleCBForceOpen(w http.ResponseWriter, r *http.Request) {
	cb := h.lookupCB(w, r)
	if cb == nil {
		return
	}
	// Force open by injecting failures until threshold is exceeded
	forceTransitionOpen(cb)
	snap := toSnapshotJSON(cb.Snapshot())
	h.broadcastCBState(r.PathValue("name"), snap)
	writeJSON(w, http.StatusOK, cbResponse{
		State:    cb.State().String(),
		Snapshot: snap,
	})
}

// HandleCBForceClose handles POST /api/v1/cb/{name}/force-close
func (h *Handlers) HandleCBForceClose(w http.ResponseWriter, r *http.Request) {
	cb := h.lookupCB(w, r)
	if cb == nil {
		return
	}
	forceTransitionClosed(cb)
	snap := toSnapshotJSON(cb.Snapshot())
	h.broadcastCBState(r.PathValue("name"), snap)
	writeJSON(w, http.StatusOK, cbResponse{
		State:    cb.State().String(),
		Snapshot: snap,
	})
}

// HandleCBForceHalfOpen handles POST /api/v1/cb/{name}/force-half-open
//
// M-18: the circuitbreaker package exposes no exported method to force a direct
// half-open transition (the transitionToHalfOpen method is unexported and only
// reachable internally after OpenTimeout elapses). The previous implementation
// called forceTransitionOpen and then reported OPEN — i.e. it lied about the
// resulting state. Rather than report a state we did not set, we return 501 Not
// Implemented with an honest message and DO NOT mutate the breaker.
func (h *Handlers) HandleCBForceHalfOpen(w http.ResponseWriter, r *http.Request) {
	cb := h.lookupCB(w, r)
	if cb == nil {
		return
	}
	writeError(w, r, http.StatusNotImplemented, "not_implemented",
		"forcing half-open is not supported: the circuit breaker only enters "+
			"half-open automatically after its open timeout elapses")
}

// HandleCBSnapshot handles GET /api/v1/cb/{name}/snapshot
func (h *Handlers) HandleCBSnapshot(w http.ResponseWriter, r *http.Request) {
	cb := h.lookupCB(w, r)
	if cb == nil {
		return
	}
	writeJSON(w, http.StatusOK, toSnapshotJSON(cb.Snapshot()))
}

// HandleCBAll handles GET /api/v1/cb/all
func (h *Handlers) HandleCBAll(w http.ResponseWriter, r *http.Request) {
	snapshots := h.registry.Snapshot()
	result := make(map[string]*snapshotJSON, len(snapshots))
	for name, snap := range snapshots {
		result[name] = toSnapshotJSON(snap)
	}
	writeJSON(w, http.StatusOK, result)
}

// forceTransitionOpen injects enough failures to open the circuit.
func forceTransitionOpen(cb *circuitbreaker.CircuitBreaker) {
	sentinel := errors.New("force-open sentinel")
	for i := 0; i < 20; i++ {
		if cb.State() == circuitbreaker.StateOpen {
			break
		}
		_ = cb.Execute(context.Background(), func(_ context.Context) error {
			return sentinel
		})
	}
}

// forceTransitionClosed injects enough successes to close the circuit.
func forceTransitionClosed(cb *circuitbreaker.CircuitBreaker) {
	for i := 0; i < 20; i++ {
		if cb.State() == circuitbreaker.StateClosed {
			break
		}
		_ = cb.Execute(context.Background(), func(_ context.Context) error {
			return nil
		})
	}
}
