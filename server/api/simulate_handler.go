package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/server/simulation"
)

// simulateRequest is the body for POST /api/v1/simulate.
type simulateRequest struct {
	Algorithm         string  `json:"algorithm"`
	Pattern           string  `json:"pattern"`
	DurationMs        int64   `json:"duration_ms"`
	RequestsPerSecond float64 `json:"requests_per_second"`
	Concurrency       int     `json:"concurrency"`
	Key               string  `json:"key"`
}

// clampSimulateRequest applies defaults and bounds to a decoded simulateRequest
// in place, defending the simulator against adversarial params (huge N, negative
// rates, runaway durations). Extracted so it can be unit- and fuzz-tested
// (§7.5) independently of running the (slow) simulation engine.
func clampSimulateRequest(req *simulateRequest) {
	if req.Algorithm == "" {
		req.Algorithm = "token_bucket"
	}
	if req.Pattern == "" {
		req.Pattern = "constant"
	}
	if req.DurationMs <= 0 {
		req.DurationMs = 5000
	}
	// Cap duration to 60 seconds to prevent runaway simulations.
	if req.DurationMs > 60_000 {
		req.DurationMs = 60_000
	}
	if req.RequestsPerSecond <= 0 {
		req.RequestsPerSecond = 20
	}
	// Cap RPS to prevent resource exhaustion. Also guards against NaN/Inf from
	// adversarial JSON: a NaN fails every comparison, so normalise it here.
	if req.RequestsPerSecond > 10_000 || req.RequestsPerSecond != req.RequestsPerSecond {
		req.RequestsPerSecond = 10_000
	}
	if req.Concurrency <= 0 {
		req.Concurrency = 1
	}
	// Cap concurrency to prevent goroutine exhaustion.
	if req.Concurrency > 500 {
		req.Concurrency = 500
	}
	if req.Key == "" {
		req.Key = "simulate"
	}
}

// decodeSimulateRequest decodes and validates the /simulate body. It returns the
// clamped request or a *simError describing how to respond. Split out for §7.5
// fuzzing without executing the engine.
func decodeSimulateRequest(r *http.Request) (simulateRequest, *simError) {
	var req simulateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return req, &simError{http.StatusBadRequest, "bad_request", "invalid JSON: " + err.Error()}
	}
	clampSimulateRequest(&req)
	if err := validateKey(req.Key); err != nil {
		return req, &simError{http.StatusBadRequest, "invalid_key", err.Error()}
	}
	return req, nil
}

// simError carries a status/code/message for a failed simulate decode.
type simError struct {
	status  int
	code    string
	message string
}

// HandleSimulate handles POST /api/v1/simulate
func (h *Handlers) HandleSimulate(w http.ResponseWriter, r *http.Request) {
	req, serr := decodeSimulateRequest(r)
	if serr != nil {
		writeError(w, r, serr.status, serr.code, serr.message)
		return
	}

	limiter, ok := h.limiters[req.Algorithm]
	if !ok {
		writeError(w, r, http.StatusNotFound, "not_found", "unknown algorithm: "+req.Algorithm)
		return
	}

	// M-19: bound the number of concurrent simulations server-wide. Acquire a
	// slot non-blockingly; reject with 429 when the cap is already reached so a
	// flood of /simulate calls can't amplify into unbounded goroutines.
	select {
	case h.simSem <- struct{}{}:
		defer func() { <-h.simSem }()
	default:
		writeError(w, r, http.StatusTooManyRequests, "too_many_simulations",
			"too many concurrent simulations; retry later")
		return
	}

	cfg := simulation.Config{
		Pattern:           simulation.Pattern(req.Pattern),
		Duration:          time.Duration(req.DurationMs) * time.Millisecond,
		RequestsPerSecond: req.RequestsPerSecond,
		Concurrency:       req.Concurrency,
		Key:               req.Key,
	}

	result, err := h.simEngine.Run(r.Context(), limiter, cfg)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, "simulation_error", err.Error())
		return
	}

	// H-19: publish the simulation results to subscribed WS clients.
	if h.hub != nil {
		h.hub.BroadcastEvent("sim_stats", req.Algorithm, result)
	}

	writeJSON(w, http.StatusOK, result)
}
