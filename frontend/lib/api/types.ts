// Exact TypeScript mirrors of Go types

export interface RateLimitResult {
  allowed: boolean
  limit: number
  remaining: number
  reset_after_ms: number
  retry_after_ms: number
  algorithm: string
  metadata: Record<string, unknown>
}

// Mirrors circuitbreaker.State.String() in the Go server (state.go): the
// stringer emits "half-open" with a hyphen, not an underscore.
export type CBState = 'closed' | 'open' | 'half-open'

export interface CBSnapshot {
  name: string
  state: CBState
  failures: number
  successes: number
  requests: number
  failure_rate: number
  opened_at: string | null
  time_until_half_open_ms: number
}

// CBExecuteResponse mirrors the Go cbResponse returned by
// POST /api/v1/cb/{name}/execute (server/api/circuitbreaker_handlers.go).
export interface CBExecuteResponse {
  state: CBState
  executed: boolean
  snapshot: CBSnapshot
  error?: string
}

// SimResult is a frontend-local per-request record used by the simulate and
// algorithm pages. It is NOT part of the server WebSocket contract.
export interface SimResult {
  timestamp: string
  allowed: boolean
  latency_ms: number
  algorithm: string
  error: string | null
  cb_state: CBState | null
}

// SimStats is the frontend-local aggregation computed on the simulate page.
export interface SimStats {
  total: number
  allowed: number
  denied: number
  errors: number
  p50_ms: number
  p95_ms: number
  p99_ms: number
  rps: number
}

// ServerSimStats mirrors the Go simulation.Result payload carried by the
// "sim_stats" WebSocket event (server/simulation/engine.go).
export interface ServerSimStats {
  total_requests: number
  allowed: number
  denied: number
  allowed_rate: number
  denied_rate: number
  duration_ms: number
}

// WSEvent mirrors the finalized server → client envelope
// (server/api/hub.go): every message is { type, name, data, ts }. The "name"
// field is the topic key (algorithm or CB name; "" for global). The
// "connected" welcome frame is intentionally not modelled here — the WS
// manager filters it out before events reach consumers.
export type WSEvent =
  | { type: 'rate_limit_result'; name: string; data: RateLimitResult; ts: number }
  | { type: 'cb_state_change'; name: string; data: CBSnapshot; ts: number }
  | { type: 'sim_stats'; name: string; data: ServerSimStats; ts: number }
