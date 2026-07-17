import type { RateLimitResult, CBSnapshot, CBExecuteResponse } from './types'

// Latency (ms) used to simulate a timeout on the circuit breaker. It must
// exceed the CB's RequestTimeout so the execute call trips the timeout path.
const CB_TIMEOUT_LATENCY_MS = 5000

const BASE_URL = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8080'

async function safeFetch<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(url, init)
  if (!res.ok) {
    throw new Error(`HTTP ${res.status}: ${res.statusText}`)
  }
  return res.json() as Promise<T>
}

export async function allow(
  algorithm: string,
  key: string = 'demo',
  n: number = 1,
): Promise<RateLimitResult> {
  return safeFetch<RateLimitResult>(
    `${BASE_URL}/api/v1/limiters/${algorithm}/allow`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ key, n }),
    },
  )
}

export async function getCBSnapshot(name: string): Promise<CBSnapshot> {
  return safeFetch<CBSnapshot>(`${BASE_URL}/api/v1/cb/${name}/snapshot`)
}

export async function getAllCBSnapshots(): Promise<CBSnapshot[]> {
  // The server returns a map keyed by breaker name ({"primary": {...}, ...}),
  // not an array. Normalize to an array so callers can iterate/guard with
  // Array.isArray. (Contract mismatch caught by browser E2E.)
  const raw = await safeFetch<Record<string, CBSnapshot> | CBSnapshot[]>(
    `${BASE_URL}/api/v1/cb/all`,
  )
  if (Array.isArray(raw)) return raw
  return Object.values(raw ?? {})
}

export async function executeCB(
  name: string,
  simulate: 'success' | 'failure' | 'timeout',
): Promise<CBExecuteResponse> {
  return safeFetch<CBExecuteResponse>(`${BASE_URL}/api/v1/cb/${name}/execute`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      simulate_failure: simulate !== 'success',
      latency_ms: simulate === 'timeout' ? CB_TIMEOUT_LATENCY_MS : 0,
    }),
  })
}

export { BASE_URL }
