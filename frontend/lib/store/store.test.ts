import { describe, it, expect, beforeEach } from 'vitest'
import { useAppStore } from '@/lib/store'
import type { RateLimitResult, CBSnapshot, SimResult, ServerSimStats } from '@/lib/api/types'

function makeResult(over: Partial<RateLimitResult> = {}): RateLimitResult {
  return {
    allowed: true,
    limit: 10,
    remaining: 9,
    reset_after_ms: 100,
    retry_after_ms: 0,
    algorithm: 'token_bucket',
    metadata: {},
    ...over,
  }
}

function makeSnapshot(over: Partial<CBSnapshot> = {}): CBSnapshot {
  return {
    name: 'primary',
    state: 'closed',
    failures: 0,
    successes: 0,
    requests: 0,
    failure_rate: 0,
    opened_at: null,
    time_until_half_open_ms: 0,
    ...over,
  }
}

describe('app store', () => {
  beforeEach(() => {
    // Reset to a clean, deterministic state before each test.
    useAppStore.setState({
      algorithms: {
        token_bucket: { lastResult: null, history: [], stats: null },
      },
      cbSnapshots: {},
      wsStatus: 'idle',
      selectedAlgorithm: 'token_bucket',
    })
  })

  it('setAlgorithmResult stores the latest result under the algorithm key', () => {
    const r = makeResult({ remaining: 3 })
    useAppStore.getState().setAlgorithmResult('token_bucket', r)
    expect(useAppStore.getState().algorithms.token_bucket.lastResult).toEqual(r)
  })

  it('setAlgorithmResult creates the entry for an unknown algorithm', () => {
    const r = makeResult({ algorithm: 'gcra' })
    useAppStore.getState().setAlgorithmResult('gcra', r)
    expect(useAppStore.getState().algorithms.gcra.lastResult).toEqual(r)
    expect(useAppStore.getState().algorithms.gcra.history).toEqual([])
  })

  it('addSimResult appends and caps history at 200 items', () => {
    const mk = (i: number): SimResult => ({
      timestamp: new Date(i).toISOString(),
      allowed: i % 2 === 0,
      latency_ms: i,
      algorithm: 'token_bucket',
      error: null,
      cb_state: null,
    })
    for (let i = 0; i < 250; i++) {
      useAppStore.getState().addSimResult(mk(i))
    }
    const history = useAppStore.getState().algorithms.token_bucket.history
    expect(history).toHaveLength(200)
    // Oldest dropped: last item is the most recent (i = 249).
    expect(history[history.length - 1].latency_ms).toBe(249)
    expect(history[0].latency_ms).toBe(50)
  })

  it('setSimStats attaches aggregated stats to the algorithm entry', () => {
    const stats: ServerSimStats = {
      total_requests: 100,
      allowed: 80,
      denied: 20,
      allowed_rate: 0.8,
      denied_rate: 0.2,
      duration_ms: 5000,
    }
    useAppStore.getState().setSimStats('token_bucket', stats)
    expect(useAppStore.getState().algorithms.token_bucket.stats).toEqual(stats)
  })

  it('setCBSnapshot upserts a single breaker keyed by name', () => {
    useAppStore.getState().setCBSnapshot(makeSnapshot({ name: 'a', state: 'open' }))
    useAppStore.getState().setCBSnapshot(makeSnapshot({ name: 'b' }))
    // Update existing "a".
    useAppStore.getState().setCBSnapshot(makeSnapshot({ name: 'a', state: 'closed' }))
    const snaps = useAppStore.getState().cbSnapshots
    expect(Object.keys(snaps).sort()).toEqual(['a', 'b'])
    expect(snaps.a.state).toBe('closed')
  })

  it('setCBSnapshots replaces the whole map keyed by name', () => {
    useAppStore.getState().setCBSnapshot(makeSnapshot({ name: 'stale' }))
    useAppStore
      .getState()
      .setCBSnapshots([makeSnapshot({ name: 'x' }), makeSnapshot({ name: 'y' })])
    const snaps = useAppStore.getState().cbSnapshots
    expect(Object.keys(snaps).sort()).toEqual(['x', 'y'])
    expect(snaps.stale).toBeUndefined()
  })

  it('setWsStatus and setSelectedAlgorithm update UI slices', () => {
    useAppStore.getState().setWsStatus('open')
    useAppStore.getState().setSelectedAlgorithm('leaky_bucket')
    expect(useAppStore.getState().wsStatus).toBe('open')
    expect(useAppStore.getState().selectedAlgorithm).toBe('leaky_bucket')
  })
})
