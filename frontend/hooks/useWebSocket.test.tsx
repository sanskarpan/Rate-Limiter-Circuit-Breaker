import { describe, it, expect, beforeEach, vi } from 'vitest'
import { renderHook } from '@testing-library/react'
import type { WSEvent } from '@/lib/api/types'

// Controllable fake manager shared by the hook under test.
const listeners = {
  event: null as ((e: WSEvent) => void) | null,
  status: null as ((s: string) => void) | null,
}
const fakeManager = {
  connect: vi.fn(),
  onEvent: vi.fn((h: (e: WSEvent) => void) => {
    listeners.event = h
    return () => {
      listeners.event = null
    }
  }),
  onStatus: vi.fn((h: (s: string) => void) => {
    listeners.status = h
    return () => {
      listeners.status = null
    }
  }),
}

vi.mock('@/lib/ws/manager', () => ({
  getWSManager: () => fakeManager,
}))

import { useWebSocket } from '@/hooks/useWebSocket'
import { useAppStore } from '@/lib/store'

describe('useWebSocket', () => {
  beforeEach(() => {
    listeners.event = null
    listeners.status = null
    fakeManager.connect.mockClear()
    useAppStore.setState({
      algorithms: { token_bucket: { lastResult: null, history: [], stats: null } },
      cbSnapshots: {},
      wsStatus: 'idle',
      selectedAlgorithm: 'token_bucket',
    })
  })

  it('connects the manager on mount and subscribes to events/status', () => {
    renderHook(() => useWebSocket())
    expect(fakeManager.connect).toHaveBeenCalledTimes(1)
    expect(listeners.event).toBeTypeOf('function')
    expect(listeners.status).toBeTypeOf('function')
  })

  it('routes rate_limit_result into the store', () => {
    renderHook(() => useWebSocket())
    listeners.event?.({
      type: 'rate_limit_result',
      name: 'token_bucket',
      data: {
        allowed: false,
        limit: 5,
        remaining: 0,
        reset_after_ms: 100,
        retry_after_ms: 100,
        algorithm: 'token_bucket',
        metadata: {},
      },
      ts: 1,
    })
    expect(useAppStore.getState().algorithms.token_bucket.lastResult?.allowed).toBe(false)
  })

  it('routes cb_state_change into the store keyed by name', () => {
    renderHook(() => useWebSocket())
    listeners.event?.({
      type: 'cb_state_change',
      name: 'primary',
      data: {
        name: 'primary',
        state: 'open',
        failures: 5,
        successes: 0,
        requests: 5,
        failure_rate: 1,
        opened_at: null,
        time_until_half_open_ms: 3000,
      },
      ts: 2,
    })
    expect(useAppStore.getState().cbSnapshots.primary.state).toBe('open')
  })

  it('propagates status updates into the store', () => {
    renderHook(() => useWebSocket())
    listeners.status?.('open')
    expect(useAppStore.getState().wsStatus).toBe('open')
  })

  it('unsubscribes on unmount', () => {
    const { unmount } = renderHook(() => useWebSocket())
    unmount()
    expect(listeners.event).toBeNull()
    expect(listeners.status).toBeNull()
  })
})
