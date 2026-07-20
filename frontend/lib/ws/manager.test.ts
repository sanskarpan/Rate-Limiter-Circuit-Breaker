import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { WSManager, getWSManager } from '@/lib/ws/manager'
import type { WSEvent } from '@/lib/api/types'

// Minimal fake WebSocket that lets tests drive open/message/close/error and
// inspect what the manager did. Mirrors the browser WebSocket surface used by
// the manager.
class FakeWebSocket {
  static instances: FakeWebSocket[] = []
  static CONNECTING = 0
  static OPEN = 1
  static CLOSING = 2
  static CLOSED = 3

  url: string
  readyState = FakeWebSocket.CONNECTING
  onopen: (() => void) | null = null
  onmessage: ((ev: { data: string }) => void) | null = null
  onclose: (() => void) | null = null
  onerror: (() => void) | null = null
  closed = false

  constructor(url: string) {
    this.url = url
    FakeWebSocket.instances.push(this)
  }

  open() {
    this.readyState = FakeWebSocket.OPEN
    this.onopen?.()
  }
  emit(data: unknown) {
    this.onmessage?.({ data: typeof data === 'string' ? data : JSON.stringify(data) })
  }
  emitRaw(data: string) {
    this.onmessage?.({ data })
  }
  triggerError() {
    this.onerror?.()
  }
  close() {
    this.closed = true
    this.readyState = FakeWebSocket.CLOSED
    this.onclose?.()
  }
}

const OPEN = FakeWebSocket.OPEN

describe('WSManager', () => {
  beforeEach(() => {
    FakeWebSocket.instances = []
    vi.stubGlobal('WebSocket', FakeWebSocket as unknown as typeof WebSocket)
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
    vi.unstubAllGlobals()
  })

  function connected() {
    const m = new WSManager('/ws/v1/events')
    m.connect()
    const sock = FakeWebSocket.instances[0]
    sock.open()
    return { m, sock }
  }

  it('parses a domain event and dispatches it to handlers', () => {
    const { m, sock } = connected()
    const received: WSEvent[] = []
    m.onEvent((e) => received.push(e))

    const evt: WSEvent = {
      type: 'rate_limit_result',
      name: 'token_bucket',
      data: {
        allowed: true,
        limit: 10,
        remaining: 9,
        reset_after_ms: 0,
        retry_after_ms: 0,
        algorithm: 'token_bucket',
        metadata: {},
      },
      ts: 1,
    }
    sock.emit(evt)
    expect(received).toEqual([evt])
  })

  it('ignores the non-domain "connected" welcome frame', () => {
    const { m, sock } = connected()
    const received: WSEvent[] = []
    m.onEvent((e) => received.push(e))
    sock.emit({ type: 'connected', name: '', data: {}, ts: 0 })
    expect(received).toHaveLength(0)
  })

  it('ignores malformed JSON without throwing', () => {
    const { m, sock } = connected()
    const received: WSEvent[] = []
    m.onEvent((e) => received.push(e))
    expect(() => sock.emitRaw('{not-json')).not.toThrow()
    expect(received).toHaveLength(0)
  })

  it('buffers events received before a handler attaches and flushes on subscribe', () => {
    const { m, sock } = connected()
    // No handler yet — this event must be buffered, not lost.
    const evt: WSEvent = {
      type: 'sim_stats',
      name: 'global',
      data: {
        total_requests: 1,
        allowed: 1,
        denied: 0,
        allowed_rate: 1,
        denied_rate: 0,
        duration_ms: 10,
      },
      ts: 5,
    }
    sock.emit(evt)

    const received: WSEvent[] = []
    m.onEvent((e) => received.push(e))
    // Handler attached after the fact still receives the buffered event.
    expect(received).toEqual([evt])
  })

  it('reports status transitions to status handlers', () => {
    const m = new WSManager('/ws/v1/events')
    const statuses: string[] = []
    m.onStatus((s) => statuses.push(s))
    m.connect()
    FakeWebSocket.instances[0].open()
    expect(statuses).toContain('connecting')
    expect(statuses).toContain('open')
  })

  it('schedules a reconnect with backoff after an unexpected close', () => {
    const { sock } = connected()
    // Deterministic jitter.
    vi.spyOn(Math, 'random').mockReturnValue(1)
    sock.close()
    // A reconnect timer is armed; advancing it creates a new socket.
    expect(FakeWebSocket.instances).toHaveLength(1)
    vi.advanceTimersByTime(1000)
    expect(FakeWebSocket.instances.length).toBeGreaterThanOrEqual(2)
  })

  it('does not reconnect after an explicit disconnect()', () => {
    const { m, sock } = connected()
    m.disconnect()
    expect(sock.closed).toBe(true)
    vi.advanceTimersByTime(60_000)
    // Only the original socket was ever created.
    expect(FakeWebSocket.instances).toHaveLength(1)
    expect(m.isStopped).toBe(true)
  })

  it('connect() is idempotent while OPEN', () => {
    const { m } = connected()
    m.connect()
    expect(FakeWebSocket.instances).toHaveLength(1)
    expect(m.readyState).toBe(OPEN)
  })

  it('getWSManager returns a fresh instance after the singleton is stopped', () => {
    const first = getWSManager()
    first.connect()
    first.disconnect()
    const second = getWSManager()
    expect(second).not.toBe(first)
    expect(second.isStopped).toBe(false)
  })
})
