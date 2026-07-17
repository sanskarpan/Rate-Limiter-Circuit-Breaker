import type { WSEvent } from '@/lib/api/types'

type EventHandler = (event: WSEvent) => void
type StatusHandler = (status: 'connecting' | 'open' | 'closed' | 'error') => void

// The server's "connected" welcome frame uses the standard envelope but is not
// a domain event; it is filtered out before reaching consumers.
type RawEnvelope = { type?: unknown }

const WS_BASE_URL =
  (process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8080').replace(
    /^http/,
    'ws',
  )

// Domain event types the client cares about. Anything else (e.g. "connected")
// is ignored.
const DOMAIN_EVENT_TYPES = new Set<WSEvent['type']>([
  'rate_limit_result',
  'cb_state_change',
  'sim_stats',
])

// Max number of events buffered while no handler is attached (e.g. during a
// reconnect before the UI re-subscribes). Oldest are dropped past this cap.
const MAX_BUFFER = 256

export class WSManager {
  private ws: WebSocket | null = null
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null
  private retryCount = 0
  private maxRetries = 10
  private baseDelay = 1000 // ms — spec: 1s, 2s, 4s … capped at 30s
  private maxDelay = 30_000 // ms
  private handlers: Set<EventHandler> = new Set()
  private statusHandlers: Set<StatusHandler> = new Set()
  private url: string
  private stopped = false
  // Events received while no handler is attached; flushed when one subscribes.
  private buffer: WSEvent[] = []

  constructor(path: string = '/ws/v1/events') {
    this.url = `${WS_BASE_URL}${path}`
  }

  connect() {
    if (this.stopped) return

    // Idempotent: don't overwrite a socket that is already OPEN or CONNECTING.
    if (
      this.ws &&
      (this.ws.readyState === WebSocket.OPEN ||
        this.ws.readyState === WebSocket.CONNECTING)
    ) {
      return
    }

    this.setStatus('connecting')

    try {
      this.ws = new WebSocket(this.url)
    } catch {
      this.scheduleReconnect()
      return
    }

    this.ws.onopen = () => {
      this.retryCount = 0
      this.setStatus('open')
      // A fresh connection is live; drain anything buffered during the gap.
      this.flushBuffer()
    }

    this.ws.onmessage = (ev: MessageEvent) => {
      let parsed: unknown
      try {
        parsed = JSON.parse(ev.data as string)
      } catch {
        return // ignore malformed messages
      }
      const envelope = parsed as RawEnvelope
      if (
        typeof envelope.type !== 'string' ||
        !DOMAIN_EVENT_TYPES.has(envelope.type as WSEvent['type'])
      ) {
        // Ignore non-domain frames such as the "connected" welcome.
        return
      }
      this.dispatch(parsed as WSEvent)
    }

    this.ws.onclose = () => {
      this.setStatus('closed')
      if (!this.stopped) this.scheduleReconnect()
    }

    this.ws.onerror = () => {
      this.setStatus('error')
      this.ws?.close()
    }
  }

  private dispatch(event: WSEvent) {
    if (this.handlers.size === 0) {
      // No consumer attached yet — buffer so nothing is lost across a reconnect.
      this.buffer.push(event)
      if (this.buffer.length > MAX_BUFFER) {
        this.buffer.splice(0, this.buffer.length - MAX_BUFFER)
      }
      return
    }
    this.handlers.forEach((h) => h(event))
  }

  private flushBuffer() {
    if (this.buffer.length === 0 || this.handlers.size === 0) return
    const pending = this.buffer
    this.buffer = []
    pending.forEach((event) => this.handlers.forEach((h) => h(event)))
  }

  private scheduleReconnect() {
    if (this.stopped || this.retryCount >= this.maxRetries) return
    // Exponential backoff (1s, 2s, 4s … capped) with full jitter to avoid a
    // thundering herd of simultaneous reconnects.
    const base = Math.min(
      this.baseDelay * Math.pow(2, this.retryCount),
      this.maxDelay,
    )
    const delay = Math.random() * base
    this.retryCount++
    this.reconnectTimer = setTimeout(() => this.connect(), delay)
  }

  disconnect() {
    this.stopped = true
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer)
      this.reconnectTimer = null
    }
    this.ws?.close()
    this.ws = null
  }

  onEvent(handler: EventHandler) {
    this.handlers.add(handler)
    // Deliver anything buffered while no handler was attached.
    this.flushBuffer()
    return () => this.handlers.delete(handler)
  }

  onStatus(handler: StatusHandler) {
    this.statusHandlers.add(handler)
    return () => this.statusHandlers.delete(handler)
  }

  private setStatus(status: 'connecting' | 'open' | 'closed' | 'error') {
    this.statusHandlers.forEach((h) => h(status))
  }

  get readyState(): number {
    return this.ws?.readyState ?? WebSocket.CLOSED
  }

  // True once disconnect() has been called; such an instance cannot reconnect
  // and must be replaced on the next mount.
  get isStopped(): boolean {
    return this.stopped
  }
}

// Singleton
let _manager: WSManager | null = null

export function getWSManager(): WSManager {
  // A previous disconnect() sets `stopped`, which would permanently wedge the
  // singleton if the app remounts. Recreate the manager in that case so a fresh
  // mount can reconnect.
  if (!_manager || _manager.isStopped) {
    _manager = new WSManager('/ws/v1/events')
  }
  return _manager
}
