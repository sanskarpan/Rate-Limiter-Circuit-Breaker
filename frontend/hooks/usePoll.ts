'use client'

import { useEffect, useRef, useCallback } from 'react'

interface UsePollOptions {
  interval?: number
  enabled?: boolean
  immediate?: boolean
}

// The polled function may accept an AbortSignal that is aborted on unmount and
// before each new tick, so in-flight fetches can be cancelled.
type PollFn = (signal: AbortSignal) => Promise<void> | void

export function usePoll(fn: PollFn, options: UsePollOptions = {}) {
  const { interval = 5000, enabled = true, immediate = true } = options
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null)
  const fnRef = useRef(fn)
  // Controller for the request that is currently in flight (if any).
  const controllerRef = useRef<AbortController | null>(null)
  // Guard so we skip a tick while the previous request has not settled.
  const inFlightRef = useRef(false)

  // Keep fn ref up to date without re-running effects.
  useEffect(() => {
    fnRef.current = fn
  })

  const runTick = useCallback(() => {
    // Skip if the previous request is still in flight.
    if (inFlightRef.current) return

    // Abort any lingering controller and start a fresh one for this tick.
    controllerRef.current?.abort()
    const controller = new AbortController()
    controllerRef.current = controller
    inFlightRef.current = true

    Promise.resolve(fnRef.current(controller.signal)).finally(() => {
      // Only clear the guard if this tick's controller is still the current one
      // (a later abort/tick may have superseded it).
      if (controllerRef.current === controller) {
        inFlightRef.current = false
        controllerRef.current = null
      }
    })
  }, [])

  const stop = useCallback(() => {
    if (timerRef.current) {
      clearInterval(timerRef.current)
      timerRef.current = null
    }
    // Abort any in-flight request and reset the guard.
    controllerRef.current?.abort()
    controllerRef.current = null
    inFlightRef.current = false
  }, [])

  const start = useCallback(() => {
    stop()
    timerRef.current = setInterval(runTick, interval)
  }, [interval, stop, runTick])

  useEffect(() => {
    if (!enabled) {
      stop()
      return
    }

    if (immediate) {
      runTick()
    }

    start()

    return stop
  }, [enabled, immediate, start, stop, runTick])

  return { stop, start }
}
