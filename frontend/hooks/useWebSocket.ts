'use client'

import { useEffect, useRef } from 'react'
import { getWSManager } from '@/lib/ws/manager'
import { useAppStore } from '@/lib/store'
import type { WSEvent } from '@/lib/api/types'

export function useWebSocket() {
  const setWsStatus = useAppStore((s) => s.setWsStatus)
  const setAlgorithmResult = useAppStore((s) => s.setAlgorithmResult)
  const setSimStats = useAppStore((s) => s.setSimStats)
  const setCBSnapshot = useAppStore((s) => s.setCBSnapshot)

  const connectedRef = useRef(false)

  useEffect(() => {
    const manager = getWSManager()

    if (!connectedRef.current) {
      connectedRef.current = true
      manager.connect()
    }

    // The server envelope is { type, name, data, ts }. "name" is the topic key
    // (algorithm or CB name; "" for global). The "connected" welcome frame is
    // filtered out by the manager, so it never reaches this handler.
    const unsubEvent = manager.onEvent((event: WSEvent) => {
      switch (event.type) {
        case 'rate_limit_result':
          setAlgorithmResult(event.name, event.data)
          break
        case 'cb_state_change':
          // data is the circuit-breaker snapshot; keyed by snapshot.name.
          setCBSnapshot(event.data)
          break
        case 'sim_stats':
          setSimStats(event.name || 'global', event.data)
          break
      }
    })

    const unsubStatus = manager.onStatus(setWsStatus)

    return () => {
      unsubEvent()
      unsubStatus()
    }
  }, [setWsStatus, setAlgorithmResult, setSimStats, setCBSnapshot])
}
