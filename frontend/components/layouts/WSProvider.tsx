'use client'

import { useWebSocket } from '@/hooks/useWebSocket'
import type { ReactNode } from 'react'

interface WSProviderProps {
  children: ReactNode
}

export function WSProvider({ children }: WSProviderProps) {
  useWebSocket()
  return <>{children}</>
}
