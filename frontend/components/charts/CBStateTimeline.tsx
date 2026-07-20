'use client'

import { useMemo } from 'react'
import {
  ResponsiveContainer,
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
} from 'recharts'
import type { CBState } from '@/lib/api/types'

export interface CBStatePoint {
  /** Epoch milliseconds when the breaker was observed in `state`. */
  t: number
  state: CBState
}

interface CBStateTimelineProps {
  points: CBStatePoint[]
  maxPoints?: number
}

// Map each state to a numeric lane so a step line reads as a timeline:
// closed (healthy) at the bottom, open (tripped) at the top.
const STATE_LEVEL: Record<CBState, number> = {
  closed: 0,
  'half-open': 1,
  open: 2,
}
const LEVEL_LABEL: Record<number, string> = {
  0: 'closed',
  1: 'half-open',
  2: 'open',
}

/**
 * Step timeline of circuit-breaker state transitions. Fed by the poll/WS stream,
 * it shows when and for how long the breaker was OPEN vs CLOSED — the single
 * most useful breaker visualization that was previously missing.
 */
export function CBStateTimeline({ points, maxPoints = 120 }: CBStateTimelineProps) {
  const data = useMemo(() => {
    return points.slice(-maxPoints).map((p) => ({
      t: p.t,
      level: STATE_LEVEL[p.state],
      state: p.state,
    }))
  }, [points, maxPoints])

  if (data.length === 0) {
    return (
      <p className="py-6 text-center text-sm text-gray-400">
        No state history yet — interact with the breaker to record transitions.
      </p>
    )
  }

  const current = data[data.length - 1].state

  return (
    <div
      role="img"
      aria-label={`Circuit breaker state timeline over ${data.length} samples. Current state: ${current}.`}
    >
      <ResponsiveContainer width="100%" height={160}>
        <LineChart data={data} margin={{ top: 8, right: 8, left: -10, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.06)" />
          <XAxis
            dataKey="t"
            tick={{ fill: '#9ca3af', fontSize: 10 }}
            stroke="rgba(255,255,255,0.1)"
            tickFormatter={(t) => new Date(t).toLocaleTimeString()}
            minTickGap={40}
          />
          <YAxis
            type="number"
            domain={[0, 2]}
            ticks={[0, 1, 2]}
            tick={{ fill: '#9ca3af', fontSize: 10 }}
            stroke="rgba(255,255,255,0.1)"
            tickFormatter={(v: number) => LEVEL_LABEL[v] ?? ''}
            width={70}
          />
          <Tooltip
            contentStyle={{
              background: '#111827',
              border: '1px solid rgba(255,255,255,0.1)',
              borderRadius: 8,
              color: '#f9fafb',
              fontSize: 12,
            }}
            formatter={(_v, _n, item) => [
              (item?.payload as { state?: string })?.state ?? '',
              'State',
            ]}
            labelFormatter={(t) => new Date(t as number).toLocaleTimeString()}
          />
          <Line
            type="stepAfter"
            dataKey="level"
            stroke="#f59e0b"
            strokeWidth={2}
            dot={false}
            isAnimationActive={false}
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}
