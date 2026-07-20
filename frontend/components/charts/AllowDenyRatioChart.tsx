'use client'

import { useMemo } from 'react'
import {
  ResponsiveContainer,
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
} from 'recharts'
import type { SimResult } from '@/lib/api/types'

interface AllowDenyRatioChartProps {
  /** Chronological request results; only the last `window` are charted. */
  results: SimResult[]
  /** Size of the trailing window used to compute each rolling ratio point. */
  window?: number
  /** Max points to retain on the chart (keeps re-renders cheap). */
  maxPoints?: number
}

interface Point {
  i: number
  allowRate: number
}

/**
 * Rolling allow-rate over the request stream: for each request we compute the
 * fraction of the trailing `window` requests that were allowed. This surfaces
 * the moment a limiter starts shedding load far better than raw counters.
 */
export function AllowDenyRatioChart({
  results,
  window = 20,
  maxPoints = 120,
}: AllowDenyRatioChartProps) {
  const data = useMemo<Point[]>(() => {
    const pts: Point[] = []
    for (let i = 0; i < results.length; i++) {
      const start = Math.max(0, i - window + 1)
      const slice = results.slice(start, i + 1)
      const allowed = slice.filter((r) => r.allowed).length
      pts.push({ i, allowRate: (allowed / slice.length) * 100 })
    }
    // Cap retained points so long simulations don't blow up the DOM/SVG.
    return pts.slice(-maxPoints)
  }, [results, window, maxPoints])

  if (data.length === 0) {
    return (
      <p className="py-6 text-center text-sm text-gray-400">
        No requests yet — start a simulation to see the rolling allow rate.
      </p>
    )
  }

  return (
    <div
      role="img"
      aria-label={`Rolling allow-rate chart over a ${window}-request window. Latest allow rate ${data[
        data.length - 1
      ].allowRate.toFixed(0)} percent.`}
    >
      <ResponsiveContainer width="100%" height={180}>
        <AreaChart data={data} margin={{ top: 8, right: 8, left: -20, bottom: 0 }}>
          <defs>
            <linearGradient id="allowRateFill" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor="#22c55e" stopOpacity={0.35} />
              <stop offset="100%" stopColor="#22c55e" stopOpacity={0} />
            </linearGradient>
          </defs>
          <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.06)" />
          <XAxis dataKey="i" tick={{ fill: '#9ca3af', fontSize: 11 }} stroke="rgba(255,255,255,0.1)" />
          <YAxis
            domain={[0, 100]}
            tick={{ fill: '#9ca3af', fontSize: 11 }}
            stroke="rgba(255,255,255,0.1)"
            unit="%"
          />
          <Tooltip
            contentStyle={{
              background: '#111827',
              border: '1px solid rgba(255,255,255,0.1)',
              borderRadius: 8,
              color: '#f9fafb',
              fontSize: 12,
            }}
            formatter={(v) => [`${Number(v).toFixed(1)}%`, 'Allow rate']}
            labelFormatter={(l) => `Request #${l}`}
          />
          <Area
            type="monotone"
            dataKey="allowRate"
            stroke="#22c55e"
            strokeWidth={2}
            fill="url(#allowRateFill)"
            isAnimationActive={false}
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  )
}
