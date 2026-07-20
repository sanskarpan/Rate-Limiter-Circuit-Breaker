'use client'

import { useMemo } from 'react'
import {
  ResponsiveContainer,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
} from 'recharts'

interface LatencyHistogramProps {
  /** Per-request latencies in milliseconds. */
  latencies: number[]
  /** Number of histogram buckets. */
  bins?: number
}

interface Bucket {
  label: string
  count: number
}

/**
 * Bucketed latency distribution. Fixed-width bins spanning [min, max] so the
 * long tail (p99) is visible next to the bulk of fast requests.
 */
export function LatencyHistogram({ latencies, bins = 12 }: LatencyHistogramProps) {
  const data = useMemo<Bucket[]>(() => {
    if (latencies.length === 0) return []
    const min = Math.min(...latencies)
    const max = Math.max(...latencies)
    // Degenerate case: all identical latencies → a single bucket.
    if (max === min) {
      return [{ label: `${min}ms`, count: latencies.length }]
    }
    const width = (max - min) / bins
    const buckets: Bucket[] = Array.from({ length: bins }, (_, b) => ({
      label: `${Math.round(min + b * width)}`,
      count: 0,
    }))
    for (const l of latencies) {
      // Clamp the max value into the last bucket.
      const idx = Math.min(bins - 1, Math.floor((l - min) / width))
      buckets[idx].count++
    }
    return buckets
  }, [latencies, bins])

  if (data.length === 0) {
    return (
      <p className="py-6 text-center text-sm text-gray-400">
        No latency samples yet.
      </p>
    )
  }

  return (
    <div
      role="img"
      aria-label={`Latency histogram across ${latencies.length} requests, bucketed into ${data.length} bins by milliseconds.`}
    >
      <ResponsiveContainer width="100%" height={180}>
        <BarChart data={data} margin={{ top: 8, right: 8, left: -20, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.06)" />
          <XAxis
            dataKey="label"
            tick={{ fill: '#9ca3af', fontSize: 10 }}
            stroke="rgba(255,255,255,0.1)"
            unit="ms"
          />
          <YAxis tick={{ fill: '#9ca3af', fontSize: 11 }} stroke="rgba(255,255,255,0.1)" allowDecimals={false} />
          <Tooltip
            cursor={{ fill: 'rgba(255,255,255,0.05)' }}
            contentStyle={{
              background: '#111827',
              border: '1px solid rgba(255,255,255,0.1)',
              borderRadius: 8,
              color: '#f9fafb',
              fontSize: 12,
            }}
            formatter={(v) => [v, 'Requests']}
            labelFormatter={(l) => `≈ ${l}ms`}
          />
          <Bar dataKey="count" fill="#3b82f6" radius={[3, 3, 0, 0]} isAnimationActive={false} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}
