'use client'

import { useState, useCallback } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { allow } from '@/lib/api/client'
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/Card'
import type { RateLimitResult } from '@/lib/api/types'

const ALL_ALGORITHMS = [
  { value: 'token_bucket', label: 'Token Bucket', color: 'blue' },
  { value: 'sliding_window', label: 'Sliding Window', color: 'purple' },
  { value: 'fixed_window', label: 'Fixed Window', color: 'amber' },
  { value: 'leaky_bucket', label: 'Leaky Bucket', color: 'green' },
]

const COLOR_MAP: Record<string, string> = {
  blue: 'bg-blue-500',
  purple: 'bg-purple-500',
  amber: 'bg-amber-500',
  green: 'bg-green-500',
}

const TEXT_COLOR_MAP: Record<string, string> = {
  blue: 'text-blue-400',
  purple: 'text-purple-400',
  amber: 'text-amber-400',
  green: 'text-green-400',
}

const BORDER_COLOR_MAP: Record<string, string> = {
  blue: 'border-blue-500/30',
  purple: 'border-purple-500/30',
  amber: 'border-amber-500/30',
  green: 'border-green-500/30',
}

interface AlgoResult {
  result: RateLimitResult
  latencyMs: number
}

interface CompareState {
  history: { allowed: boolean; latencyMs: number }[]
  lastResult: AlgoResult | null
  loading: boolean
}

export default function ComparePage() {
  const [selected, setSelected] = useState<string[]>(['token_bucket', 'sliding_window'])
  const [key, setKey] = useState('compare-demo')
  const [n, setN] = useState(1)
  const [state, setState] = useState<Record<string, CompareState>>({})
  const [sending, setSending] = useState(false)

  const toggleAlgo = useCallback((algo: string) => {
    setSelected((prev) => {
      if (prev.includes(algo)) {
        if (prev.length <= 2) return prev // keep at least 2
        return prev.filter((a) => a !== algo)
      }
      if (prev.length >= 4) return prev // max 4
      return [...prev, algo]
    })
  }, [])

  const sendToAll = useCallback(async () => {
    setSending(true)
    await Promise.all(
      selected.map(async (algo) => {
        setState((prev) => ({
          ...prev,
          [algo]: { ...prev[algo], loading: true, history: prev[algo]?.history ?? [], lastResult: prev[algo]?.lastResult ?? null },
        }))
        const t0 = Date.now()
        try {
          const result = await allow(algo, key, n)
          const latencyMs = Date.now() - t0
          setState((prev) => ({
            ...prev,
            [algo]: {
              loading: false,
              lastResult: { result, latencyMs },
              history: [...(prev[algo]?.history ?? []).slice(-49), { allowed: result.allowed, latencyMs }],
            },
          }))
        } catch {
          setState((prev) => ({
            ...prev,
            [algo]: {
              loading: false,
              lastResult: prev[algo]?.lastResult ?? null,
              history: [...(prev[algo]?.history ?? []).slice(-49), { allowed: false, latencyMs: Date.now() - t0 }],
            },
          }))
        }
      }),
    )
    setSending(false)
  }, [selected, key, n])

  return (
    <div className="space-y-8">
      <div>
        <h1 className="text-3xl font-bold text-white">Algorithm Comparison</h1>
        <p className="mt-1 text-sm text-gray-400">
          Send the same request to multiple algorithms side-by-side and compare results
        </p>
      </div>

      {/* Algorithm selector */}
      <Card>
        <CardHeader>
          <CardTitle>Select Algorithms (2–4)</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex flex-wrap gap-3">
            {ALL_ALGORITHMS.map((a) => {
              const isSelected = selected.includes(a.value)
              return (
                <button
                  key={a.value}
                  onClick={() => toggleAlgo(a.value)}
                  className={`rounded-lg border px-4 py-2 text-sm font-medium transition-all ${
                    isSelected
                      ? `${BORDER_COLOR_MAP[a.color]} bg-white/10 ${TEXT_COLOR_MAP[a.color]}`
                      : 'border-white/10 text-gray-500 hover:border-white/20 hover:text-gray-300'
                  }`}
                >
                  {a.label}
                  {isSelected && <span className="ml-2 text-xs">✓</span>}
                </button>
              )
            })}
          </div>
        </CardContent>
      </Card>

      {/* Controls */}
      <Card>
        <CardHeader>
          <CardTitle>Request Config</CardTitle>
        </CardHeader>
        <CardContent className="flex flex-wrap items-end gap-4">
          <div className="min-w-[200px]">
            <label className="mb-1 block text-xs font-medium text-gray-400">Key</label>
            <input
              value={key}
              onChange={(e) => setKey(e.target.value)}
              className="w-full rounded-lg border border-white/10 bg-white/5 px-3 py-2 text-sm text-white focus:outline-none focus:ring-1 focus:ring-blue-500"
            />
          </div>
          <div>
            <label className="mb-1 block text-xs font-medium text-gray-400">N (tokens)</label>
            <div className="flex items-center gap-2">
              <input
                type="range" min={1} max={10} value={n}
                onChange={(e) => setN(Number(e.target.value))}
                className="w-24 accent-blue-500"
              />
              <span className="w-6 text-center font-mono text-sm text-white">{n}</span>
            </div>
          </div>
          <motion.button
            whileTap={{ scale: 0.96 }}
            onClick={sendToAll}
            disabled={sending}
            className="rounded-lg bg-blue-600 px-6 py-2.5 text-sm font-semibold text-white transition-colors hover:bg-blue-500 disabled:opacity-50"
          >
            {sending ? 'Sending...' : 'Send to All'}
          </motion.button>
        </CardContent>
      </Card>

      {/* Side-by-side results */}
      <div className={`grid gap-6 ${selected.length === 2 ? 'grid-cols-2' : selected.length === 3 ? 'grid-cols-3' : 'grid-cols-4'}`}>
        {selected.map((algo) => {
          const algoInfo = ALL_ALGORITHMS.find((a) => a.value === algo)!
          const s = state[algo]
          const last = s?.lastResult
          const history = s?.history ?? []
          const allowedCount = history.filter((h) => h.allowed).length
          const deniedCount = history.filter((h) => !h.allowed).length

          return (
            <div key={algo} className="space-y-4">
              <div className={`rounded-lg border ${BORDER_COLOR_MAP[algoInfo.color]} bg-white/5 p-4`}>
                <div className="flex items-center justify-between mb-3">
                  <h3 className={`font-semibold ${TEXT_COLOR_MAP[algoInfo.color]}`}>{algoInfo.label}</h3>
                  {s?.loading && (
                    <span className="h-2 w-2 animate-pulse rounded-full bg-blue-400" />
                  )}
                </div>

                {last ? (
                  <>
                    <div className={`mb-3 rounded-lg py-2 text-center text-sm font-bold ${
                      last.result.allowed ? 'bg-green-500/20 text-green-400' : 'bg-red-500/20 text-red-400'
                    }`}>
                      {last.result.allowed ? '✓ ALLOWED' : '✗ DENIED'}
                    </div>
                    <div className="space-y-1.5 text-xs">
                      <div className="flex justify-between">
                        <span className="text-gray-500">Remaining</span>
                        <span className="font-mono text-white">{last.result.remaining}/{last.result.limit}</span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-gray-500">Latency</span>
                        <span className="font-mono text-white">{last.latencyMs}ms</span>
                      </div>
                      {!last.result.allowed && last.result.retry_after_ms > 0 && (
                        <div className="flex justify-between">
                          <span className="text-gray-500">Retry After</span>
                          <span className="font-mono text-amber-400">{last.result.retry_after_ms}ms</span>
                        </div>
                      )}
                    </div>

                    {/* Capacity bar */}
                    <div className="mt-3">
                      <div className="h-2 w-full overflow-hidden rounded-full bg-gray-800">
                        <motion.div
                          className={`h-full rounded-full ${COLOR_MAP[algoInfo.color]}`}
                          animate={{ width: `${(last.result.remaining / Math.max(last.result.limit, 1)) * 100}%` }}
                          transition={{ type: 'spring', stiffness: 120, damping: 20 }}
                        />
                      </div>
                    </div>
                  </>
                ) : (
                  <p className="text-xs text-gray-600 text-center py-4">No requests yet</p>
                )}
              </div>

              {/* History dots */}
              {history.length > 0 && (
                <div className="rounded-lg border border-white/10 bg-white/5 p-3">
                  <div className="mb-2 flex items-center justify-between text-xs text-gray-500">
                    <span>History</span>
                    <span>
                      <span className="text-green-400">{allowedCount}↑</span>{' '}
                      <span className="text-red-400">{deniedCount}↓</span>
                    </span>
                  </div>
                  <div className="flex flex-wrap gap-1">
                    <AnimatePresence initial={false}>
                      {history.map((h, i) => (
                        <motion.div
                          key={i}
                          initial={{ scale: 0 }}
                          animate={{ scale: 1 }}
                          className={`h-2 w-2 rounded-full ${h.allowed ? 'bg-green-400' : 'bg-red-400'}`}
                        />
                      ))}
                    </AnimatePresence>
                  </div>
                </div>
              )}
            </div>
          )
        })}
      </div>

      {/* Diff table */}
      {Object.keys(state).length >= 2 && (
        <Card>
          <CardHeader>
            <CardTitle>Last Result Comparison</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-white/10">
                    <th className="pb-2 text-left font-medium text-gray-400">Metric</th>
                    {selected.map((algo) => {
                      const info = ALL_ALGORITHMS.find((a) => a.value === algo)!
                      return (
                        <th key={algo} className={`pb-2 text-right font-medium ${TEXT_COLOR_MAP[info.color]}`}>
                          {info.label}
                        </th>
                      )
                    })}
                  </tr>
                </thead>
                <tbody className="divide-y divide-white/5">
                  {['Allowed', 'Remaining', 'Limit', 'Retry After (ms)', 'Latency (ms)'].map((metric) => (
                    <tr key={metric}>
                      <td className="py-2 text-gray-500">{metric}</td>
                      {selected.map((algo) => {
                        const last = state[algo]?.lastResult
                        let val: string = '—'
                        if (last) {
                          switch (metric) {
                            case 'Allowed': val = last.result.allowed ? '✓' : '✗'; break
                            case 'Remaining': val = String(last.result.remaining); break
                            case 'Limit': val = String(last.result.limit); break
                            case 'Retry After (ms)': val = last.result.retry_after_ms > 0 ? String(last.result.retry_after_ms) : '—'; break
                            case 'Latency (ms)': val = String(last.latencyMs); break
                          }
                        }
                        const info = ALL_ALGORITHMS.find((a) => a.value === algo)!
                        return (
                          <td key={algo} className={`py-2 text-right font-mono ${
                            metric === 'Allowed'
                              ? val === '✓' ? 'text-green-400' : val === '✗' ? 'text-red-400' : 'text-gray-500'
                              : TEXT_COLOR_MAP[info.color]
                          }`}>
                            {val}
                          </td>
                        )
                      })}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  )
}
