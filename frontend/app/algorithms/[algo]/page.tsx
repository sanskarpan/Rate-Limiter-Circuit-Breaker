'use client'

import { useState, useCallback } from 'react'
import { useParams } from 'next/navigation'
import { motion, AnimatePresence } from 'framer-motion'
import { useAppStore, ALGO_LABELS } from '@/lib/store'
import { allow } from '@/lib/api/client'
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/Card'
import { StatusBadge } from '@/components/ui/StatusBadge'
import { MetricTile } from '@/components/ui/MetricTile'
import { TokenBucketViz } from '@/components/algorithms/TokenBucketViz'
import type { RateLimitResult } from '@/lib/api/types'

function AlgorithmViz({
  algo,
  lastResult,
}: {
  algo: string
  lastResult: RateLimitResult | null
}) {
  if (algo === 'token_bucket') {
    const tokens = lastResult?.remaining ?? 0
    const capacity = lastResult?.limit ?? 10
    return (
      <TokenBucketViz
        tokens={tokens}
        capacity={capacity}
        lastAllowed={lastResult?.allowed}
      />
    )
  }

  // Generic bar for other algorithms
  const ratio = lastResult
    ? lastResult.remaining / Math.max(lastResult.limit, 1)
    : 0
  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between text-sm text-gray-400">
        <span>Remaining capacity</span>
        <span className="font-mono">
          {lastResult?.remaining ?? '—'} / {lastResult?.limit ?? '—'}
        </span>
      </div>
      <div className="h-4 w-full overflow-hidden rounded-full bg-gray-800">
        <motion.div
          className="h-full rounded-full bg-blue-500"
          animate={{ width: `${ratio * 100}%` }}
          transition={{ type: 'spring', stiffness: 100, damping: 20 }}
        />
      </div>
      {lastResult?.reset_after_ms != null && (
        <p className="text-xs text-gray-500">
          Resets in {lastResult.reset_after_ms}ms
        </p>
      )}
    </div>
  )
}

export default function AlgorithmPage() {
  const params = useParams<{ algo: string }>()
  const algo = params?.algo ?? 'token_bucket'
  const label = ALGO_LABELS[algo] ?? algo

  const entry = useAppStore((s) => s.algorithms[algo])
  const setAlgorithmResult = useAppStore((s) => s.setAlgorithmResult)
  const addSimResult = useAppStore((s) => s.addSimResult)

  const [isLoading, setIsLoading] = useState(false)
  const [key, setKey] = useState('demo')
  const [requestCount, setRequestCount] = useState(1)
  const [burstCount, setBurstCount] = useState(5)

  const lastResult = entry?.lastResult ?? null
  const history = entry?.history ?? []

  const sendRequest = useCallback(
    async (n: number = 1) => {
      setIsLoading(true)
      try {
        const result = await allow(algo, key, n)
        setAlgorithmResult(algo, result as RateLimitResult)
        addSimResult({
          timestamp: new Date().toISOString(),
          allowed: (result as RateLimitResult).allowed,
          latency_ms: 0,
          algorithm: algo,
          error: null,
          cb_state: null,
        })
      } catch (err) {
        console.error(err)
      } finally {
        setIsLoading(false)
      }
    },
    [algo, key, setAlgorithmResult, addSimResult],
  )

  const sendBurst = useCallback(async () => {
    for (let i = 0; i < burstCount; i++) {
      await sendRequest(1)
      await new Promise((r) => setTimeout(r, 50))
    }
  }, [burstCount, sendRequest])

  const allowedCount = history.filter((r) => r.allowed).length
  const deniedCount = history.filter((r) => !r.allowed).length

  return (
    <div className="space-y-8">
      {/* Header */}
      <div className="flex items-start justify-between">
        <div>
          <h1 className="text-3xl font-bold text-white">{label}</h1>
          <p className="mt-1 font-mono text-sm text-gray-500">{algo}</p>
        </div>
        {lastResult && (
          <StatusBadge
            state={lastResult.allowed ? 'allowed' : 'denied'}
            className="mt-1"
          />
        )}
      </div>

      <div className="grid gap-6 lg:grid-cols-2">
        {/* Visualization */}
        <Card>
          <CardHeader>
            <CardTitle>Visualization</CardTitle>
          </CardHeader>
          <CardContent>
            <AlgorithmViz algo={algo} lastResult={lastResult} />
          </CardContent>
        </Card>

        {/* Controls */}
        <Card>
          <CardHeader>
            <CardTitle>Controls</CardTitle>
          </CardHeader>
          <CardContent className="space-y-5">
            <div>
              <label className="mb-1 block text-xs font-medium text-gray-400">
                Key
              </label>
              <input
                value={key}
                onChange={(e) => setKey(e.target.value)}
                className="w-full rounded-lg border border-white/10 bg-white/5 px-3 py-2 text-sm text-white placeholder-gray-600 focus:outline-none focus:ring-1 focus:ring-blue-500"
                placeholder="rate limit key"
              />
            </div>

            <div>
              <label className="mb-1 block text-xs font-medium text-gray-400">
                N (tokens requested)
              </label>
              <div className="flex items-center gap-3">
                <input
                  type="range"
                  min={1}
                  max={10}
                  value={requestCount}
                  onChange={(e) => setRequestCount(Number(e.target.value))}
                  className="flex-1 accent-blue-500"
                />
                <span className="w-6 text-center text-sm font-mono text-white">
                  {requestCount}
                </span>
              </div>
            </div>

            <div className="flex gap-3">
              <motion.button
                whileTap={{ scale: 0.96 }}
                disabled={isLoading}
                onClick={() => sendRequest(requestCount)}
                className="flex-1 rounded-lg bg-blue-600 px-4 py-2.5 text-sm font-semibold text-white transition-colors hover:bg-blue-500 disabled:opacity-50"
              >
                {isLoading ? 'Sending...' : 'Send Request'}
              </motion.button>
            </div>

            <div className="border-t border-white/10 pt-4">
              <p className="mb-2 text-xs font-medium text-gray-400">Burst Test</p>
              <div className="flex items-center gap-3">
                <input
                  type="range"
                  min={2}
                  max={20}
                  value={burstCount}
                  onChange={(e) => setBurstCount(Number(e.target.value))}
                  className="flex-1 accent-amber-500"
                />
                <span className="w-8 text-center text-sm font-mono text-white">
                  {burstCount}
                </span>
              </div>
              <motion.button
                whileTap={{ scale: 0.96 }}
                disabled={isLoading}
                onClick={sendBurst}
                className="mt-2 w-full rounded-lg bg-amber-600/20 px-4 py-2.5 text-sm font-semibold text-amber-400 transition-colors hover:bg-amber-600/30 disabled:opacity-50 border border-amber-600/30"
              >
                Send {burstCount} Burst Requests
              </motion.button>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Stats */}
      {lastResult && (
        <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
          <MetricTile label="Limit" value={lastResult.limit} color="blue" />
          <MetricTile
            label="Remaining"
            value={lastResult.remaining}
            color={lastResult.remaining > 0 ? 'green' : 'red'}
          />
          <MetricTile
            label="Allowed"
            value={allowedCount}
            color="green"
          />
          <MetricTile label="Denied" value={deniedCount} color="red" />
        </div>
      )}

      {/* History */}
      <Card>
        <CardHeader>
          <CardTitle>Request History</CardTitle>
          <span className="text-xs text-gray-500">{history.length} requests</span>
        </CardHeader>
        <CardContent>
          {history.length === 0 ? (
            <p className="text-sm text-gray-500">No requests sent yet.</p>
          ) : (
            <div className="max-h-64 space-y-1 overflow-y-auto">
              <AnimatePresence initial={false}>
                {[...history].reverse().map((result, idx) => (
                  <motion.div
                    key={`${result.timestamp}-${idx}`}
                    initial={{ opacity: 0, x: -10 }}
                    animate={{ opacity: 1, x: 0 }}
                    className="flex items-center justify-between rounded-lg bg-white/5 px-3 py-2 text-xs"
                  >
                    <span className="font-mono text-gray-400">
                      {new Date(result.timestamp).toLocaleTimeString()}
                    </span>
                    <StatusBadge state={result.allowed ? 'allowed' : 'denied'} />
                  </motion.div>
                ))}
              </AnimatePresence>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
