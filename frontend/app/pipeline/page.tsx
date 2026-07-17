'use client'

import { useState, useCallback } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { allow } from '@/lib/api/client'
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/Card'
import type { RateLimitResult } from '@/lib/api/types'

interface Stage {
  id: string
  name: string
  type: 'rate_limit' | 'circuit_breaker' | 'timeout' | 'retry' | 'bulkhead'
  enabled: boolean
  config: Record<string, number | string>
  description: string
  icon: string
}

const DEFAULT_STAGES: Stage[] = [
  {
    id: 'rate_limit',
    name: 'Rate Limiter',
    type: 'rate_limit',
    enabled: true,
    icon: '⏱',
    description: 'Reject requests exceeding the configured rate. First stage — saves bulkhead slots.',
    config: { algorithm: 'token_bucket', rps: 10, key: 'pipeline-demo' },
  },
  {
    id: 'bulkhead',
    name: 'Bulkhead',
    type: 'bulkhead',
    enabled: true,
    icon: '🚧',
    description: 'Limit concurrent requests. Prevents thread pool exhaustion.',
    config: { maxConcurrency: 20, maxWaitMs: 50 },
  },
  {
    id: 'timeout',
    name: 'Timeout',
    type: 'timeout',
    enabled: true,
    icon: '⌛',
    description: 'Cancel requests that exceed the deadline. Counts as CB failure.',
    config: { timeoutMs: 5000 },
  },
  {
    id: 'circuit_breaker',
    name: 'Circuit Breaker',
    type: 'circuit_breaker',
    enabled: true,
    icon: '🔌',
    description: 'Open circuit after threshold failures. Prevents cascade failure.',
    config: { failureThreshold: 5, openTimeoutMs: 10000 },
  },
  {
    id: 'retry',
    name: 'Retry',
    type: 'retry',
    enabled: false,
    icon: '🔄',
    description: 'Retry transient failures with backoff. Innermost stage — retries the real call only.',
    config: { maxAttempts: 3, backoffMs: 100, backoffStrategy: 'exponential' },
  },
]

interface RequestResult {
  timestamp: string
  passed: boolean
  blockedAt: string | null
  latencyMs: number
}

function StagePill({ stage, active }: { stage: Stage; active: boolean }) {
  return (
    <div className={`flex items-center gap-1 rounded-full px-3 py-1 text-xs font-medium transition-all ${
      active
        ? stage.enabled
          ? 'bg-green-500/20 text-green-400 border border-green-500/30'
          : 'bg-gray-800 text-gray-500 border border-white/10 line-through'
        : 'bg-white/5 text-gray-400 border border-white/10'
    }`}>
      <span>{stage.icon}</span>
      <span>{stage.name}</span>
    </div>
  )
}

export default function PipelinePage() {
  const [stages, setStages] = useState<Stage[]>(DEFAULT_STAGES)
  const [results, setResults] = useState<RequestResult[]>([])
  const [loading, setLoading] = useState(false)
  const [draggedIdx, setDraggedIdx] = useState<number | null>(null)

  const toggleStage = useCallback((id: string) => {
    setStages((prev) =>
      prev.map((s) => (s.id === id ? { ...s, enabled: !s.enabled } : s)),
    )
  }, [])

  const moveStage = useCallback((fromIdx: number, toIdx: number) => {
    setStages((prev) => {
      const next = [...prev]
      const [moved] = next.splice(fromIdx, 1)
      next.splice(toIdx, 0, moved)
      return next
    })
  }, [])

  const sendRequest = useCallback(async () => {
    setLoading(true)
    const t0 = Date.now()

    // Simulate pipeline execution client-side
    const enabledStages = stages.filter((s) => s.enabled)

    // Rate limit stage — use real API
    const rateLimitStage = enabledStages.find((s) => s.type === 'rate_limit')
    let passed = true
    let blockedAt: string | null = null

    if (rateLimitStage) {
      try {
        const algo = String(rateLimitStage.config.algorithm)
        const key = String(rateLimitStage.config.key)
        const result = await allow(algo, key, 1) as RateLimitResult
        if (!result.allowed) {
          passed = false
          blockedAt = rateLimitStage.name
        }
      } catch {
        passed = false
        blockedAt = rateLimitStage.name + ' (error)'
      }
    }

    // Simulate other stages (bulkhead, timeout, CB, retry) — demo purposes
    if (passed) {
      const cbStage = enabledStages.find((s) => s.type === 'circuit_breaker')
      if (cbStage) {
        // Simulate 10% failure rate for demo
        if (Math.random() < 0.1) {
          passed = false
          blockedAt = cbStage.name + ' (open)'
        }
      }
    }

    const latencyMs = Date.now() - t0
    const result: RequestResult = {
      timestamp: new Date().toISOString(),
      passed,
      blockedAt,
      latencyMs,
    }

    setResults((prev) => [...prev.slice(-99), result])
    setLoading(false)
  }, [stages])

  const allowedCount = results.filter((r) => r.passed).length
  const deniedCount = results.filter((r) => !r.passed).length

  return (
    <div className="space-y-8">
      <div>
        <h1 className="text-3xl font-bold text-white">Pipeline Builder</h1>
        <p className="mt-1 text-sm text-gray-400">
          Compose resilience patterns in the correct order. Toggle stages on/off, reorder with drag handles.
        </p>
      </div>

      {/* Stage order info */}
      <Card>
        <CardHeader>
          <CardTitle>Stage Ordering (fixed best-practice order)</CardTitle>
        </CardHeader>
        <CardContent>
          <p className="mb-4 text-xs text-gray-500">
            The order Rate→Bulkhead→Timeout→CB→Retry is the correct production order.
            Drag to reorder, toggle to enable/disable.
          </p>

          {/* Pipeline visualization */}
          <div className="flex items-center gap-2 flex-wrap mb-6">
            {stages.map((stage, idx) => (
              <div key={stage.id} className="flex items-center gap-2">
                <StagePill stage={stage} active={true} />
                {idx < stages.length - 1 && (
                  <span className="text-gray-600">→</span>
                )}
              </div>
            ))}
          </div>

          {/* Stage cards */}
          <div className="space-y-3">
            {stages.map((stage, idx) => (
              <motion.div
                key={stage.id}
                layout
                draggable
                onDragStart={() => setDraggedIdx(idx)}
                onDragOver={(e) => e.preventDefault()}
                onDrop={() => {
                  if (draggedIdx !== null && draggedIdx !== idx) {
                    moveStage(draggedIdx, idx)
                  }
                  setDraggedIdx(null)
                }}
                className={`flex items-start gap-4 rounded-lg border p-4 cursor-grab active:cursor-grabbing transition-all ${
                  stage.enabled
                    ? 'border-white/10 bg-white/5'
                    : 'border-white/5 bg-white/2 opacity-50'
                } ${draggedIdx === idx ? 'opacity-30' : ''}`}
              >
                <span className="text-2xl">{stage.icon}</span>
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2 mb-1">
                    <span className={`font-semibold text-sm ${stage.enabled ? 'text-white' : 'text-gray-500 line-through'}`}>
                      {stage.name}
                    </span>
                    <span className="text-xs text-gray-600 bg-gray-800 rounded px-1.5 py-0.5">
                      Stage {idx + 1}
                    </span>
                  </div>
                  <p className="text-xs text-gray-500">{stage.description}</p>
                  <div className="mt-2 flex flex-wrap gap-2">
                    {Object.entries(stage.config).map(([k, v]) => (
                      <span key={k} className="rounded bg-white/5 px-2 py-0.5 text-xs font-mono text-gray-400">
                        {k}={String(v)}
                      </span>
                    ))}
                  </div>
                </div>
                <button
                  onClick={() => toggleStage(stage.id)}
                  className={`flex-shrink-0 rounded-lg px-3 py-1.5 text-xs font-medium transition-colors ${
                    stage.enabled
                      ? 'bg-green-500/20 text-green-400 hover:bg-red-500/20 hover:text-red-400'
                      : 'bg-gray-700 text-gray-400 hover:bg-green-500/20 hover:text-green-400'
                  }`}
                >
                  {stage.enabled ? 'ON' : 'OFF'}
                </button>
              </motion.div>
            ))}
          </div>
        </CardContent>
      </Card>

      {/* Execute */}
      <div className="flex gap-4 items-center">
        <motion.button
          whileTap={{ scale: 0.97 }}
          onClick={sendRequest}
          disabled={loading}
          className="rounded-lg bg-blue-600 px-6 py-2.5 text-sm font-semibold text-white transition-colors hover:bg-blue-500 disabled:opacity-50"
        >
          {loading ? 'Executing...' : 'Send Request Through Pipeline'}
        </motion.button>
        <div className="flex gap-4 text-sm">
          <span className="text-green-400">{allowedCount} passed</span>
          <span className="text-red-400">{deniedCount} blocked</span>
        </div>
      </div>

      {/* Funnel chart */}
      {results.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle>Request Funnel</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-3">
              {stages.filter((s) => s.enabled).map((stage) => {
                const blockedHere = results.filter((r) => r.blockedAt?.startsWith(stage.name)).length
                const passedThrough = results.length - results.filter((r) => {
                  const idx = stages.findIndex((s) => s.id === stage.id)
                  const priorBlocked = stages.slice(0, idx).filter((s) => s.enabled)
                  return priorBlocked.some((s) => r.blockedAt?.startsWith(s.name))
                }).length
                const pct = results.length > 0 ? (passedThrough / results.length) * 100 : 100

                return (
                  <div key={stage.id}>
                    <div className="mb-1 flex justify-between text-xs text-gray-400">
                      <span>{stage.icon} {stage.name}</span>
                      <span className="font-mono">{passedThrough}/{results.length} passed ({pct.toFixed(0)}%)</span>
                    </div>
                    <div className="h-3 overflow-hidden rounded-full bg-gray-800">
                      <motion.div
                        className="h-full rounded-full bg-blue-500"
                        animate={{ width: `${pct}%` }}
                        transition={{ type: 'spring', stiffness: 100 }}
                      />
                    </div>
                    {blockedHere > 0 && (
                      <p className="mt-0.5 text-xs text-red-400">{blockedHere} blocked here</p>
                    )}
                  </div>
                )
              })}
            </div>
          </CardContent>
        </Card>
      )}

      {/* Result log */}
      <Card>
        <CardHeader>
          <CardTitle>Request Log</CardTitle>
        </CardHeader>
        <CardContent>
          {results.length === 0 ? (
            <p className="text-sm text-gray-500">No requests yet — click &ldquo;Send Request Through Pipeline&rdquo;.</p>
          ) : (
            <div className="max-h-64 space-y-1 overflow-y-auto">
              <AnimatePresence initial={false}>
                {[...results].reverse().map((r, i) => (
                  <motion.div
                    key={`${r.timestamp}-${i}`}
                    initial={{ opacity: 0, x: -8 }}
                    animate={{ opacity: 1, x: 0 }}
                    className="flex items-center justify-between rounded bg-white/5 px-3 py-2 text-xs"
                  >
                    <span className="font-mono text-gray-500">
                      {new Date(r.timestamp).toLocaleTimeString()}
                    </span>
                    {r.passed ? (
                      <span className="text-green-400 font-semibold">✓ PASSED</span>
                    ) : (
                      <span className="text-red-400 font-semibold">✗ {r.blockedAt}</span>
                    )}
                    <span className="font-mono text-gray-500">{r.latencyMs}ms</span>
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
