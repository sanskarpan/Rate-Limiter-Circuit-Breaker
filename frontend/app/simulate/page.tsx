'use client'

import { useState, useCallback, useRef } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { allow } from '@/lib/api/client'
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/Card'
import { AllowDenyRatioChart } from '@/components/charts/AllowDenyRatioChart'
import { LatencyHistogram } from '@/components/charts/LatencyHistogram'
import type { SimResult, SimStats } from '@/lib/api/types'

const ALGORITHMS = [
  { value: 'token_bucket', label: 'Token Bucket' },
  { value: 'sliding_window', label: 'Sliding Window' },
  { value: 'fixed_window', label: 'Fixed Window' },
  { value: 'leaky_bucket', label: 'Leaky Bucket' },
]

const SCENARIOS = [
  { value: 'steady_state', label: 'Steady State', desc: 'Constant request rate' },
  { value: 'burst', label: 'Burst', desc: 'Spike of requests all at once' },
  { value: 'gradual_ramp', label: 'Gradual Ramp', desc: 'Slowly increase from 0 to max RPS' },
  { value: 'thundering_herd', label: 'Thundering Herd', desc: 'Mass concurrent requests' },
]

interface SimConfig {
  algorithm: string
  scenario: string
  durationMs: number
  rps: number
  concurrency: number
  key: string
}

function StatCard({ label, value, color }: { label: string; value: number | string; color: string }) {
  const colorMap: Record<string, string> = {
    blue: 'text-blue-400',
    green: 'text-green-400',
    red: 'text-red-400',
    amber: 'text-amber-400',
    purple: 'text-purple-400',
  }
  return (
    <div className="rounded-lg border border-white/10 bg-white/5 p-4" role="group" aria-label={`${label}: ${value}`}>
      <p className="text-xs font-medium text-gray-400 uppercase tracking-wider">{label}</p>
      <p className={`mt-1 text-2xl font-bold font-mono ${colorMap[color] ?? 'text-white'}`}>{value}</p>
    </div>
  )
}

function RequestDot({ allowed }: { allowed: boolean }) {
  return (
    <motion.div
      initial={{ scale: 0, opacity: 0 }}
      animate={{ scale: 1, opacity: 1 }}
      exit={{ opacity: 0 }}
      className={`h-2 w-2 rounded-full flex-shrink-0 ${allowed ? 'bg-green-400' : 'bg-red-400'}`}
    />
  )
}

export default function SimulatePage() {
  const [config, setConfig] = useState<SimConfig>({
    algorithm: 'token_bucket',
    scenario: 'steady_state',
    durationMs: 5000,
    rps: 20,
    concurrency: 10,
    key: 'sim-demo',
  })
  const [running, setRunning] = useState(false)
  const [results, setResults] = useState<SimResult[]>([])
  const [stats, setStats] = useState<SimStats | null>(null)
  const cancelRef = useRef(false)

  const addResult = useCallback((r: SimResult) => {
    setResults((prev) => [...prev.slice(-199), r])
  }, [])

  const computeStats = useCallback((rs: SimResult[]): SimStats => {
    const allowed = rs.filter((r) => r.allowed).length
    const denied = rs.filter((r) => !r.allowed).length
    const latencies = rs.map((r) => r.latency_ms).sort((a, b) => a - b)
    const p = (pct: number) => latencies[Math.floor(latencies.length * pct)] ?? 0
    return {
      total: rs.length,
      allowed,
      denied,
      errors: rs.filter((r) => r.error !== null).length,
      p50_ms: p(0.5),
      p95_ms: p(0.95),
      p99_ms: p(0.99),
      rps: rs.length / (config.durationMs / 1000),
    }
  }, [config.durationMs])

  const runSteadyState = useCallback(async (collected: SimResult[]) => {
    const intervalMs = 1000 / config.rps
    const endTime = Date.now() + config.durationMs
    while (Date.now() < endTime && !cancelRef.current) {
      const t0 = Date.now()
      try {
        const res = await allow(config.algorithm, config.key, 1)
        const r: SimResult = {
          timestamp: new Date().toISOString(),
          allowed: res.allowed,
          latency_ms: Date.now() - t0,
          algorithm: config.algorithm,
          error: null,
          cb_state: null,
        }
        collected.push(r)
        addResult(r)
      } catch (e) {
        const r: SimResult = {
          timestamp: new Date().toISOString(),
          allowed: false,
          latency_ms: Date.now() - t0,
          algorithm: config.algorithm,
          error: String(e),
          cb_state: null,
        }
        collected.push(r)
        addResult(r)
      }
      await new Promise((res) => setTimeout(res, Math.max(0, intervalMs - (Date.now() - t0))))
    }
  }, [config, addResult])

  const runBurst = useCallback(async (collected: SimResult[]) => {
    const count = Math.round(config.rps * (config.durationMs / 1000))
    const promises = Array.from({ length: count }, async () => {
      const t0 = Date.now()
      try {
        const res = await allow(config.algorithm, config.key, 1)
        const r: SimResult = {
          timestamp: new Date().toISOString(),
          allowed: res.allowed,
          latency_ms: Date.now() - t0,
          algorithm: config.algorithm,
          error: null,
          cb_state: null,
        }
        collected.push(r)
        addResult(r)
      } catch (e) {
        const r: SimResult = {
          timestamp: new Date().toISOString(),
          allowed: false,
          latency_ms: Date.now() - t0,
          algorithm: config.algorithm,
          error: String(e),
          cb_state: null,
        }
        collected.push(r)
        addResult(r)
      }
    })
    await Promise.all(promises)
  }, [config, addResult])

  const runThunderingHerd = useCallback(async (collected: SimResult[]) => {
    const waves = Math.ceil(config.durationMs / 1000)
    for (let w = 0; w < waves && !cancelRef.current; w++) {
      const concurrentRequests = Array.from({ length: config.concurrency }, async () => {
        const t0 = Date.now()
        try {
          const res = await allow(config.algorithm, config.key, 1)
          const r: SimResult = {
            timestamp: new Date().toISOString(),
            allowed: res.allowed,
            latency_ms: Date.now() - t0,
            algorithm: config.algorithm,
            error: null,
            cb_state: null,
          }
          collected.push(r)
          addResult(r)
        } catch (e) {
          const r: SimResult = {
            timestamp: new Date().toISOString(),
            allowed: false,
            latency_ms: Date.now() - t0,
            algorithm: config.algorithm,
            error: String(e),
            cb_state: null,
          }
          collected.push(r)
          addResult(r)
        }
      })
      await Promise.all(concurrentRequests)
      await new Promise((res) => setTimeout(res, 1000))
    }
  }, [config, addResult])

  const runGradualRamp = useCallback(async (collected: SimResult[]) => {
    const steps = 10
    const stepDuration = config.durationMs / steps
    for (let step = 1; step <= steps && !cancelRef.current; step++) {
      const stepRps = (config.rps * step) / steps
      const intervalMs = 1000 / stepRps
      const stepEnd = Date.now() + stepDuration
      while (Date.now() < stepEnd && !cancelRef.current) {
        const t0 = Date.now()
        try {
          const res = await allow(config.algorithm, config.key, 1)
          const r: SimResult = {
            timestamp: new Date().toISOString(),
            allowed: res.allowed,
            latency_ms: Date.now() - t0,
            algorithm: config.algorithm,
            error: null,
            cb_state: null,
          }
          collected.push(r)
          addResult(r)
        } catch (e) {
          const r: SimResult = {
            timestamp: new Date().toISOString(),
            allowed: false,
            latency_ms: Date.now() - t0,
            algorithm: config.algorithm,
            error: String(e),
            cb_state: null,
          }
          collected.push(r)
          addResult(r)
        }
        await new Promise((res) => setTimeout(res, Math.max(0, intervalMs - (Date.now() - t0))))
      }
    }
  }, [config, addResult])

  const startSimulation = useCallback(async () => {
    setRunning(true)
    setResults([])
    setStats(null)
    cancelRef.current = false
    const collected: SimResult[] = []

    try {
      switch (config.scenario) {
        case 'burst':
          await runBurst(collected)
          break
        case 'thundering_herd':
          await runThunderingHerd(collected)
          break
        case 'gradual_ramp':
          await runGradualRamp(collected)
          break
        default:
          await runSteadyState(collected)
      }
    } finally {
      setStats(computeStats(collected))
      setRunning(false)
    }
  }, [config, runBurst, runThunderingHerd, runGradualRamp, runSteadyState, computeStats])

  const stopSimulation = useCallback(() => {
    cancelRef.current = true
  }, [])

  const allowedCount = results.filter((r) => r.allowed).length
  const deniedCount = results.filter((r) => !r.allowed).length
  const allowRate = results.length > 0 ? ((allowedCount / results.length) * 100).toFixed(1) : '—'
  const latencies = results.map((r) => r.latency_ms)

  return (
    <div className="space-y-8">
      <div>
        <h1 className="text-3xl font-bold text-white">Load Simulator</h1>
        <p className="mt-1 text-sm text-gray-400">
          Run configurable load scenarios against any rate limiter algorithm
        </p>
      </div>

      <div className="grid gap-6 lg:grid-cols-3">
        {/* Config panel */}
        <Card className="lg:col-span-1">
          <CardHeader>
            <CardTitle>Configuration</CardTitle>
          </CardHeader>
          <CardContent className="space-y-5">
            <div>
              <label htmlFor="sim-algorithm" className="mb-1 block text-xs font-medium text-gray-300">Algorithm</label>
              <select
                id="sim-algorithm"
                value={config.algorithm}
                onChange={(e) => setConfig((c) => ({ ...c, algorithm: e.target.value }))}
                disabled={running}
                className="w-full rounded-lg border border-white/10 bg-gray-900 px-3 py-2 text-sm text-white focus:outline-none focus:ring-1 focus:ring-blue-500 disabled:opacity-50"
              >
                {ALGORITHMS.map((a) => (
                  <option key={a.value} value={a.value}>{a.label}</option>
                ))}
              </select>
            </div>

            <div>
              <label htmlFor="sim-scenario" className="mb-1 block text-xs font-medium text-gray-300">Scenario</label>
              <select
                id="sim-scenario"
                value={config.scenario}
                onChange={(e) => setConfig((c) => ({ ...c, scenario: e.target.value }))}
                disabled={running}
                className="w-full rounded-lg border border-white/10 bg-gray-900 px-3 py-2 text-sm text-white focus:outline-none focus:ring-1 focus:ring-blue-500 disabled:opacity-50"
              >
                {SCENARIOS.map((s) => (
                  <option key={s.value} value={s.value}>{s.label}</option>
                ))}
              </select>
              <p className="mt-1 text-xs text-gray-500">
                {SCENARIOS.find((s) => s.value === config.scenario)?.desc}
              </p>
            </div>

            <div>
              <label htmlFor="sim-duration" className="mb-1 flex justify-between text-xs font-medium text-gray-300">
                <span>Duration</span>
                <span className="font-mono text-white">{config.durationMs / 1000}s</span>
              </label>
              <input
                id="sim-duration"
                type="range" min={1000} max={30000} step={1000}
                value={config.durationMs}
                aria-valuetext={`${config.durationMs / 1000} seconds`}
                onChange={(e) => setConfig((c) => ({ ...c, durationMs: Number(e.target.value) }))}
                disabled={running}
                className="w-full accent-blue-500"
              />
            </div>

            <div>
              <label htmlFor="sim-rps" className="mb-1 flex justify-between text-xs font-medium text-gray-300">
                <span>Target RPS</span>
                <span className="font-mono text-white">{config.rps}</span>
              </label>
              <input
                id="sim-rps"
                type="range" min={1} max={100} step={1}
                value={config.rps}
                aria-valuetext={`${config.rps} requests per second`}
                onChange={(e) => setConfig((c) => ({ ...c, rps: Number(e.target.value) }))}
                disabled={running}
                className="w-full accent-blue-500"
              />
            </div>

            <div>
              <label htmlFor="sim-concurrency" className="mb-1 flex justify-between text-xs font-medium text-gray-300">
                <span>Concurrency</span>
                <span className="font-mono text-white">{config.concurrency}</span>
              </label>
              <input
                id="sim-concurrency"
                type="range" min={1} max={100} step={1}
                value={config.concurrency}
                aria-valuetext={`${config.concurrency} concurrent requests`}
                onChange={(e) => setConfig((c) => ({ ...c, concurrency: Number(e.target.value) }))}
                disabled={running}
                className="w-full accent-purple-500"
              />
            </div>

            <div>
              <label htmlFor="sim-key" className="mb-1 block text-xs font-medium text-gray-300">Key</label>
              <input
                id="sim-key"
                value={config.key}
                onChange={(e) => setConfig((c) => ({ ...c, key: e.target.value }))}
                disabled={running}
                className="w-full rounded-lg border border-white/10 bg-white/5 px-3 py-2 text-sm text-white focus:outline-none focus:ring-1 focus:ring-blue-500 disabled:opacity-50"
              />
            </div>

            {!running ? (
              <motion.button
                type="button"
                whileTap={{ scale: 0.97 }}
                onClick={startSimulation}
                className="w-full rounded-lg bg-blue-600 px-4 py-2.5 text-sm font-semibold text-white transition-colors hover:bg-blue-500"
              >
                Start Simulation
              </motion.button>
            ) : (
              <motion.button
                type="button"
                whileTap={{ scale: 0.97 }}
                onClick={stopSimulation}
                className="w-full rounded-lg bg-red-600 px-4 py-2.5 text-sm font-semibold text-white transition-colors hover:bg-red-500"
              >
                Stop
              </motion.button>
            )}
          </CardContent>
        </Card>

        {/* Live metrics */}
        <div className="lg:col-span-2 space-y-6">
          {/* Running indicator */}
          {running && (
            <div
              className="flex items-center gap-3 rounded-lg border border-blue-500/30 bg-blue-500/10 px-4 py-3"
              role="status"
              aria-live="polite"
            >
              <span className="h-2 w-2 animate-pulse rounded-full bg-blue-400" aria-hidden="true" />
              <span className="text-sm text-blue-300">
                Simulation running — {results.length} requests sent
              </span>
            </div>
          )}

          {/* Stats grid — live so screen readers hear the streaming totals */}
          <div className="grid grid-cols-2 gap-4 sm:grid-cols-4" aria-live="polite">
            <StatCard label="Total" value={results.length} color="blue" />
            <StatCard label="Allowed" value={allowedCount} color="green" />
            <StatCard label="Denied" value={deniedCount} color="red" />
            <StatCard label="Allow Rate" value={`${allowRate}%`} color="amber" />
          </div>

          {/* Rolling allow/deny ratio */}
          <Card>
            <CardHeader>
              <CardTitle>Rolling Allow Rate</CardTitle>
              <span className="text-xs text-gray-400">last 20-request window</span>
            </CardHeader>
            <CardContent>
              <AllowDenyRatioChart results={results} />
            </CardContent>
          </Card>

          {/* Latency distribution */}
          <Card>
            <CardHeader>
              <CardTitle>Latency Distribution</CardTitle>
              <span className="text-xs text-gray-400">{results.length} samples</span>
            </CardHeader>
            <CardContent>
              <LatencyHistogram latencies={latencies} />
            </CardContent>
          </Card>

          {/* Final stats */}
          {stats && !running && (
            <Card>
              <CardHeader>
                <CardTitle>Results Summary</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-3 gap-4">
                  <div className="text-center">
                    <p className="text-xs text-gray-500">P50 Latency</p>
                    <p className="font-mono text-lg font-bold text-white">{stats.p50_ms}ms</p>
                  </div>
                  <div className="text-center">
                    <p className="text-xs text-gray-500">P95 Latency</p>
                    <p className="font-mono text-lg font-bold text-white">{stats.p95_ms}ms</p>
                  </div>
                  <div className="text-center">
                    <p className="text-xs text-gray-500">P99 Latency</p>
                    <p className="font-mono text-lg font-bold text-amber-400">{stats.p99_ms}ms</p>
                  </div>
                </div>
              </CardContent>
            </Card>
          )}

          {/* Request timeline dots */}
          <Card>
            <CardHeader>
              <CardTitle>Request Stream</CardTitle>
              <span className="text-xs text-gray-500">
                <span className="inline-block h-2 w-2 rounded-full bg-green-400 mr-1" />allowed
                <span className="inline-block h-2 w-2 rounded-full bg-red-400 ml-3 mr-1" />denied
              </span>
            </CardHeader>
            <CardContent>
              {results.length === 0 ? (
                <p className="text-sm text-gray-500">No data yet — start a simulation.</p>
              ) : (
                <div className="flex flex-wrap gap-1 max-h-40 overflow-y-auto">
                  <AnimatePresence initial={false}>
                    {results.slice(-200).map((r, i) => (
                      <RequestDot key={`${r.timestamp}-${i}`} allowed={r.allowed} />
                    ))}
                  </AnimatePresence>
                </div>
              )}
            </CardContent>
          </Card>

          {/* Recent log */}
          <Card>
            <CardHeader>
              <CardTitle>Recent Events</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="max-h-48 space-y-1 overflow-y-auto">
                {results.length === 0 ? (
                  <p className="text-sm text-gray-500">Events will appear here.</p>
                ) : (
                  <AnimatePresence initial={false}>
                    {[...results].reverse().slice(0, 30).map((r, i) => (
                      <motion.div
                        key={`${r.timestamp}-${i}`}
                        initial={{ opacity: 0, x: -8 }}
                        animate={{ opacity: 1, x: 0 }}
                        className="flex items-center justify-between rounded bg-white/5 px-3 py-1.5 text-xs"
                      >
                        <span className="font-mono text-gray-500">
                          {new Date(r.timestamp).toLocaleTimeString()}
                        </span>
                        <span className="font-mono text-gray-400">{r.algorithm}</span>
                        <span className={`font-semibold ${r.allowed ? 'text-green-400' : 'text-red-400'}`}>
                          {r.allowed ? 'ALLOWED' : 'DENIED'}
                        </span>
                        <span className="font-mono text-gray-500">{r.latency_ms}ms</span>
                      </motion.div>
                    ))}
                  </AnimatePresence>
                )}
              </div>
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  )
}
