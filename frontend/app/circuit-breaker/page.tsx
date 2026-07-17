'use client'

import { useCallback, useState } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { useAppStore } from '@/lib/store'
import { getAllCBSnapshots, executeCB } from '@/lib/api/client'
import { usePoll } from '@/hooks/usePoll'
import { StateMachineViz } from '@/components/cb/StateMachineViz'
import { CBSnapshotCard } from '@/components/cb/CBSnapshotCard'
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/Card'
import { MetricTile } from '@/components/ui/MetricTile'
import type { CBState } from '@/lib/api/types'

interface ExecEntry {
  id: number
  name: string
  simulate: string
  state: CBState | null
  timestamp: string
}

let execId = 0

const EXPLANATION: Record<CBState, string> = {
  closed:
    'CLOSED: The circuit is operating normally. Requests pass through. Failures are counted, and if they exceed the threshold the circuit opens.',
  open:
    'OPEN: The circuit is open — requests are rejected immediately to protect the downstream service. After a timeout, the circuit transitions to HALF OPEN.',
  'half-open':
    'HALF OPEN: A single probe request is allowed through. If it succeeds, the circuit closes. If it fails, the circuit reopens.',
}

export default function CircuitBreakerPage() {
  const cbSnapshots = useAppStore((s) => s.cbSnapshots)
  const setCBSnapshots = useAppStore((s) => s.setCBSnapshots)
  const setCBSnapshot = useAppStore((s) => s.setCBSnapshot)

  const [execLog, setExecLog] = useState<ExecEntry[]>([])
  const [selectedCB, setSelectedCB] = useState<string | null>(null)

  const fetchSnapshots = useCallback(async () => {
    try {
      const snapshots = await getAllCBSnapshots()
      if (Array.isArray(snapshots)) {
        setCBSnapshots(snapshots)
        if (!selectedCB && snapshots.length > 0) {
          setSelectedCB(snapshots[0].name as string)
        }
      }
    } catch {
      // backend may not be running
    }
  }, [setCBSnapshots, selectedCB])

  usePoll(fetchSnapshots, { interval: 2000 })

  const handleExecute = useCallback(
    async (name: string, simulate: 'success' | 'failure' | 'timeout') => {
      try {
        const result = await executeCB(name, simulate)
        // Refresh snapshot after execute
        const snapshots = await getAllCBSnapshots()
        if (Array.isArray(snapshots)) {
          setCBSnapshots(snapshots)
          const updated = snapshots.find((s: { name: string }) => s.name === name)
          if (updated) {
            setCBSnapshot(updated)
          }
        }
        setExecLog((log) =>
          [
            {
              id: ++execId,
              name,
              simulate,
              state: result.state,
              timestamp: new Date().toLocaleTimeString(),
            },
            ...log,
          ].slice(0, 50),
        )
      } catch (err) {
        console.error(err)
      }
    },
    [setCBSnapshots, setCBSnapshot],
  )

  const cbList = Object.values(cbSnapshots)
  const selectedSnapshot = selectedCB ? cbSnapshots[selectedCB] : cbList[0]

  return (
    <div className="space-y-8">
      {/* Header */}
      <div>
        <h1 className="text-3xl font-bold text-white">Circuit Breaker</h1>
        <p className="mt-1 text-gray-400">
          Visualize state transitions and test failure handling behavior.
        </p>
      </div>

      {/* Main layout */}
      <div className="grid gap-6 lg:grid-cols-3">
        {/* State machine */}
        <Card className="lg:col-span-1">
          <CardHeader>
            <CardTitle>State Machine</CardTitle>
          </CardHeader>
          <CardContent>
            {selectedSnapshot ? (
              <>
                <StateMachineViz state={selectedSnapshot.state} />
                <div className="mt-4 rounded-lg bg-white/5 p-3 text-xs text-gray-400">
                  {EXPLANATION[selectedSnapshot.state]}
                </div>
              </>
            ) : (
              <p className="text-sm text-gray-500">No circuit breakers found.</p>
            )}
          </CardContent>
        </Card>

        {/* CB list + controls */}
        <div className="space-y-4 lg:col-span-2">
          {cbList.length === 0 ? (
            <Card>
              <CardContent>
                <p className="py-6 text-center text-gray-500">
                  No circuit breaker data. Ensure the backend is running at{' '}
                  <code className="text-gray-400">http://localhost:8080</code>.
                </p>
              </CardContent>
            </Card>
          ) : (
            <>
              {/* CB selector tabs */}
              <div className="flex gap-2">
                {cbList.map((cb) => (
                  <button
                    key={cb.name}
                    onClick={() => setSelectedCB(cb.name)}
                    className={`rounded-lg border px-3 py-1.5 text-sm font-medium transition-colors ${
                      selectedCB === cb.name
                        ? 'border-blue-500/50 bg-blue-600/20 text-blue-400'
                        : 'border-white/10 bg-white/5 text-gray-400 hover:bg-white/10'
                    }`}
                  >
                    {cb.name}
                  </button>
                ))}
              </div>

              {selectedSnapshot && (
                <CBSnapshotCard
                  snapshot={selectedSnapshot}
                  onExecute={(sim) => handleExecute(selectedSnapshot.name, sim)}
                />
              )}

              {/* Stats grid */}
              {selectedSnapshot && (
                <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
                  <MetricTile
                    label="Total Requests"
                    value={selectedSnapshot.requests}
                  />
                  <MetricTile
                    label="Failures"
                    value={selectedSnapshot.failures}
                    color={selectedSnapshot.failures > 0 ? 'red' : 'green'}
                  />
                  <MetricTile
                    label="Successes"
                    value={selectedSnapshot.successes}
                    color="green"
                  />
                  <MetricTile
                    label="Failure Rate"
                    value={`${(selectedSnapshot.failure_rate * 100).toFixed(1)}%`}
                    color={
                      selectedSnapshot.failure_rate > 0.5
                        ? 'red'
                        : selectedSnapshot.failure_rate > 0.2
                          ? 'amber'
                          : 'green'
                    }
                  />
                </div>
              )}
            </>
          )}
        </div>
      </div>

      {/* Execution log */}
      <Card>
        <CardHeader>
          <CardTitle>Execution Log</CardTitle>
          <span className="text-xs text-gray-500">{execLog.length} entries</span>
        </CardHeader>
        <CardContent>
          {execLog.length === 0 ? (
            <p className="text-sm text-gray-500">
              No executions yet. Use the controls above to test the circuit breaker.
            </p>
          ) : (
            <div className="max-h-48 space-y-1 overflow-y-auto">
              <AnimatePresence initial={false}>
                {execLog.map((entry) => (
                  <motion.div
                    key={entry.id}
                    initial={{ opacity: 0, x: -8 }}
                    animate={{ opacity: 1, x: 0 }}
                    className="flex items-center justify-between rounded-lg bg-white/5 px-3 py-2 text-xs"
                  >
                    <span className="font-mono text-gray-500">{entry.timestamp}</span>
                    <span className="font-medium text-gray-300">{entry.name}</span>
                    <span
                      className={
                        entry.simulate === 'success'
                          ? 'text-green-400'
                          : entry.simulate === 'failure'
                            ? 'text-red-400'
                            : 'text-amber-400'
                      }
                    >
                      {entry.simulate}
                    </span>
                    {entry.state && (
                      <span className="font-mono text-gray-400">{entry.state}</span>
                    )}
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
