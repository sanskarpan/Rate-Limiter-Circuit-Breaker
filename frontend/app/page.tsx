'use client'

import { useCallback } from 'react'
import { motion } from 'framer-motion'
import { useAppStore } from '@/lib/store'
import { getAllCBSnapshots } from '@/lib/api/client'
import { AlgorithmCard } from '@/components/algorithms/AlgorithmCard'
import { CBSnapshotCard } from '@/components/cb/CBSnapshotCard'
import { usePoll } from '@/hooks/usePoll'

const ALGORITHMS = ['token_bucket', 'sliding_window', 'fixed_window', 'leaky_bucket']

const container = {
  hidden: { opacity: 0 },
  show: { opacity: 1, transition: { staggerChildren: 0.08 } },
}

const item = {
  hidden: { opacity: 0, y: 16 },
  show: { opacity: 1, y: 0 },
}

export default function OverviewPage() {
  const algorithms = useAppStore((s) => s.algorithms)
  const cbSnapshots = useAppStore((s) => s.cbSnapshots)
  const setCBSnapshots = useAppStore((s) => s.setCBSnapshots)

  const fetchCBSnapshots = useCallback(async () => {
    try {
      const snapshots = await getAllCBSnapshots()
      if (Array.isArray(snapshots)) {
        setCBSnapshots(snapshots)
      }
    } catch {
      // backend may not be running in dev
    }
  }, [setCBSnapshots])

  usePoll(fetchCBSnapshots, { interval: 3000 })

  const cbList = Object.values(cbSnapshots)

  return (
    <div className="space-y-10">
      {/* Header */}
      <div>
        <h1 className="text-3xl font-bold text-white">Dashboard</h1>
        <p className="mt-1 text-gray-400">
          Real-time overview of rate limiting algorithms and circuit breakers.
        </p>
      </div>

      {/* Algorithms */}
      <section>
        <h2 className="mb-4 text-lg font-semibold text-gray-300">
          Rate Limiting Algorithms
        </h2>
        <motion.div
          variants={container}
          initial="hidden"
          animate="show"
          className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4"
        >
          {ALGORITHMS.map((algo) => (
            <motion.div key={algo} variants={item}>
              <AlgorithmCard
                algorithm={algo}
                lastResult={algorithms[algo]?.lastResult ?? null}
                requestCount={algorithms[algo]?.history.length ?? 0}
              />
            </motion.div>
          ))}
        </motion.div>
      </section>

      {/* Circuit Breakers */}
      <section>
        <h2 className="mb-4 text-lg font-semibold text-gray-300">
          Circuit Breakers
        </h2>
        {cbList.length === 0 ? (
          <div className="rounded-xl border border-white/10 bg-white/5 p-8 text-center text-gray-500">
            No circuit breaker data. Ensure the backend is running at{' '}
            <code className="text-gray-400">http://localhost:8080</code>.
          </div>
        ) : (
          <motion.div
            variants={container}
            initial="hidden"
            animate="show"
            className="grid gap-4 sm:grid-cols-2"
          >
            {cbList.map((snapshot) => (
              <motion.div key={snapshot.name} variants={item}>
                <CBSnapshotCard snapshot={snapshot} />
              </motion.div>
            ))}
          </motion.div>
        )}
      </section>
    </div>
  )
}
