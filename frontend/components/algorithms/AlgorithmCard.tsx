'use client'

import Link from 'next/link'
import { motion } from 'framer-motion'
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/Card'
import { StatusBadge } from '@/components/ui/StatusBadge'
import { MetricTile } from '@/components/ui/MetricTile'
import type { RateLimitResult } from '@/lib/api/types'
import { ALGO_LABELS } from '@/lib/store'

interface AlgorithmCardProps {
  algorithm: string
  lastResult: RateLimitResult | null
  requestCount?: number
}

export function AlgorithmCard({ algorithm, lastResult, requestCount }: AlgorithmCardProps) {
  const label = ALGO_LABELS[algorithm] ?? algorithm

  return (
    <Link href={`/algorithms/${algorithm}`} className="block">
      <motion.div
        whileHover={{ scale: 1.02 }}
        whileTap={{ scale: 0.98 }}
        transition={{ type: 'spring', stiffness: 300, damping: 25 }}
      >
        <Card className="cursor-pointer transition-colors hover:border-blue-500/30">
          <CardHeader>
            <CardTitle>{label}</CardTitle>
            {lastResult && (
              <StatusBadge state={lastResult.allowed ? 'allowed' : 'denied'} />
            )}
          </CardHeader>
          <CardContent>
            {lastResult ? (
              <div className="grid grid-cols-2 gap-3">
                <MetricTile
                  label="Remaining"
                  value={lastResult.remaining}
                  color={lastResult.remaining > 0 ? 'green' : 'red'}
                />
                <MetricTile
                  label="Limit"
                  value={lastResult.limit}
                  color="blue"
                />
                <MetricTile
                  label="Reset"
                  value={`${lastResult.reset_after_ms}ms`}
                  color="amber"
                />
                <MetricTile
                  label="Requests"
                  value={requestCount ?? 0}
                />
              </div>
            ) : (
              <p className="text-sm text-gray-500">No requests yet. Click to explore.</p>
            )}
          </CardContent>
        </Card>
      </motion.div>
    </Link>
  )
}
