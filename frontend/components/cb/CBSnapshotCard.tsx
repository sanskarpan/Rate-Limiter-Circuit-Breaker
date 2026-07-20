'use client'

import { motion } from 'framer-motion'
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/Card'
import { StatusBadge } from '@/components/ui/StatusBadge'
import { MetricTile } from '@/components/ui/MetricTile'
import type { CBSnapshot } from '@/lib/api/types'

interface CBSnapshotCardProps {
  snapshot: CBSnapshot
  onExecute?: (simulate: 'success' | 'failure' | 'timeout') => void
}

const BUTTON_STYLES = {
  success: 'bg-green-600/20 text-green-400 hover:bg-green-600/40 border-green-600/30',
  failure: 'bg-red-600/20 text-red-400 hover:bg-red-600/40 border-red-600/30',
  timeout: 'bg-amber-600/20 text-amber-400 hover:bg-amber-600/40 border-amber-600/30',
}

export function CBSnapshotCard({ snapshot, onExecute }: CBSnapshotCardProps) {
  const { name, state, failures, successes, requests, failure_rate, time_until_half_open_ms } =
    snapshot

  return (
    <Card>
      <CardHeader>
        <CardTitle>{name}</CardTitle>
        <StatusBadge state={state} />
      </CardHeader>
      <CardContent>
        <div className="grid grid-cols-2 gap-3 sm:grid-cols-3">
          <MetricTile label="Requests" value={requests} />
          <MetricTile
            label="Failures"
            value={failures}
            color={failures > 0 ? 'red' : 'green'}
          />
          <MetricTile
            label="Successes"
            value={successes}
            color={successes > 0 ? 'green' : 'default'}
          />
          <MetricTile
            label="Fail Rate"
            value={`${(failure_rate * 100).toFixed(1)}%`}
            color={failure_rate > 0.5 ? 'red' : failure_rate > 0.2 ? 'amber' : 'green'}
          />
          {state === 'open' && time_until_half_open_ms > 0 && (
            <MetricTile
              label="Half-Open In"
              value={`${Math.ceil(time_until_half_open_ms / 1000)}s`}
              color="amber"
            />
          )}
        </div>

        {onExecute && (
          <div
            className="mt-4 flex gap-2"
            role="group"
            aria-label={`Simulate a request on the ${name} circuit breaker`}
          >
            {(['success', 'failure', 'timeout'] as const).map((sim) => (
              <motion.button
                key={sim}
                type="button"
                whileTap={{ scale: 0.95 }}
                onClick={() => onExecute(sim)}
                // Give a longer description via aria-description while keeping the
                // accessible NAME as the visible word ("success"/"failure"/
                // "timeout") — the surrounding role="group" supplies breaker
                // context, and existing e2e selectors match on the visible name.
                aria-description={`Simulate a ${sim} on the ${name} circuit breaker`}
                className={`flex-1 rounded-lg border px-3 py-1.5 text-xs font-medium capitalize transition-colors ${BUTTON_STYLES[sim]}`}
              >
                {sim}
              </motion.button>
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  )
}
