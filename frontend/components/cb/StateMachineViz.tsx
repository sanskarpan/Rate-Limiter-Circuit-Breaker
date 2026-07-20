'use client'

import { motion } from 'framer-motion'
import type { CBState } from '@/lib/api/types'
import { cn } from '@/lib/utils'

interface StateMachineVizProps {
  state: CBState
}

const STATES: { id: CBState; label: string; x: number; y: number }[] = [
  { id: 'closed', label: 'CLOSED', x: 100, y: 40 },
  { id: 'open', label: 'OPEN', x: 260, y: 40 },
  { id: 'half-open', label: 'HALF OPEN', x: 180, y: 160 },
]

const TRANSITIONS = [
  {
    from: 'closed',
    to: 'open',
    label: 'failures ≥ threshold',
    dx1: 30,
    dy1: -10,
    dx2: -30,
    dy2: -10,
    offset: -20,
  },
  {
    from: 'open',
    to: 'half-open',
    label: 'timeout expires',
    dx1: 10,
    dy1: 30,
    dx2: 10,
    dy2: -30,
    offset: 20,
  },
  {
    from: 'half-open',
    to: 'closed',
    label: 'success',
    dx1: -10,
    dy1: -30,
    dx2: -10,
    dy2: 30,
    offset: -20,
  },
  {
    from: 'half-open',
    to: 'open',
    label: 'failure',
    dx1: 10,
    dy1: -30,
    dx2: -10,
    dy2: 30,
    offset: 20,
  },
]

const STATE_COLORS: Record<CBState, { fill: string; stroke: string; text: string }> = {
  closed: { fill: '#14532d', stroke: '#22c55e', text: '#4ade80' },
  open: { fill: '#7f1d1d', stroke: '#ef4444', text: '#f87171' },
  'half-open': { fill: '#78350f', stroke: '#f59e0b', text: '#fbbf24' },
}

function getNodeCenter(id: CBState) {
  const node = STATES.find((s) => s.id === id)!
  return { x: node.x, y: node.y }
}

export function StateMachineViz({ state }: StateMachineVizProps) {
  return (
    <div className="flex flex-col items-center gap-4">
      <svg
        viewBox="0 0 360 220"
        className="w-full max-w-sm"
        role="img"
        aria-label={`Circuit breaker state machine. Current state: ${state}.`}
      >
        <title>Circuit breaker state machine</title>
        <desc>
          {`States: CLOSED, OPEN and HALF OPEN. The breaker is currently ${state}.`}
        </desc>
        <defs>
          {(['closed', 'open', 'half-open'] as CBState[]).map((id) => (
            <marker
              key={id}
              id={`arrow-${id}`}
              markerWidth="8"
              markerHeight="8"
              refX="6"
              refY="3"
              orient="auto"
            >
              <path d="M0,0 L0,6 L8,3 z" fill={STATE_COLORS[id].stroke} opacity={0.7} />
            </marker>
          ))}
          <marker id="arrow-default" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto">
            <path d="M0,0 L0,6 L8,3 z" fill="#6b7280" opacity={0.7} />
          </marker>
        </defs>

        {/* Edges */}
        {TRANSITIONS.map((t, i) => {
          const from = getNodeCenter(t.from as CBState)
          const to = getNodeCenter(t.to as CBState)
          const mx = (from.x + to.x) / 2 + t.offset
          const my = (from.y + to.y) / 2 + t.offset
          const isActive =
            state === t.from || state === t.to
          const pathColor = isActive ? '#6b7280' : '#374151'

          return (
            <g key={i}>
              <motion.path
                d={`M${from.x},${from.y} Q${mx},${my} ${to.x},${to.y}`}
                fill="none"
                stroke={pathColor}
                strokeWidth={isActive ? 2 : 1.5}
                markerEnd="url(#arrow-default)"
                animate={{ stroke: pathColor }}
                transition={{ duration: 0.3 }}
              />
              <text
                x={mx}
                y={my - 6}
                textAnchor="middle"
                fill="#6b7280"
                fontSize={8}
              >
                {t.label}
              </text>
            </g>
          )
        })}

        {/* Nodes */}
        {STATES.map(({ id, label, x, y }) => {
          const colors = STATE_COLORS[id]
          const isActive = state === id

          return (
            <motion.g key={id}>
              <motion.circle
                cx={x}
                cy={y}
                r={32}
                fill={colors.fill}
                stroke={colors.stroke}
                strokeWidth={isActive ? 3 : 1.5}
                animate={{
                  strokeWidth: isActive ? 3 : 1.5,
                  opacity: isActive ? 1 : 0.5,
                }}
                transition={{ duration: 0.3 }}
              />
              {isActive && (
                <motion.circle
                  cx={x}
                  cy={y}
                  r={38}
                  fill="none"
                  stroke={colors.stroke}
                  strokeWidth={1}
                  opacity={0.4}
                  animate={{ r: [38, 44, 38] }}
                  transition={{ duration: 2, repeat: Infinity, ease: 'easeInOut' }}
                />
              )}
              <text
                x={x}
                y={y + 5}
                textAnchor="middle"
                fill={isActive ? colors.text : '#6b7280'}
                fontSize={9}
                fontWeight={isActive ? 'bold' : 'normal'}
              >
                {label}
              </text>
            </motion.g>
          )
        })}
      </svg>

      {/* Legend */}
      <div className="flex gap-4 text-xs">
        {STATES.map(({ id, label }) => (
          <div key={id} className="flex items-center gap-1.5">
            <span
              className={cn(
                'inline-block h-2.5 w-2.5 rounded-full',
                state === id ? 'ring-2 ring-offset-1 ring-offset-gray-900' : '',
              )}
              style={{ background: STATE_COLORS[id].stroke }}
            />
            <span className={cn('text-gray-500', state === id && 'font-semibold text-white')}>
              {label}
            </span>
          </div>
        ))}
      </div>
    </div>
  )
}
