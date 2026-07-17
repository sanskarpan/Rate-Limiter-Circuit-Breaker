import { cn } from '@/lib/utils'
import type { CBState } from '@/lib/api/types'

interface StatusBadgeProps {
  state: CBState | 'allowed' | 'denied' | 'unknown'
  className?: string
}

const STATE_STYLES: Record<string, string> = {
  closed: 'bg-green-500/20 text-green-400 border-green-500/30',
  'half-open': 'bg-amber-500/20 text-amber-400 border-amber-500/30',
  open: 'bg-red-500/20 text-red-400 border-red-500/30',
  allowed: 'bg-blue-500/20 text-blue-400 border-blue-500/30',
  denied: 'bg-orange-500/20 text-orange-400 border-orange-500/30',
  unknown: 'bg-gray-500/20 text-gray-400 border-gray-500/30',
}

const STATE_LABELS: Record<string, string> = {
  closed: 'CLOSED',
  'half-open': 'HALF OPEN',
  open: 'OPEN',
  allowed: 'ALLOWED',
  denied: 'DENIED',
  unknown: 'UNKNOWN',
}

export function StatusBadge({ state, className }: StatusBadgeProps) {
  return (
    <span
      className={cn(
        'inline-flex items-center rounded-full border px-2.5 py-0.5 text-xs font-semibold tracking-wide',
        STATE_STYLES[state] ?? STATE_STYLES.unknown,
        className,
      )}
    >
      {STATE_LABELS[state] ?? state.toUpperCase()}
    </span>
  )
}
