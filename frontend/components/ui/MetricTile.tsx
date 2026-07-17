import { cn } from '@/lib/utils'

interface MetricTileProps {
  label: string
  value: string | number
  sub?: string
  color?: 'default' | 'green' | 'red' | 'amber' | 'blue'
  className?: string
}

const COLOR_MAP = {
  default: 'text-white',
  green: 'text-green-400',
  red: 'text-red-400',
  amber: 'text-amber-400',
  blue: 'text-blue-400',
}

export function MetricTile({
  label,
  value,
  sub,
  color = 'default',
  className,
}: MetricTileProps) {
  return (
    <div
      className={cn(
        'flex flex-col gap-1 rounded-lg border border-white/10 bg-white/5 p-4',
        className,
      )}
    >
      <span className="text-xs font-medium uppercase tracking-widest text-gray-500">
        {label}
      </span>
      <span className={cn('text-2xl font-bold tabular-nums', COLOR_MAP[color])}>
        {value}
      </span>
      {sub && <span className="text-xs text-gray-500">{sub}</span>}
    </div>
  )
}
