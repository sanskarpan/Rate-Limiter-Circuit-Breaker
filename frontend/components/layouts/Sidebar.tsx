'use client'

import Link from 'next/link'
import { usePathname } from 'next/navigation'
import { cn } from '@/lib/utils'
import { useAppStore } from '@/lib/store'
import {
  LayoutDashboard,
  Zap,
  GitBranch,
  Activity,
  Circle,
  Play,
  GitCompare,
  Layers,
  BookOpen,
} from 'lucide-react'

const ALGO_LINKS = [
  { href: '/algorithms/token_bucket', label: 'Token Bucket' },
  { href: '/algorithms/sliding_window', label: 'Sliding Window' },
  { href: '/algorithms/fixed_window', label: 'Fixed Window' },
  { href: '/algorithms/leaky_bucket', label: 'Leaky Bucket' },
  { href: '/algorithms/compare', label: 'Compare', icon: GitCompare },
]

const NAV_LINKS = [
  { href: '/', label: 'Overview', icon: LayoutDashboard },
  { href: '/circuit-breaker', label: 'Circuit Breaker', icon: GitBranch },
  { href: '/simulate', label: 'Simulator', icon: Play },
  { href: '/pipeline', label: 'Pipeline Builder', icon: Layers },
  { href: '/docs', label: 'Documentation', icon: BookOpen },
]

const WS_STATUS_COLORS = {
  idle: 'text-gray-500',
  connecting: 'text-amber-400 animate-pulse',
  open: 'text-green-400',
  closed: 'text-red-400',
  error: 'text-red-500',
}

export function Sidebar() {
  const pathname = usePathname()
  const wsStatus = useAppStore((s) => s.wsStatus)

  return (
    <aside
      className="flex h-screen w-64 flex-col border-r border-white/10 bg-gray-950 px-4 py-6"
      aria-label="Primary"
    >
      {/* Logo */}
      <div className="mb-8 flex items-center gap-3 px-2">
        <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-blue-600">
          <Activity className="h-5 w-5 text-white" aria-hidden="true" />
        </div>
        <div>
          <p className="text-sm font-bold text-white">Rate Limiter</p>
          <p className="text-xs text-gray-500">Dashboard</p>
        </div>
      </div>

      {/* Main nav */}
      <nav className="flex flex-col gap-1" aria-label="Main">
        {NAV_LINKS.map(({ href, label, icon: Icon }) => {
          const active = pathname === href
          return (
            <Link
              key={href}
              href={href}
              aria-current={active ? 'page' : undefined}
              className={cn(
                'flex items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium transition-colors',
                active
                  ? 'bg-blue-600/20 text-blue-400'
                  : 'text-gray-400 hover:bg-white/5 hover:text-white',
              )}
            >
              <Icon className="h-4 w-4" aria-hidden="true" />
              {label}
            </Link>
          )
        })}
      </nav>

      {/* Algorithms section */}
      <div className="mt-6">
        <p className="mb-2 px-3 text-xs font-semibold uppercase tracking-widest text-gray-600">
          Algorithms
        </p>
        <nav className="flex flex-col gap-1" aria-label="Algorithms">
          {ALGO_LINKS.map(({ href, label, icon: Icon }) => {
            const active = pathname === href
            return (
              <Link
                key={href}
                href={href}
                aria-current={active ? 'page' : undefined}
                className={cn(
                  'flex items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium transition-colors',
                  active
                    ? 'bg-blue-600/20 text-blue-400'
                    : 'text-gray-400 hover:bg-white/5 hover:text-white',
                )}
              >
                {Icon ? (
                  <Icon className="h-4 w-4" aria-hidden="true" />
                ) : (
                  <Zap className="h-4 w-4" aria-hidden="true" />
                )}
                {label}
              </Link>
            )
          })}
        </nav>
      </div>

      {/* WS status at bottom — live region so screen readers announce changes */}
      <div
        className="mt-auto flex items-center gap-2 rounded-lg border border-white/10 px-3 py-2"
        role="status"
        aria-live="polite"
        aria-label={`WebSocket connection status: ${wsStatus}`}
      >
        <Circle
          className={cn('h-2 w-2 fill-current', WS_STATUS_COLORS[wsStatus])}
          aria-hidden="true"
        />
        <span className="text-xs text-gray-400">
          WS:{' '}
          <span className={cn('font-medium', WS_STATUS_COLORS[wsStatus])}>
            {wsStatus}
          </span>
        </span>
      </div>
    </aside>
  )
}
