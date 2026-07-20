import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { AllowDenyRatioChart } from './AllowDenyRatioChart'
import { LatencyHistogram } from './LatencyHistogram'
import { CBStateTimeline, type CBStatePoint } from './CBStateTimeline'
import type { SimResult } from '@/lib/api/types'

function mkResult(allowed: boolean, latency: number): SimResult {
  return {
    timestamp: new Date().toISOString(),
    allowed,
    latency_ms: latency,
    algorithm: 'token_bucket',
    error: null,
    cb_state: null,
  }
}

describe('AllowDenyRatioChart', () => {
  it('shows an empty state with no results', () => {
    render(<AllowDenyRatioChart results={[]} />)
    expect(screen.getByText(/no requests yet/i)).toBeInTheDocument()
  })

  it('renders an accessible chart with data and reports the latest allow rate', () => {
    const results = [
      mkResult(true, 1),
      mkResult(true, 2),
      mkResult(false, 3),
      mkResult(true, 4),
    ]
    render(<AllowDenyRatioChart results={results} />)
    expect(
      screen.getByRole('img', { name: /rolling allow-rate chart/i }),
    ).toBeInTheDocument()
  })
})

describe('LatencyHistogram', () => {
  it('shows an empty state with no samples', () => {
    render(<LatencyHistogram latencies={[]} />)
    expect(screen.getByText(/no latency samples yet/i)).toBeInTheDocument()
  })

  it('renders a histogram for a spread of latencies', () => {
    render(<LatencyHistogram latencies={[1, 2, 3, 10, 20, 50, 100]} />)
    expect(
      screen.getByRole('img', { name: /latency histogram across 7 requests/i }),
    ).toBeInTheDocument()
  })

  it('collapses identical latencies into a single bucket without error', () => {
    expect(() =>
      render(<LatencyHistogram latencies={[5, 5, 5, 5]} />),
    ).not.toThrow()
    expect(screen.getByRole('img')).toBeInTheDocument()
  })
})

describe('CBStateTimeline', () => {
  it('shows an empty state with no points', () => {
    render(<CBStateTimeline points={[]} />)
    expect(screen.getByText(/no state history yet/i)).toBeInTheDocument()
  })

  it('renders a timeline for a series of transitions', () => {
    const points: CBStatePoint[] = [
      { t: 1000, state: 'closed' },
      { t: 2000, state: 'open' },
      { t: 3000, state: 'half-open' },
      { t: 4000, state: 'closed' },
    ]
    render(<CBStateTimeline points={points} />)
    expect(
      screen.getByRole('img', { name: /circuit breaker state timeline/i }),
    ).toBeInTheDocument()
  })
})
