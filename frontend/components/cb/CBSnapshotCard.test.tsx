import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { axe } from 'jest-axe'
import { CBSnapshotCard } from './CBSnapshotCard'
import type { CBSnapshot } from '@/lib/api/types'

const snapshot: CBSnapshot = {
  name: 'primary',
  state: 'open',
  failures: 4,
  successes: 1,
  requests: 5,
  failure_rate: 0.8,
  opened_at: '2026-01-01T00:00:00Z',
  time_until_half_open_ms: 3000,
}

describe('CBSnapshotCard', () => {
  it('renders the breaker name, state and metrics', () => {
    render(<CBSnapshotCard snapshot={snapshot} />)
    expect(screen.getByText('primary')).toBeInTheDocument()
    expect(screen.getByText('OPEN')).toBeInTheDocument()
    expect(screen.getByText('80.0%')).toBeInTheDocument()
    // OPEN + timeout > 0 → shows the half-open countdown.
    expect(screen.getByText('Half-Open In')).toBeInTheDocument()
    expect(screen.getByText('3s')).toBeInTheDocument()
  })

  it('does not render controls when onExecute is absent', () => {
    render(<CBSnapshotCard snapshot={snapshot} />)
    expect(screen.queryByRole('button')).toBeNull()
  })

  it('renders control buttons grouped with breaker context and fires onExecute', async () => {
    const onExecute = vi.fn()
    render(<CBSnapshotCard snapshot={snapshot} onExecute={onExecute} />)
    // The buttons live in a labelled group that supplies the breaker context...
    expect(
      screen.getByRole('group', {
        name: 'Simulate a request on the primary circuit breaker',
      }),
    ).toBeInTheDocument()
    // ...while each button's accessible name stays the visible verb, so both
    // screen readers and the existing e2e selectors work.
    const failBtn = screen.getByRole('button', { name: 'failure' })
    await userEvent.click(failBtn)
    expect(onExecute).toHaveBeenCalledWith('failure')
  })

  it('has no axe violations with controls', async () => {
    const { container } = render(
      <CBSnapshotCard snapshot={snapshot} onExecute={() => {}} />,
    )
    expect(await axe(container)).toHaveNoViolations()
  })
})
