import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { axe } from 'jest-axe'
import { MetricTile } from './MetricTile'

describe('MetricTile', () => {
  it('renders label and value', () => {
    render(<MetricTile label="Remaining" value={7} />)
    expect(screen.getByText('Remaining')).toBeInTheDocument()
    expect(screen.getByText('7')).toBeInTheDocument()
  })

  it('exposes an accessible group label combining label + value', () => {
    render(<MetricTile label="Fail Rate" value="12.5%" sub="rolling" />)
    expect(
      screen.getByRole('group', { name: 'Fail Rate: 12.5%, rolling' }),
    ).toBeInTheDocument()
  })

  it('marks the tile as a polite live region when live', () => {
    const { container } = render(<MetricTile label="Total" value={3} live />)
    expect(container.querySelector('[aria-live="polite"]')).not.toBeNull()
  })

  it('is not a live region by default', () => {
    const { container } = render(<MetricTile label="Total" value={3} />)
    expect(container.querySelector('[aria-live]')).toBeNull()
  })

  it('has no axe violations', async () => {
    const { container } = render(<MetricTile label="Limit" value={10} color="blue" />)
    expect(await axe(container)).toHaveNoViolations()
  })
})
