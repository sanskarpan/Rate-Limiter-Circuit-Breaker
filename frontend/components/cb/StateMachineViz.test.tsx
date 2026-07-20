import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { axe } from 'jest-axe'
import { StateMachineViz } from './StateMachineViz'

describe('StateMachineViz', () => {
  it('exposes the current state in its accessible label', () => {
    render(<StateMachineViz state="open" />)
    expect(
      screen.getByRole('img', { name: /Current state: open/i }),
    ).toBeInTheDocument()
  })

  it('renders all three state labels (node + legend)', () => {
    render(<StateMachineViz state="closed" />)
    // Each label appears in both the SVG node and the legend below it.
    expect(screen.getAllByText('CLOSED').length).toBeGreaterThanOrEqual(1)
    expect(screen.getAllByText('OPEN').length).toBeGreaterThanOrEqual(1)
    expect(screen.getAllByText('HALF OPEN').length).toBeGreaterThanOrEqual(1)
  })

  it('has no axe violations', async () => {
    const { container } = render(<StateMachineViz state="half-open" />)
    expect(await axe(container)).toHaveNoViolations()
  })
})
