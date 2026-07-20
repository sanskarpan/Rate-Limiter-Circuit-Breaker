import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { axe } from 'jest-axe'
import { TokenBucketViz } from './TokenBucketViz'

describe('TokenBucketViz', () => {
  it('renders an accessible image label describing the fill', () => {
    render(<TokenBucketViz tokens={6} capacity={10} />)
    expect(
      screen.getByRole('img', { name: /6 of 10 tokens available/i }),
    ).toBeInTheDocument()
  })

  it('notes when the last request was denied', () => {
    render(<TokenBucketViz tokens={0} capacity={10} lastAllowed={false} />)
    expect(
      screen.getByRole('img', { name: /last request denied/i }),
    ).toBeInTheDocument()
  })

  it('exposes a progressbar with the correct value range', () => {
    render(<TokenBucketViz tokens={3} capacity={12} />)
    const bar = screen.getByRole('progressbar', { name: 'Tokens available' })
    expect(bar).toHaveAttribute('aria-valuenow', '3')
    expect(bar).toHaveAttribute('aria-valuemax', '12')
    expect(bar).toHaveAttribute('aria-valuetext', '3 of 12 tokens')
  })

  it('handles zero capacity without dividing by zero', () => {
    expect(() => render(<TokenBucketViz tokens={0} capacity={0} />)).not.toThrow()
  })

  it('has no axe violations', async () => {
    const { container } = render(<TokenBucketViz tokens={5} capacity={10} />)
    expect(await axe(container)).toHaveNoViolations()
  })
})
