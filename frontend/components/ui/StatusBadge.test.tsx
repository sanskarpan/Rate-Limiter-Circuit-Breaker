import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { StatusBadge } from './StatusBadge'

describe('StatusBadge', () => {
  it('renders the human label for a known state', () => {
    render(<StatusBadge state="half-open" />)
    expect(screen.getByText('HALF OPEN')).toBeInTheDocument()
  })

  it('renders allowed/denied variants', () => {
    const { rerender } = render(<StatusBadge state="allowed" />)
    expect(screen.getByText('ALLOWED')).toBeInTheDocument()
    rerender(<StatusBadge state="denied" />)
    expect(screen.getByText('DENIED')).toBeInTheDocument()
  })

  it('falls back to uppercased text for an unknown state', () => {
    // @ts-expect-error - exercising the runtime fallback path
    render(<StatusBadge state="weird" />)
    expect(screen.getByText('WEIRD')).toBeInTheDocument()
  })
})
