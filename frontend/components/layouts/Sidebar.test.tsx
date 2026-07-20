import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { axe } from 'jest-axe'

vi.mock('next/navigation', () => ({
  usePathname: () => '/circuit-breaker',
}))

// next/link renders a plain anchor in tests.
vi.mock('next/link', () => ({
  default: ({ children, href, ...rest }: { children: React.ReactNode; href: string }) => (
    <a href={href} {...rest}>
      {children}
    </a>
  ),
}))

import { Sidebar } from './Sidebar'
import { useAppStore } from '@/lib/store'

describe('Sidebar', () => {
  it('marks the active route with aria-current="page"', () => {
    render(<Sidebar />)
    const current = screen.getByRole('link', { name: /Circuit Breaker/i })
    expect(current).toHaveAttribute('aria-current', 'page')
  })

  it('exposes labelled navigation landmarks', () => {
    render(<Sidebar />)
    expect(screen.getByRole('navigation', { name: 'Main' })).toBeInTheDocument()
    expect(screen.getByRole('navigation', { name: 'Algorithms' })).toBeInTheDocument()
  })

  it('renders the WS status as a live region reflecting the store', () => {
    useAppStore.setState({ wsStatus: 'open' })
    render(<Sidebar />)
    const status = screen.getByRole('status')
    expect(status).toHaveAttribute('aria-live', 'polite')
    expect(status).toHaveTextContent(/open/i)
  })

  it('has no axe violations', async () => {
    const { container } = render(<Sidebar />)
    expect(await axe(container)).toHaveNoViolations()
  })
})
