import '@testing-library/jest-dom/vitest'
import { afterEach, expect, vi } from 'vitest'
import { cleanup } from '@testing-library/react'
import { toHaveNoViolations } from 'jest-axe'

// jest-axe ships a custom matcher (toHaveNoViolations) — register it on Vitest's
// expect so a11y assertions read naturally in tests. `toHaveNoViolations` is an
// object of the shape { toHaveNoViolations(...) }, so spread it into extend.
expect.extend(toHaveNoViolations)

// jsdom does not implement these; several components (framer-motion, recharts)
// touch them. Provide inert stubs so rendering does not throw.
if (!window.matchMedia) {
  window.matchMedia = vi.fn().mockImplementation((query: string) => ({
    matches: false,
    media: query,
    onchange: null,
    addListener: vi.fn(),
    removeListener: vi.fn(),
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
    dispatchEvent: vi.fn(),
  }))
}

if (!('ResizeObserver' in globalThis)) {
  class ResizeObserverStub {
    observe() {}
    unobserve() {}
    disconnect() {}
  }
  globalThis.ResizeObserver = ResizeObserverStub as unknown as typeof ResizeObserver
}

afterEach(() => {
  cleanup()
})
