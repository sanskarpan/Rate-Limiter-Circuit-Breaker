import 'vitest'
import type { AxeResults } from 'jest-axe'

// Augment Vitest's assertion types with the jest-axe matcher registered in
// vitest.setup.ts, so `expect(results).toHaveNoViolations()` type-checks.
interface AxeMatchers<R = unknown> {
  toHaveNoViolations(): R
}

declare module 'vitest' {
  /* eslint-disable @typescript-eslint/no-empty-object-type */
  interface Assertion<T = AxeResults> extends AxeMatchers<T> {}
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  interface AsymmetricMatchersContaining extends AxeMatchers<any> {}
  /* eslint-enable @typescript-eslint/no-empty-object-type */
}
