import { test, expect } from '@playwright/test'

test('overview renders algorithm cards (SSR + hydration)', async ({ page }) => {
  const errors: string[] = []
  page.on('pageerror', (e) => errors.push(String(e)))
  await page.goto('/')
  await expect(page.getByText('Token Bucket').first()).toBeVisible({ timeout: 15000 })
  expect(errors, `page JS errors: ${errors.join('\n')}`).toHaveLength(0)
})

test('token bucket: Send Request round-trips to live API and shows Allowed, then Denied when exhausted', async ({ page }) => {
  await page.goto('/algorithms/token_bucket')
  const send = page.getByRole('button', { name: 'Send Request' })
  await expect(send).toBeVisible({ timeout: 15000 })
  await send.click()
  // Real fetch to the Go server must resolve and render an Allowed result.
  await expect(page.getByText(/Allowed/i).first()).toBeVisible({ timeout: 15000 })

  // Hammer to exhaust the bucket (capacity 20). Click well past capacity so
  // refill (10/s) can't keep up.
  for (let i = 0; i < 45; i++) {
    await send.click()
  }
  await expect(page.getByText(/Denied/i).first()).toBeVisible({ timeout: 15000 })
})

test('circuit breaker: simulating failures trips primary to OPEN (H-20 execute contract)', async ({ page }) => {
  await page.goto('/circuit-breaker')
  await expect(page.getByText('primary').first()).toBeVisible({ timeout: 15000 })
  const fail = page.getByRole('button', { name: 'failure' }).first()
  await expect(fail).toBeVisible({ timeout: 15000 })
  for (let i = 0; i < 8; i++) {
    await fail.click()
    await page.waitForTimeout(250)
  }
  // If executeCB sent the wrong body (old H-20 bug), the breaker would never
  // see failures and would stay CLOSED. It must reach OPEN.
  await expect(page.getByText('OPEN').first()).toBeVisible({ timeout: 15000 })
})
