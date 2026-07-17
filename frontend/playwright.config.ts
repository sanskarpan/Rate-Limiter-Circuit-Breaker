import { defineConfig } from '@playwright/test'
export default defineConfig({
  testDir: './e2e',
  timeout: 45000,
  retries: 0,
  reporter: 'line',
  use: { baseURL: 'http://localhost:3000', headless: true, actionTimeout: 15000 },
})
