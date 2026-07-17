# Rate Limiter & Circuit Breaker — Visual Playground

An interactive [Next.js](https://nextjs.org) frontend for exploring the
rate-limiting algorithms and circuit-breaker behavior implemented in the Go
library. It renders live charts and simulations by calling the bundled **Go demo
server**, so you can *see* token buckets refill, GCRA shape traffic, and circuit
breakers trip and recover in real time.

This app is a demonstration and teaching tool — it is not required to use the Go
library itself.

## What's inside

Built with Next.js 16 (App Router) and React 19, using Tailwind CSS v4, Radix UI
primitives, Recharts for visualizations, Framer Motion for animation, and
Zustand for state. Rate-limiter / circuit-breaker responses are validated with
Zod.

### Pages

| Route                      | Description                                              |
| -------------------------- | -------------------------------------------------------- |
| `/`                        | Home / overview and entry point to the playground.       |
| `/algorithms/[algo]`       | Deep-dive view for a single rate-limiting algorithm.     |
| `/algorithms/compare`      | Side-by-side comparison of multiple algorithms.          |
| `/circuit-breaker`         | Interactive circuit-breaker state machine visualization. |
| `/pipeline`                | Composed rate-limit + circuit-breaker request pipeline.  |
| `/simulate`                | Traffic simulator to drive load against the algorithms.  |
| `/docs` and `/docs/[slug]` | In-app documentation pages.                              |

## Prerequisites

- **Node.js 20.9+ or 22+** (required by Next.js 16). Check with `node --version`.
- **npm** (ships with Node). Yarn/pnpm/bun also work if you prefer.
- The **Go demo server** running locally (or reachable) — see below. Without it,
  the pages load but charts will have no data.

## Getting started

### 1. Install dependencies

```bash
npm install
```

### 2. Configure the API endpoint

The app talks to the Go demo server via the `NEXT_PUBLIC_API_URL` environment
variable, which **defaults to `http://localhost:8080`**.

Copy the committed example and adjust only if your server runs elsewhere:

```bash
cp .env.example .env.local
```

```dotenv
# .env.local
NEXT_PUBLIC_API_URL=http://localhost:8080
```

`NEXT_PUBLIC_API_URL` is inlined at build time (the `NEXT_PUBLIC_` prefix exposes
it to the browser), so **rebuild / restart the dev server after changing it**.

### 3. Start the Go demo server

From the repository root, run the demo server so the frontend has a backend to
call. It listens on `:8080` by default. See the top-level project README for the
exact command (typically a `make` target or `go run ./server/`).

### 4. Run the frontend

```bash
npm run dev
```

Open <http://localhost:3000>. If you pointed `NEXT_PUBLIC_API_URL` at a
different host/port, make sure that server is running and reachable from the
browser.

## Available scripts

| Script             | What it does                                            |
| ------------------ | ------------------------------------------------------- |
| `npm run dev`      | Start the Next.js dev server (hot reload) on port 3000. |
| `npm run build`    | Production build.                                       |
| `npm run start`    | Serve the production build (run `build` first).         |
| `npm run lint`     | Run ESLint.                                             |
| `npm run test:e2e` | Run the Playwright end-to-end tests.                    |

## Production build

```bash
npm run build
npm run start
```

Ensure `NEXT_PUBLIC_API_URL` points at a reachable demo server for the
environment you're deploying to (it is baked in at build time).

## End-to-end tests

E2E tests use [Playwright](https://playwright.dev) and live in `e2e/`.

The Playwright config uses `baseURL: http://localhost:3000` and does **not**
auto-start anything, so you must have both servers running first:

1. Start the Go demo server (so the app has data).
2. Start the frontend: `npm run dev` (serving on port 3000).
3. In another terminal, run the tests:

   ```bash
   npm run test:e2e
   ```

On first run, install the Playwright browsers if prompted:

```bash
npx playwright install
```

## Notes

- `.env.local` is git-ignored; commit only `.env.example`.
- Because `NEXT_PUBLIC_API_URL` is a build-time public variable, do not put
  secrets in it — it ends up in the client bundle.
