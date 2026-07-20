# Deploying the playground frontend (Vercel)

The `frontend/` app is a standalone Next.js UI that talks to the demo server purely
over REST + WebSocket (`NEXT_PUBLIC_API_URL`), so it deploys independently of the Go
modules. This guide covers Vercel deployment with automatic **preview environments**
for every pull request.

## One-time setup (Vercel dashboard)

1. **Import the repo** at <https://vercel.com/new> and pick this repository.
2. **Root Directory:** set it to `frontend`. Vercel auto-detects Next.js and uses
   `next build` / `next start`; no build overrides are needed. The committed
   [`frontend/vercel.json`](../frontend/vercel.json) pins the framework and adds
   baseline security headers.
3. **Environment variables** (Project → Settings → Environment Variables):
   - `NEXT_PUBLIC_API_URL` — the public URL of the demo server's REST/WS API
     (e.g. `https://resilience-demo.example.com`). Set it for **Production**,
     **Preview**, and **Development** as appropriate. For ephemeral preview
     deployments you can point this at a shared staging server.
4. Click **Deploy**.

## Preview environments

Once connected via the Vercel Git integration:

- **Every pull request** gets its own immutable **Preview Deployment** with a unique
  URL, posted back to the PR by the Vercel bot. This is the "preview environment"
  called for in the roadmap — no extra workflow is required.
- Pushes to `main` publish to the **Production** deployment (gated by
  `git.deploymentEnabled.main` in `vercel.json`).
- Because the frontend is decoupled from the API, a preview can be pointed at any
  running demo server via its `NEXT_PUBLIC_API_URL` Preview-scope value.

## Optional: CI-driven deploys

If you prefer deploying from GitHub Actions instead of the native Git integration,
add the [`amondnet/vercel-action`](https://github.com/amondnet/vercel-action) (or the
`vercel` CLI) as a job with `working-directory: frontend`, gated on the repo secrets
`VERCEL_TOKEN`, `VERCEL_ORG_ID`, and `VERCEL_PROJECT_ID`. Keep it **off the required
PR checks** so contributors without deploy secrets are never blocked. The native
integration above is the recommended path and needs no secrets in the repo.

## Local production preview

```sh
cd frontend
npm ci
NEXT_PUBLIC_API_URL=http://localhost:8080 npm run build
NEXT_PUBLIC_API_URL=http://localhost:8080 npm run start   # serves on :3000
```

Run the demo server alongside it (`make build-go && ./bin/demo-server`, or
`make test-e2e` which wires both together for the Playwright suite).
