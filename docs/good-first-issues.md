# Good first issues

A curated set of small, well-scoped starter tasks for new contributors. Each is
derived from a **still-unchecked** item in
[ENHANCEMENTS.md](../ENHANCEMENTS.md) — none of them require deep prior context,
and each has a clear definition of done.

New here? Read the [Contributor on-ramp](../CONTRIBUTING.md#contributor-on-ramp)
in `CONTRIBUTING.md` first — it covers the dev setup, the one non-negotiable
rule (zero-dependency core), and the PR checklist. Then pick an issue below,
comment on the tracking issue to claim it, and open a focused PR.

**Ground rules for every task:**

- Core packages must stay dependency-free — `make verify-deps` must pass. If a
  task needs a third-party import, it belongs in an adapter package (`contrib/`,
  `ratelimit/store`, `ratelimit/middleware`, `observability/`, `metric/`) or a
  test file, never in a core algorithm package.
- `go test -race ./...` and `golangci-lint run` must stay green.
- Add or update tests for any behaviour change; update godoc for exported symbols.

---

## 1. Add `doc.go` package overviews for multi-file packages

- **Source:** ENHANCEMENTS.md §8.7 (P3)
- **Why it's approachable:** Pure documentation — no logic, no API changes. Great
  way to learn the package layout while improving the pkg.go.dev landing pages.
- **Files:** new `doc.go` in the larger packages, e.g. `ratelimit/`,
  `circuitbreaker/`, `pipeline/`, `retry/`, `bulkhead/`, `loadshed/`,
  `concurrency/`.
- **Acceptance criteria:**
  - Each targeted package has a `doc.go` with a package-level comment giving a
    short overview and pointing at the relevant `example_test.go`.
  - `go vet ./...` and `golangci-lint run` stay clean; `make godoc` renders the
    new overviews.
  - No behavioural change; no new imports.

## 2. Add benchmarks for the currently-unbenchmarked hot paths

- **Source:** ENHANCEMENTS.md §3.5 (P3) and the gaps listed in
  [docs/benchmarks.md](benchmarks.md#coverage-and-gaps)
- **Why it's approachable:** Follows the exact pattern of existing
  `*_bench_test.go` files (see `ratelimit/tokenbucket/tokenbucket_bench_test.go`).
  Copy, adapt, run.
- **Files:** new `*_bench_test.go` in `bulkhead/`, `ratelimit/composite/`,
  `ratelimit/adaptive/`, `pipeline/`, and `ratelimit/middleware/`.
- **Acceptance criteria:**
  - At least one `Benchmark*` per targeted package, using `b.ReportAllocs()`.
  - Benchmarks run clean via
    `go test -bench=. -benchmem -run='^$' ./<pkg>/`.
  - Update the "Coverage and gaps" section of `docs/benchmarks.md` to move the
    newly-covered packages out of the gap list (with real captured numbers).

## 3. Extract a read-only `Peeker` interface from `Limiter`

- **Source:** ENHANCEMENTS.md §2.6 (P3)
- **Why it's approachable:** Small, non-breaking interface refactor (embedding).
  Good for learning Go interface segregation.
- **Files:** `ratelimit/limiter.go`.
- **Acceptance criteria:**
  - A new `Peeker interface { Peek(...) State }` is defined and `Limiter` embeds
    it, so existing implementations satisfy both without code changes.
  - All existing tests pass unchanged (proving it is non-breaking).
  - Godoc explains when to depend on `Peeker` vs `Limiter`.

## 4. Add `Default()` constructors / zero-value guidance for limiters

- **Source:** ENHANCEMENTS.md §2.7 (P3)
- **Why it's approachable:** Additive, localized per package; mirrors the
  existing `circuitbreaker.Config` defaults pattern.
- **Files:** `ratelimit/tokenbucket/`, `ratelimit/gcra/`,
  `ratelimit/fixedwindow/` (add `Default()` helpers + doc notes).
- **Acceptance criteria:**
  - Each targeted limiter gains a `Default()` (or documented sane preset)
    returning a usable limiter without panicking.
  - Godoc states which zero/preset values are legal.
  - Tests cover the new constructors.

## 5. Optional structured-logging hook (`WithLogger`) for library consumers

- **Source:** ENHANCEMENTS.md §4.4 (P2)
- **Why it's approachable:** Reuses the existing functional-options pattern
  (`WithOnDecision`, `WithClock`); stdlib-only (`log/slog`), so it stays
  zero-dep.
- **Files:** `options.go` in the limiter packages and `circuitbreaker/config.go`;
  guard log calls behind `slog.Logger.Enabled` level checks.
- **Acceptance criteria:**
  - An optional `*slog.Logger` can be attached; `nil` = silent (default).
  - Debug-level logs for decisions, info-level for state changes.
  - `make verify-deps` stays green (stdlib only); hot path unaffected when no
    logger is set.

## 6. Distributed leaky bucket via a Lua script

- **Source:** ENHANCEMENTS.md §1.8 (P2)
- **Why it's approachable:** Leaky bucket maps cleanly onto the GCRA math that
  already has a distributed implementation — use `ratelimit/gcra/distributed.go`
  and its Lua script as the template.
- **Files:** new `ratelimit/leakybucket/distributed.go`; a Lua script in
  `ratelimit/store/` alongside `GCRAScript`; parity test in `ratelimit/store/`.
- **Acceptance criteria:**
  - `leakybucket.NewDistributed(...)` implements the same `Limiter` interface.
  - The in-memory script emulation matches the Redis result (parity test), as
    the other distributed algorithms do.
  - Integration test passes under `-tags=integration` against a real Redis.

## 7. Differential fuzzing for the remaining rate-limit algorithms

- **Source:** ENHANCEMENTS.md §6.2 (P2)
- **Why it's approachable:** Native Go fuzzing; copy the shape of
  `ratelimit/tokenbucket/fuzz_test.go` / `ratelimit/gcra/fuzz_test.go`.
- **Files:** new `fuzz_test.go` in `ratelimit/fixedwindow/`,
  `ratelimit/slidingwindow/`, `ratelimit/leakybucket/`, `ratelimit/composite/`.
- **Acceptance criteria:**
  - A `Fuzz*` target per algorithm that asserts the admission bound holds over
    random operation/time schedules (reuse `internal/clock.ManualClock`).
  - `go test -run=Fuzz -fuzz=Fuzz -fuzztime=30s ./<pkg>/` finds no failures.

## 8. Mutation testing on the core algorithm packages

- **Source:** ENHANCEMENTS.md §6.6 (P3)
- **Why it's approachable:** Tooling-driven; a good way to discover weak
  assertions without writing new logic.
- **Files:** a `make mutation` target (or `.github/workflows/` nightly job) that
  runs `go-gremlins/gremlins` on `ratelimit/...` and `circuitbreaker/`; document
  in `CONTRIBUTING.md`.
- **Acceptance criteria:**
  - `gremlins` runs on the core packages and produces a mutation report.
  - At least the surviving mutants in one core package are triaged (fixed with a
    new test, or documented as acceptable in the PR description).

## 9. Fault/latency-injection test helper

- **Source:** ENHANCEMENTS.md §6.7 (P2)
- **Why it's approachable:** Self-contained test utility; builds on the existing
  `internal/testutil` + `internal/clock` primitives.
- **Files:** `internal/testutil/` (new `FaultyFunc` helper); a scenario test
  under `pipeline/` or `fallback/`.
- **Acceptance criteria:**
  - A `testutil.FaultyFunc` injects configurable latency/error rates driven by
    `ManualClock`.
  - A scenario test uses it to assert a concrete pipeline behaviour (e.g. hedge
    fires, breaker opens, retry budget stops retries).

## 10. Frontend accessibility pass

- **Source:** ENHANCEMENTS.md §10.1 (P2)
- **Why it's approachable:** Front-end-only; scoped, visible, and testable with
  `@axe-core/playwright`. Radix UI primitives already provide accessible
  building blocks.
- **Files:** `frontend/components/**` (add ARIA live regions, labels,
  keyboard controls), `frontend/e2e/` (axe checks).
- **Acceptance criteria:**
  - ARIA live regions announce the WebSocket-driven metric tiles.
  - SVG visualizations carry `role`/labels; the simulator is keyboard-operable.
  - An `@axe-core/playwright` check passes in the e2e suite with no critical
    violations.

## 11. Frontend component unit tests

- **Source:** ENHANCEMENTS.md §10.5 (P3)
- **Why it's approachable:** Isolated unit tests with Vitest + React Testing
  Library; no backend required.
- **Files:** `frontend/` — tests for the Zustand store reducers and the
  `lib/ws/manager.ts` reconnect/backoff logic.
- **Acceptance criteria:**
  - Vitest config added; unit tests cover the store reducers and WS reconnect
    logic.
  - Tests run via `npm test` and pass in CI.

## 12. Cross-link and "run it" the examples from the README

- **Source:** ENHANCEMENTS.md §11.4 (P3)
- **Why it's approachable:** Docs + light wiring; no library code.
- **Files:** `README.md`, `examples/*/README.md` (add per-example run
  instructions).
- **Acceptance criteria:**
  - The `examples/{http,grpc,distributed,pipeline}` directories are cross-linked
    from the README with a short "run it" snippet each.
  - Each referenced example actually builds (`go build ./examples/...`).

---

## Picking up an issue

1. Open a GitHub issue (or comment on the tracking one) referencing the section
   number above so work isn't duplicated.
2. Follow the [development workflow](../CONTRIBUTING.md#development-workflow):
   fork, branch off `main`, keep the change focused.
3. Before opening the PR, run the [pre-PR checks](../CONTRIBUTING.md#contributor-on-ramp):
   `make test-race`, `golangci-lint run`, and `make verify-deps`.
4. Fill out the PR template and link the issue.
