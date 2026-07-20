# Mutation testing

> ENHANCEMENTS §6.6 — measure whether the test suite's *assertions* actually
> catch faults, not just how many tests exist.

A high test count does not prove the tests would fail if the logic were wrong.
**Mutation testing** perturbs the source (flips `<` to `<=`, `&&` to `||`, `+`
to `-`, …) one change at a time, re-runs the tests, and asks: *did a test
notice?* A mutant that makes no test fail is a **survivor** — a gap where the
code could be broken without any test complaining.

This repository uses [**go-gremlins**](https://gremlins.dev). It is invoked via
`go run`, so **no dependency is added to the core `go.mod`/`go.sum`** — the
zero-dependency guarantee (`make verify-deps`) is preserved.

## Running it

Mutation testing is **slow** (it re-runs the test suite once per mutant). Run it
locally or on a nightly/manual CI job — **never** as a required PR check.

```sh
# Full run on the core algorithm packages (see Makefile PKG default):
make mutation

# Target a single package:
make mutation PKG=./ratelimit/tokenbucket/...

# Fast sanity check — list the mutants that WOULD be tested, without running
# the suite (shows which lines are covered enough to be worth mutating):
make mutation-dry
```

Configuration lives in [`.gremlins.yaml`](../.gremlins.yaml). The tool version is
pinned in the `Makefile` (`MUTATION_TOOL`).

> **Note on `workers`.** The config sets `workers: 1`. go-gremlins v0.5.0's
> parallel worktree "dealer" can panic (`error, this is temporary`) when several
> workers race to materialise the per-worker source copy under newer Go
> toolchains. A single worker is slower but reliable. If your gremlins/Go
> combination is stable with more, raise `unleash.workers` in `.gremlins.yaml`.

## Which packages to target

Mutation testing pays off most on **arithmetic- and boundary-heavy logic**,
which is exactly where this library's correctness lives. The default `PKG` set
in the `Makefile` targets the algorithm packages:

- `circuitbreaker/...` — failure counting, thresholds, half-open probe math,
  open-timeout boundary.
- `retry/...` — attempt counting, backoff, the retry-budget token math.
- `ratelimit/tokenbucket`, `ratelimit/gcra`, `ratelimit/fixedwindow`,
  `ratelimit/slidingwindow`, `ratelimit/leakybucket` — token/window arithmetic
  and the allow/deny boundary conditions.
- `bulkhead/...` — concurrency-slot accounting and rejection conditions.

Adapter/plumbing packages (`ratelimit/store`, `circuitbreaker/middleware`,
`server`, `observability`) are intentionally excluded: their behaviour is better
covered by the integration tests (`integration/`, §6.3) than by mutating I/O
glue.

## Interpreting results

go-gremlins reports each mutant with one of these outcomes:

| Outcome | Meaning | Action |
| ------- | ------- | ------ |
| **KILLED** | A test failed when the mutant was applied. Good — the assertion works. | none |
| **LIVED** (survivor) | No test failed. The logic could be broken here undetected. | **triage** — add/strengthen an assertion, or justify why it's a false positive. |
| **NOT COVERED** | No test exercises this line at all. | add a test (this is a coverage gap, not a weak assertion). |
| **TIMED OUT** | The mutant caused an infinite loop; counted as killed. | none |
| **NOT VIABLE** | The mutant didn't compile. | none (ignored). |

The **mutation score** (efficacy) is `KILLED / (KILLED + LIVED)` — the fraction
of *covered* mutants the tests caught. `.gremlins.yaml` sets
`threshold-efficacy: 0` so a run never fails the build today; raise it once the
survivor backlog is triaged so a nightly job can **ratchet** the score upward
and prevent regressions.

### Triaging a survivor

For each LIVED mutant:

1. Read the reported file:line and the mutation applied (e.g. "changed `>=` to
   `>`").
2. Decide: is there an input for which the mutated code produces a *different,
   observable* result? If yes, add a test asserting that result. That test will
   kill the mutant.
3. If the mutation is genuinely equivalent (no observable behaviour change —
   common with boundary mutations on already-clamped values), record it as a
   known-equivalent survivor rather than contorting a test around it.

Prioritise survivors in the **allow/deny decision paths** and **state-transition
conditions** — a survivor there means a real correctness bug could ship
unnoticed; a survivor in a log message or metric label does not.

## CI

Mutation testing is wired as a **manual / scheduled** GitHub Actions workflow
(`.github/workflows/mutation.yml`), triggered by `workflow_dispatch` or a weekly
cron — **not** on every PR, because it is too slow to gate merges. It uploads the
gremlins report as an artifact for triage.
