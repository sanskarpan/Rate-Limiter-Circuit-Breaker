# API Stability & Versioning Policy

This document defines what "stable" means for this project, how versions are
numbered, what the **public API** surface is, and how deprecations are handled.

## Current status: pre-1.0 (`v0.x`)

The latest tagged release is **`v0.1.0`**. The project is **pre-1.0**.

Under [Semantic Versioning](https://semver.org/), the major-version-zero clause
applies: **anything MAY change in any `0.x` release.** In practice we try to be
conservative, but while the module is `v0.x`:

- **Minor** releases (`v0.1.0` → `v0.2.0`) **may contain breaking API changes.**
- **Patch** releases (`v0.1.0` → `v0.1.1`) are intended to be
  backward-compatible bug/security fixes only.

**Pin a specific version in your `go.mod`** for reproducible builds, and read the
[CHANGELOG.md](../CHANGELOG.md) before upgrading a minor version. This mirrors the
note in the [README](../README.md#api-stability).

## Post-1.0 intentions (SemVer)

Once a `v1.0.0` release exists, the project will follow SemVer strictly for the
**public API** (defined below), consistent with the
[Go module compatibility guidelines](https://go.dev/blog/module-compatibility):

- **MAJOR** (`v1` → `v2`): breaking changes to the public API. A new major
  version ships as a new module path (`.../v2`), per Go's import-compatibility
  rule.
- **MINOR** (`v1.0` → `v1.1`): backward-compatible additions (new packages,
  new exported identifiers, new optional functional options).
- **PATCH** (`v1.0.0` → `v1.0.1`): backward-compatible bug and security fixes.

## What "public API" means

The **public API** is the set of **exported identifiers** (types, functions,
methods, constants, variables) in the library's importable packages —
specifically:

- The rate-limiting packages: `ratelimit` and its algorithm subpackages
  (`ratelimit/tokenbucket`, `ratelimit/gcra`, `ratelimit/slidingwindow`,
  `ratelimit/fixedwindow`, `ratelimit/leakybucket`, `ratelimit/adaptive`,
  `ratelimit/composite`), plus `ratelimit/store` and `ratelimit/middleware`.
- The resilience packages: `circuitbreaker` (and `circuitbreaker/middleware`),
  `retry` (and `retry/backoff`), `timeout`, `fallback`, `bulkhead`, `pipeline`,
  `loadshed`, `concurrency`, and `metric`.

The following are **explicitly NOT part of the public API** and may change at any
time without a major-version bump:

- **`internal/`** packages (e.g. `internal/clock`, `internal/atomicx`). The Go
  toolchain already forbids importing these from outside the module; they carry
  no compatibility guarantee.
- The **demo server** (`server/`), the **`frontend/`** application, and the
  runnable **`examples/`** — these exist to demonstrate the library, are not
  importable as a stable API, and pull in dependencies the core does not.
- Behaviour explicitly documented as experimental in package docs.

The `adaptive` limiter is functional but its tuning heuristics (adjustment
signals and step sizes) may evolve; treat its *numerical behaviour* as subject to
change even though its `ratelimit.Limiter` interface is stable.

## The `/contrib` module is versioned separately

Framework adapters (chi, gin, echo, fiber, connect) live in the **separate
`contrib` module** (`contrib/go.mod`, module path
`github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/contrib`). This keeps
third-party framework dependencies out of the core module so the "zero external
runtime dependencies" guarantee for the core holds (see
[ADR-0001](adr/0001-zero-dependency-core.md)).

Because it is its own module, `contrib` carries its **own version tags and its own
SemVer timeline**, independent of the core module's version. A breaking change in
a framework adapter does not force a major bump of the core, and vice versa. Pin
the `contrib` module version separately in your `go.mod`.

## Deprecation policy

When an exported identifier is slated for removal (post-1.0):

1. It is marked with a `// Deprecated:` doc comment (the convention `gopls`,
   `staticcheck`, and pkg.go.dev recognize) that names the replacement.
2. It continues to work for **at least one full minor release** after the
   deprecation is announced.
3. The deprecation is recorded in [CHANGELOG.md](../CHANGELOG.md).
4. Removal happens no sooner than the **next minor release** after the notice
   (post-1.0, removals that break compatibility are deferred to the next major).

While pre-1.0, we still aim to give a one-minor-version deprecation window as a
courtesy, but reserve the right to remove deprecated pre-1.0 API sooner if it is
clearly unused or actively harmful.
