# Architecture Decision Records (ADRs)

An **Architecture Decision Record** captures a single significant, load-bearing
design decision — the context that forced it, the decision itself, and the
consequences (good and bad) we accepted. ADRs make the *why* durable and
reviewable, so future contributors don't have to reverse-engineer intent from
code comments and audit documents.

We use the lightweight
[Michael Nygard format](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions):
each record has a **Status**, **Context**, **Decision**, and **Consequences**.
Records are numbered sequentially and are immutable once accepted — a decision is
changed by adding a new ADR that supersedes an older one, not by editing history.

## Index

| ADR | Title | Status |
| --- | ----- | ------ |
| [0001](0001-zero-dependency-core.md) | Zero-dependency core; dependencies isolated in adapters | Accepted |
| [0002](0002-clock-interface-for-determinism.md) | Clock interface for deterministic, testable time | Accepted |
| [0003](0003-store-interface-and-lua-for-distributed.md) | `Store` interface + Lua scripts for distributed limiting | Accepted |
| [0004](0004-fail-open-resilience-philosophy.md) | Fail-open on store errors | Accepted |

## Writing a new ADR

1. Copy the structure of an existing record (Status / Context / Decision /
   Consequences).
2. Give it the next sequential number: `NNNN-short-kebab-title.md`.
3. Base it on what the code **actually does** and cite real file paths.
4. Add a row to the index above.
