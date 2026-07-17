<!--
Thanks for contributing! Please fill out the sections below and complete the
checklist. See CONTRIBUTING.md for the full workflow.
-->

## Summary

<!-- What does this PR do and why? Keep it focused. -->

## Related issue

<!-- Link the issue this addresses, e.g. "Closes #123". Required for non-trivial changes. -->

Closes #

## Type of change

- [ ] Bug fix (`fix:`)
- [ ] New feature (`feat:`)
- [ ] Documentation (`docs:`)
- [ ] Refactor / performance (`refactor:` / `perf:`)
- [ ] Build / CI / chore (`build:` / `ci:` / `chore:`)

## Checklist

- [ ] Tests added or updated for the change.
- [ ] `go test -race ./...` passes locally (or `make test-race`).
- [ ] `golangci-lint run` is clean (or `make lint`).
- [ ] `make verify-deps` passes — no new external deps in core packages.
- [ ] Exported symbols have godoc; relevant docs/README/examples updated.
- [ ] `CHANGELOG.md` `## [Unreleased]` updated for user-facing changes.
- [ ] Commits follow [Conventional Commits](https://www.conventionalcommits.org/).
- [ ] Integration tests run if Redis-backed code changed (`go test -tags=integration ./...`).

## Notes for reviewers

<!-- Anything reviewers should focus on, benchmark results, trade-offs, etc. -->
