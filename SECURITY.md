# Security Policy

Thanks for helping keep this project and its users safe. This document explains
which versions receive security fixes and how to report a vulnerability
responsibly.

## Supported Versions

This library has not yet had a tagged release; development happens on `main`
toward an initial `0.1.0`. Until a stable release exists, security fixes are
applied to the latest `main`.

| Version | Supported |
| ------- | --------- |
| `main` (unreleased, pre-`0.1.0`) | ✅ |
| Tagged releases | ✅ latest minor once released |

This table will be updated as versions are tagged. Once `0.x`/`1.x` releases
exist, only the latest released minor line will receive security fixes unless
otherwise noted.

## Reporting a Vulnerability

**Please do not open a public issue for security vulnerabilities.**

Report privately using one of the following, in order of preference:

1. **GitHub private security advisory** (preferred):
   <https://github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/security/advisories/new>
   This keeps the report confidential and lets us collaborate on a fix and CVE
   if warranted.

2. **Email:**
   > **[PLACEHOLDER — maintainer to fill in]** Add a monitored security contact
   > here, e.g. `security@your-domain.example`. Until one is configured, use the
   > GitHub private advisory link above.

Please include, where possible:

- A description of the vulnerability and its impact.
- The affected package(s) and version/commit (`git rev-parse HEAD`) and your Go
  version (`go version`).
- Reproduction steps or a minimal proof of concept.
- Any suggested remediation.

## Response Targets

These are goals, not contractual guarantees, for a project maintained on a
best-effort basis:

- **Acknowledgement:** within **3 business days** of your report.
- **Initial assessment / severity triage:** within **7 business days**.
- **Fix or mitigation plan:** communicated after triage, prioritized by
  severity.

We will keep you informed of progress throughout.

## Disclosure Policy

We follow **coordinated disclosure**:

1. You report the issue privately using a channel above.
2. We confirm the issue and develop a fix in a private branch/advisory.
3. We prepare a release and, where appropriate, request a CVE.
4. We publicly disclose (release notes, [CHANGELOG.md](CHANGELOG.md), and the
   GitHub advisory) once a fix is available — typically coordinated with the
   reporter, and generally no later than **90 days** after the report.

We are happy to credit reporters in the advisory and changelog unless you prefer
to remain anonymous.

## Scope

In scope: the Go library packages (rate limiters, circuit breaker, bulkhead,
retry, timeout, fallback, pipeline), the Redis/gRPC adapters, the demo server,
and the `frontend/` application in this repository.

Out of scope: vulnerabilities in third-party dependencies (please report those
upstream, though we appreciate a heads-up so we can update), and issues that
require a misconfigured or already-compromised host environment.
