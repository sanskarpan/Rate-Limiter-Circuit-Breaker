# Security Policy

Thanks for helping keep this project and its users safe. This document explains
which versions receive security fixes and how to report a vulnerability
responsibly.

## Supported Versions

This project is **pre-1.0** (see [docs/STABILITY.md](docs/STABILITY.md)). Security
fixes land on `main` and are shipped in the next `0.x` release. Only the latest
released `0.x` minor line receives security fixes; there is no long-term support
for older minor lines while the project is pre-1.0.

| Version | Supported |
| ------- | --------- |
| `main` (development branch) | ✅ |
| `v0.1.x` (latest released `0.x` minor) | ✅ |
| Older `0.x` minor lines | ❌ (upgrade to the latest `0.x`) |

This table will be updated as versions are tagged. Once a `1.0.0` release exists,
the [SemVer](https://semver.org/) policy in [docs/STABILITY.md](docs/STABILITY.md)
governs which release lines receive fixes.

## Reporting a Vulnerability

**Please do not open a public issue for security vulnerabilities.**

Report privately through **GitHub's private vulnerability reporting** (GitHub
Security Advisories). This is the only supported channel — the project does not
publish a monitored security email.

1. Go to the repository's **Security** tab:
   <https://github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/security/advisories>
2. Click **"Report a vulnerability"** (or use the direct link
   <https://github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/security/advisories/new>).
3. Fill in the advisory form with the details below.

Reports are delivered privately to the maintainer
([`@sanskarpan`](https://github.com/sanskarpan)) and let us collaborate on a fix
and request a CVE if warranted, without any public disclosure until a patch is
ready.

If you are unable to use GitHub Security Advisories, open a **minimal** public
issue asking the maintainer to open a private advisory with you — do **not**
include any exploit details, proof of concept, or reproduction steps in that
public issue.

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
