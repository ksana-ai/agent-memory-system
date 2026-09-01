# Security Policy

## Supported versions

`v0.1.0-alpha` is experimental. Security fixes are applied to the latest code
on the default branch; there is no long-term support commitment yet.

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability or include secrets,
personal data, exploit details, or production evidence in an issue.

Use GitHub's private vulnerability reporting for this repository. If that
feature is unavailable, contact a repository maintainer privately through the
owner's GitHub profile and provide a minimal reproduction, affected revision,
impact, and any suggested mitigation. Maintainers will acknowledge the report
when available, investigate it, and coordinate disclosure after a fix exists.

## Current security boundary

- The HTTP API has no authentication or authorization. Scope and reviewer
  headers are untrusted caller inputs. Do not expose the service publicly.
- The default server binds to loopback and is intended for local evaluation.
- Evidence and model output are untrusted data and must not be treated as tool
  instructions.
- Remote model endpoints require an independent TLS, credentials, privacy,
  retention, residency, and deletion review.
- PostgreSQL backup/PITR erasure, production secret management, rate limiting,
  security monitoring, and a production deployment profile are not provided.

See `README.md` and `docs/architecture.md` for the complete evidence and trust
boundaries.
