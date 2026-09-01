# Changelog

All notable project releases are documented here.

## [v0.1.0-alpha] - 2026-09-01

First experimental open-source release candidate.

### Included

- Evidence-first lifecycle from persisted evidence through pending candidate,
  explicit review, versioned memory card, and source-backed Context Pack.
- Optional structured evidence extraction that cannot approve candidates.
- PostgreSQL persistence, FTS, exact pgvector dense retrieval, and strict RRF
  hybrid retrieval.
- Durable projection outbox, fenced worker, reconciliation, and atomic serving
  promotion.
- User erasure across primary lifecycle, vector, and projection-job rows.
- OpenAPI, architecture/ADR documentation, synthetic evaluation fixtures, and
  executable local component/process verification.

### Alpha limitations

- No authentication, authorization, trusted reviewer identity, rate limiting,
  production deployment profile, backup-aware erasure, load/SLA evidence, or
  production-traffic quality claim.
- Default PostgreSQL FTS has limited Chinese tokenization.
- Remote model use requires a separate privacy and security review.

[v0.1.0-alpha]: https://github.com/ksana-ai/agent-memory-system/releases/tag/v0.1.0-alpha
