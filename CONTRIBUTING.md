# Contributing

Thanks for helping improve Go Agent Memory System.

## Before opening a change

- Discuss large behavior, schema, retrieval-policy, or trust-boundary changes
  in an issue before implementation.
- Add or update an ADR when a change alters a durable invariant or operator
  protocol.
- Do not represent planned, synthetic, or local evidence as production proof.
- Never commit credentials, private datasets, personal data, `.env`, generated
  binaries, or local evaluation manifests.

## Local verification

Install Go 1.25+ and Docker with Docker Compose, then run:

```bash
make verify
make verify-postgres
make verify-extraction
```

Changes to vector serving or projection workflows should also run the relevant
targets documented in `README.md`. Targets using a live LM Studio endpoint are
kept separate from the baseline CI gate.

## Pull requests

- Keep changes focused and explain the user-visible or invariant-level reason.
- Add tests for success, failure, scope isolation, and redaction boundaries.
- Update the OpenAPI contract and both READMEs when public behavior changes.
- Keep migrations append-only and backward-aware; do not edit a migration that
  may already have run elsewhere.
- Ensure the branch is formatted and all relevant verification targets pass.

By contributing, you agree that your contribution is licensed under the MIT
License in this repository.
