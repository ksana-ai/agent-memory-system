# ADR 0006: Atomic serving projection promotion

- Status: Accepted
- Date: 2026-08-20

## Context

A complete reconciliation report is a point-in-time database coverage fact for one immutable embedding space. It does not itself choose a serving space, invalidate query caches, prove current embedding-endpoint behavior, or prevent another approval, worker finalization, erasure, target edit, or promotion from changing deployment state before an operator acts.

Serving membership therefore cannot be a generic target-state edit. A safe transition needs one transaction that rechecks every currently serviceable card, compares the operator's expected current serving space, swaps the target states, invalidates every live scope, advances the deployment generation, and records an immutable replay receipt. Rollback is another transition and must pass the same gate.

## Decision

Migration 007 adds `embedding_projection_promotions`, an immutable aggregate receipt keyed by a canonical lowercase UUID operation ID. It also requires a serving target to enqueue new jobs. Target registration can create only non-serving targets, and the generic target editor cannot enter, leave, or rewrite `serving`; `PromoteProjection` is the only public serving-state transition.

`cmd/projection-promoter` is an environment-only administrative binary. It requires an explicit destination space, canonical lowercase operation UUID, and compare-and-swap source. The literal `PROJECTION_PROMOTER_EXPECTED_FROM=none` means that no serving space is expected; an omitted value never means "any". Empty deployments fail closed unless `PROJECTION_PROMOTER_ALLOW_EMPTY=true` is explicitly recorded in the command and receipt.

The CLI opens PostgreSQL and looks up the operation UUID before contacting the embedding endpoint:

1. If an immutable receipt exists and its source, destination, and empty-deployment choice exactly match the request, the CLI returns that historical receipt without another provider request or database mutation.
2. Reusing an operation UUID with a different request shape fails closed.
3. A new operation sends only `ProbeTextV1` to the configured OpenAI-compatible endpoint, derives the immutable space from the returned 1024-dimensional vector fingerprint, and requires it to equal the explicit destination before entering `PromoteProjection`.

The receipt fast path is crash recovery for an unknown commit result. It is evidence of the earlier database commit, not evidence that the endpoint is currently reachable or unchanged. A first execution's probe is a fixed public behavior check, not a model-weights hash or full model identity. The probe happens before the promotion transaction, so there is an unavoidable probe-to-commit time-of-check/time-of-use window; every projection job's paired probe remains the per-document drift check.

`PromoteProjection` takes the deployment singleton exclusively, then locks all scopes with active cards in `C` order. After competing scope writers finish, it captures a PostgreSQL-clock serviceability cutoff. It checks the expected serving source, locks the source/destination targets in stable order, and requires the destination to be an enqueue-enabled `shadow` target with the supported document and raw-query versions. It then performs an O(N) validation of every active, unexpired card at that cutoff. Coverage requires, for the destination space, one succeeded natural job with the exact card version and one vector whose content SHA-256 matches the current versioned `MemoryCardDocumentV1`. Missing, in-flight, dead, cancelled, stale-version, missing-vector, or content-mismatched projections all block the transition.

When coverage is complete, the same transaction:

- moves the old serving target to enqueue-enabled `shadow`, when one exists;
- moves the destination from `shadow` to enqueue-enabled `serving`;
- advances each live scope's context revision exactly once;
- advances the singleton deployment generation exactly once; and
- inserts a receipt containing only operation/source/destination, the explicit empty choice, live scope/card and covered-card counts, prior/new generation, cutoff time, and promotion time.

An exact store-level retry returns the same receipt and does not repeat revisions, generation advancement, or state changes. A rollback uses a new operation UUID with source and destination reversed, a live endpoint configured for that destination, and the same full coverage and compare-and-swap checks.

## Operational boundary

The validation is intentionally an offline O(N) gate. It holds the deployment lock and ordered scope locks while hashing card documents and checking PostgreSQL rows, so approvals and scope writers can pause during a large transition. No provider HTTP request runs inside that transaction. Scaling this gate requires a separately designed snapshot/manifest protocol rather than weakening atomicity.

Promotion changes only PostgreSQL deployment metadata and revision/generation fences. It does not itself change a running server mode; ADR 0007 subsequently adds explicit serving-pinned dense/hybrid query paths while retaining FTS as the default. Promotion does not establish relevance quality, identify model weights, create missing vectors, add ANN, prove provider-side deletion, or make an availability/SLA claim.

Receipts are idempotency history in v1 and have no public deletion API. Retiring a target does not remove its receipt or immutable space definition. Receipt source/destination foreign keys use `ON DELETE RESTRICT`, so a referenced space cannot be deleted behind the historical operation. A future receipt-retention policy must be designed explicitly and acknowledge that deleting an expired receipt ends that operation UUID's replay window; this ADR does not claim unbounded production retention.

## Executable evidence

- Store integration tests cover non-empty full coverage, missing/stale blockers, expected-source compare-and-swap, empty opt-in, immutable operation replay/conflict, atomic target/revision/generation updates, rollback under the same gate, and serialization with lifecycle writers.
- The actual CLI process test uses a bounded fake public-probe endpoint and an isolated Docker PostgreSQL database. It promotes a fully covered shadow space, proves old/new target state plus durable generation/revision/receipt values, takes the probe endpoint offline, restarts with the same UUID, and proves receipt recovery without another probe or second invalidation.
- Unit tests cover environment-only configuration, canonical UUIDs, explicit `none`, exact replay shape, provider mismatch, aggregate-only logging, redacted failures, and Make dry-runs that keep connection strings out of process arguments.

These tests are component evidence, not production deployment or retrieval-quality evidence.
