# ADR 0003: Transactional embedding projection outbox

- Status: accepted
- Date: 2026-08-19

## Context

The evaluated dense path embeds an approved card after `ReviewCandidate` has committed. That is sufficient for an isolated component benchmark, but it is not a durable production handoff: a process failure between approval and projection can leave an active card permanently absent from vector retrieval.

Embedding HTTP calls also cannot run inside the approval transaction. They are comparatively slow, can fail independently, and would hold the per-scope lifecycle lock across external I/O. Retrying an embedding upsert and acknowledging a queue item in separate transactions would create a second crash gap and could advance `context_revision` more than once.

## Decision

PostgreSQL is the durability boundary for projection work.

Migration 005 introduces:

- immutable embedding-space metadata, retained in `embedding_spaces`;
- `embedding_projection_targets`, which selects spaces as `shadow`, `serving`, `blocked`, or `retired` and controls whether newly approved cards are enqueued;
- `embedding_projection_jobs`, a content-free, tenant/user/card/space-scoped outbox with persistent retry and lease-fencing fields.

When at least one target has `enqueue_new=true`, candidate approval inserts the active card and one pending job per eligible `shadow` or `serving` target in the same scope-serialized transaction. Rejection creates no job. Supersession removes the old card's vector projections and projection jobs before it enqueues the replacement. `ForgetUser` deletes cards, vectors, and every job through scoped foreign keys in the same lifecycle transaction.

Jobs contain identifiers, expected memory version, scheduling state, lease fencing data, and a stable redacted error code. They do not contain memory text, vectors, endpoint URLs, credentials, response bodies, or raw provider errors.

A later worker will claim jobs in a short `FOR UPDATE SKIP LOCKED` transaction, release all database locks before calling the embedding endpoint, and finalize the vector plus job acknowledgement atomically. Finalization must reacquire the existing scope row, validate the lease owner/version, target, active card, expected version, expiry, document hash, and embedding space. A stale worker may not recreate a projection after supersession or erasure.

Target rotation is explicit:

1. probe and register a new immutable embedding space;
2. enable it as a shadow target;
3. backfill and reconcile to zero missing or failed projections;
4. evaluate it under a frozen retrieval configuration;
5. promote at most one target to serving;
6. retire the prior target separately.

The single monotonic `context_revision` keeps its lifecycle semantics. Approval/supersession and `ForgetUser` each advance it once. Queue bookkeeping and shadow projection do not advance it. A future serving projection advances it only when an atomic finalize materially changes a query-visible vector; duplicate delivery is a no-op.

## Consequences

- A projection target that is eligible in the approval enqueue statement's database snapshot receives a durable job in the same commit. Targets enabled later require backfill and reconciliation.
- External embedding latency and failure do not extend the approval transaction.
- PostgreSQL remains the primary deletion boundary; card deletion cascades to jobs and vectors. The schema provides lease-fencing fields, and the future worker must enforce them before it can claim protection against late restoration.
- Model aliases alone are insufficient. Workers and query processes must pin the expected behavior fingerprint and derived embedding-space ID.
- The HTTP server remains on PostgreSQL FTS until worker recovery, backfill, reconciliation, coverage, and process-level deletion tests pass. This ADR does not authorize an implicit dense fallback or a default retrieval-mode change.
- A committed deletion cannot retract an embedding request already sent to an external provider or prove deletion from provider logs or caches. Remote-provider retention and deletion require a separate contract; the current trusted development endpoint is loopback LM Studio.

## Rejected alternatives

- **Embed inside `ReviewCandidate`:** holds a database transaction and scope lock across external I/O.
- **Call the embedder after commit without an outbox:** leaves an unrecoverable crash window.
- **Upsert the vector and acknowledge the job separately:** permits duplicate revision changes and ambiguous success after a crash.
- **Treat a model name as a vector space:** allows incompatible vectors when endpoint behavior changes behind the same alias.
- **Switch the server to pure dense immediately:** creates an unbounded read-after-approval gap before durable worker and coverage semantics exist.
