# ADR 0005: Generation-fenced projection backfill and reconciliation

- Status: accepted
- Date: 2026-08-20

## Context

The transactional outbox covers cards approved after a projection target is enabled. It does not cover active cards that predate the target, nor does a succeeded job alone prove that its vector still exists and matches the current versioned card document. Retiring, blocking, or replacing a target while a long scan is running can also make a mixed-generation coverage report look complete when it is not.

Backfill must not call an embedding provider, persist card content in reconciliation state, or invent a second delivery mechanism. The repository must briefly read content-bearing card fields to recompute the versioned `MemoryCardDocumentV1` SHA-256 used to detect a stale derived vector. Those content-bearing fields must not enter a job, cursor, report, log, or returned error. The existing content-free jobs and cursor necessarily carry scope/card identifiers, but the CLI never logs them. The worker remains the only component that sends a reviewed card document to the configured embedding endpoint. An audit must not mutate projection/card state and must fail closed when coverage is incomplete; it is not a PostgreSQL read-only transaction because startup may migrate the schema and coverage queries take locks.

## Decision

Migration 006 adds a singleton deployment generation. Projection-target registration and each non-idempotent state/configuration update increment it in the same target transaction. Approval takes a shared generation lock before its scope and target locks, so it observes either the complete old deployment or the complete new deployment.

`cmd/projection-reconciler` is an environment-only, PostgreSQL-only administrative binary with two modes:

- `backfill` scans active serviceable cards in stable tenant/user/memory order and idempotently repairs only database projection work for one explicitly named target space;
- `audit` performs the same paged classification without mutation and returns the fixed `projection reconciliation incomplete` error unless every eligible card is converged.

Each run begins with a database-clock control snapshot containing the target space, deployment generation, start time, and repair flag; it is not one MVCC snapshot held across pages. Every bounded page and the final report verify the same generation and target state. If deployment membership changes, the CLI discards its content-free cursor and restarts from the beginning. A fixed restart maximum and a fixed page maximum prevent a continuously changing deployment or non-advancing cursor from creating an unbounded process loop.

Final audit runs at `READ COMMITTED`. Its transaction first waits for and obtains the deployment singleton `FOR UPDATE`; only then does the coverage statement establish its fresh statement snapshot. An approval that held the shared deployment lock and committed while finalization was waiting is therefore visible to coverage. Worker completion or deletion around that statement can make an incomplete result conservatively stale, but neither can turn a coverage set observed as complete into invalid coverage for the cards that remain serviceable.

The reconciliation cursor contains only tenant, user, and memory identifiers and is held in process memory; it is neither logged nor persisted. Reports expose only aggregate counts, the embedding-space ID/generation, and database check time. The command does not connect to LM Studio, log scope/card identifiers, place content-bearing card fields in reconciliation output/durable state, or accept a database URL in process arguments. Repository hash verification temporarily materializes the versioned document in process memory; this design does not claim heap zeroization.

Backfill is accepted only while the target is an enqueue-enabled `shadow` or `serving` target. It converges missing or structurally stale database work through the existing natural job key and worker protocol. It may enqueue/reset jobs or remove an invalid derived vector according to the repository classification, but it does not synthesize a vector. Removing a query-visible serving vector—or resetting a serving success whose vector is already absent—conservatively advances that scope's revision once per page. Pending, leased, and retry jobs remain in flight for the worker. Dead and cancelled jobs remain explicit blockers: reconciliation does not automatically retry operator-visible terminal failures. A stored version invariant aborts and rolls back the page rather than guessing which version should be projected.

## Consequences

- A killed backfill can be restarted from the beginning without duplicating a natural projection job. PostgreSQL transactions define page atomicity; there is no external checkpoint to repair.
- A concurrent approval, supersession, expiration, erasure, or target change is resolved by database locks, active-card rechecks, generation fencing, scoped foreign keys, and idempotent rescanning. Backfill cannot resurrect a deleted card or scope.
- Audit completion is a point-in-time database coverage statement for one target and one stable generation. Finalization holds the deployment singleton exclusively during its O(N) scan and document-hash recomputation, so approvals pause for this offline gate. It is not proof that the embedding model is semantically good, that a remote provider deleted prior requests, or that the server uses the vector.
- Backfill creates worker input; operators must run the worker and repeat audit. Dead/cancelled blockers require an explicit operational decision rather than an automatic terminal-state retry.
- Provider I/O already received before erasure cannot be recalled by reconciliation. PostgreSQL deletion prevents later persistence and removes jobs/vectors, but provider retention, garbage collection, and process-memory zeroization remain separate contracts.
- Serving-space promotion was intentionally outside this reconciliation decision and is now implemented separately by ADR 0006 with a fresh full coverage proof. Dense/hybrid server retrieval, RRF/reranking, and ANN remain unimplemented. The default server continues to use PostgreSQL FTS.

## Executable evidence

- Repository tests classify converged, missing, in-flight, terminal, missing-vector, content-hash, and version-invariant states under a stable generation and exercise repair/no-repair behavior.
- Generation-race tests prove that a target change invalidates a scan rather than producing a mixed-generation report.
- A process test blocks a real backfill job insert in PostgreSQL, kills the actual CLI, restarts it, proves natural-key idempotency, erases one scope, reruns backfill, and proves deletion is not resurrected.
- CLI unit tests prove audit requests a non-repairing snapshot, incomplete audit returns a fixed error, generation restarts are bounded, cursors must advance, and DSNs stay out of arguments/errors.

These are local database/process claims. They are not production promotion, load, provider-privacy, or query-quality evidence.

## Rejected alternatives

- **Call the embedding endpoint from backfill:** duplicates worker policy, exposes card content to another process path, and creates a second fencing protocol.
- **Treat a completed scan as valid after target mutation:** can mix old and new deployment membership in one coverage claim.
- **Persist a content-bearing checkpoint:** increases deletion and credential exposure without adding correctness; rescanning the idempotent natural key is sufficient.
- **Reset dead or cancelled jobs automatically:** hides terminal/provider/operator decisions and can create an unbounded retry loop.
- **Promote through ADR 0006 when audit reaches zero blockers:** promotion repeats the full database coverage proof under its own atomic gate; reconciliation alone cannot authorize it, and server read-path acceptance remains separate.
