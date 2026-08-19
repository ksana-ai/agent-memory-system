# ADR 0002: Serialize memory lifecycle writes by tenant/user scope

- Status: accepted
- Date: 2026-08-19

## Context

Candidate creation, approval, version promotion, and privacy erasure touch multiple related rows. Locking only the candidate or active card leaves races such as creating a candidate from evidence that erasure just removed, or approving a candidate while its source graph is being deleted. A context revision that is deleted and recreated can also return to an earlier value and hide a concurrent change from optimistic readers.

## Decision

Use `agent_memory.user_scope_state` as the first row locked by every write for `(tenant_id, user_id)`.

- Candidate creation locks the scope, rechecks all source events, and inserts the candidate/source graph in one transaction.
- Approval locks the scope, candidate, and identity chain in that order; version promotion, supersession, review state, and revision advancement commit together.
- Erasure locks the scope, deletes all content-bearing lifecycle rows, and increments rather than deletes the scope revision.
- Composite keys and foreign keys include tenant and user scope.
- A partial unique index permits only one active card per identity, while `(identity, version)` remains unique.

The PostgreSQL adapter uses read committed isolation plus explicit row locks. The scope lock intentionally serializes writes for one user; different scopes remain independent.

## Consequences

Benefits:

- Approval and erasure have a deterministic serialization order.
- A failed transition cannot leave a half-reviewed candidate or a superseded card without its replacement.
- Context revisions remain monotonic across delete-and-recreate cycles.
- The deletion receipt can count all affected primary entities inside the same transaction.

Costs and boundaries:

- One user's write-heavy workload is serialized on a single row.
- BM25 currently performs live PostgreSQL reads instead of maintaining a derived index.
- The optimistic Context Pack read is not a single serializable transaction; the post-delete guarantee applies to requests begun after erasure commits.
- Backup-aware erasure and any future external vector projection need their own durable propagation protocol and tests.
