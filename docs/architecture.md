# Architecture

## System boundary

This service owns reviewed cross-session user memory. It does not own an agent's current prompt, tool policy, skills, or run checkpoint. Those Context Engineering and Harness components may consume a Context Pack, but their state does not become durable memory automatically.

The current server runtime path is:

```text
HTTP API
   │
   ▼
Application service ─────► Retriever port ─────► BM25 adapter
   │                              │                    │
   ▼                              ▼                    │
Store port ──────────────► PostgreSQL adapter ◄────────┘
                                  │
                                  ▼
                         Docker PostgreSQL 18
                         + pgvector extension
```

BM25 loads serviceable cards from PostgreSQL on each search; it is not a separate materialized index. Serviceable means active and either without `expires_at` or with `expires_at` strictly after the request's `as_of` time. The in-memory Store adapter is retained for unit tests and deterministic offline evaluation only. The server binary has no memory fallback.

## Data layers

### Evidence

`EvidenceEvent` is the raw source: user, agent, or tool content associated with a tenant, user, and session. It is append-only during normal operation. The explicit privacy-erasure path is the only implemented operation that removes it.

The PostgreSQL primary key is `(tenant_id, user_id, id)`, so a caller-chosen event ID can be reused safely in another scope. Candidate source links use a composite foreign key to prevent a cross-scope reference.

### Candidate

`MemoryCandidate` is an untrusted proposal. It carries a memory kind, Advanced-JSON-Card-style fields, extractor identity/version, metadata, an optional absolute `expires_at`, and one or more ordered source event IDs. Creating it never makes it retrievable.

The service performs an early source lookup for a useful API error. The PostgreSQL adapter repeats that validation while holding the scope transaction lock; correctness does not depend on the earlier check.

### Memory card

Approval creates a `MemoryCard`. Its identity is the normalized tuple:

```text
(kind, category, key, person, relationship)
```

Go trims and lowercases each field. A length-delimited SHA-256 key addresses the identity chain, while the normalized fields are also stored under `C` collation and protected by a natural unique constraint. PostgreSQL locale-specific `lower()` is deliberately not used, avoiding Go/database normalization drift.

Approving a candidate copies its optional `expires_at` to the card. The application canonicalizes it to UTC microsecond precision, matching PostgreSQL `timestamptz`. Expiration is an availability boundary, not a lifecycle transition: an expired card remains `active` for audit/version semantics but is not serviceable. Equality is expired (`expires_at <= as_of`).

Approving another candidate with the same identity supersedes the active version and creates version `n+1`. This is deterministic last-approved-wins behavior, not semantic conflict resolution.

### Context Pack

A Context Pack is a request-scoped projection, not another persistent memory type. It captures one `as_of` value for retries, retrieval filtering, a final fail-closed serviceability check, and `generated_at`. It contains ranked serviceable cards and their ordered source evidence. The calling agent remains responsible for prompt ordering, token budgets, truncation, and instruction/data separation.

## Transaction and lock model

`agent_memory.user_scope_state` is both a persistent context revision and the first lock acquired by every write for `(tenant_id, user_id)`. The row survives erasure, preventing revision ABA after the user later creates new data.

Candidate creation runs in one transaction:

1. Upsert and lock the scope state row.
2. Revalidate every source event in the same scope.
3. Insert the pending candidate and its ordered source links.
4. Commit all rows together.

Candidate approval runs in one transaction:

1. Lock the scope state row, then the scoped pending candidate.
2. Revalidate source evidence.
3. Insert or lock the normalized identity chain.
4. Compute the next version and a timestamp strictly later than the prior version.
5. Supersede the current active card, if any.
6. Insert the new active card, including the candidate's optional expiration, and advance the identity chain.
7. Mark the candidate approved and increment the context revision.
8. Commit.

Rejection updates only the candidate and does not advance the retrieval revision. The database adds unique constraints for identity/version and a partial unique index allowing only one active card per identity. Concurrent tests cover two reviewers of one candidate, two candidates for one identity, a failure after superseding but before inserting, and approval racing erasure.

## Erasure and read consistency

`ForgetUser` holds the same scope lock and deletes, in one transaction:

1. every active and superseded memory card;
2. identity chains;
3. pending, rejected, and approved candidates plus source links;
4. evidence events;
5. then increments the retained revision and records `last_deleted_at`.

Because BM25 reads PostgreSQL directly, deletion reaches the current retrieval path at commit; there is no asynchronous projection to lag. The process integration test proves data is not returned after the DELETE response and remains absent after another server restart. Other tenant/user scopes are retained as controls.

`BuildContext` compares the scope revision before and after its multi-statement retrieval and retries when it observes a concurrent change. This is an optimistic consistency guard, not a single serializable read transaction. The current propagation guarantee applies to requests begun after erasure commits; an HTTP request already in flight at commit is not proven to be a linearizable privacy barrier.

No external vector index exists yet, so this phase cannot claim distributed index deletion. PostgreSQL backups and point-in-time recovery retention also need a separate erasure policy before production use.

## Migrations and runtime

Docker Compose pins `pgvector/pgvector:0.8.6-pg18-bookworm`, binds PostgreSQL to loopback, uses explicit development credentials, checks readiness with `pg_isready`, and mounts the PostgreSQL 18 data root at `/var/lib/postgresql`.

Versioned SQL is embedded into both `cmd/migrate` and `cmd/server`. Tern applies each migration transactionally and serializes concurrent migrators with an advisory lock. Migrations do not rely on `/docker-entrypoint-initdb.d`, so an existing named volume can be upgraded.

The initial migration installs pgvector but intentionally creates no embedding column or ANN index. A second migration adds candidate/card expiration and the serviceability lookup index. Vector dimensions and distance semantics must follow a measured embedding choice, not precede one.

## Trust boundaries

- `X-Tenant-ID` and `X-User-ID` are scope inputs, not authentication credentials. Loopback binding reduces exposure but is not authorization.
- The reviewer ID and reason are audit fields supplied by the caller; reviewer independence is not verified.
- Extracted content is data, never an instruction to execute tools.
- Database constraints and scoped SQL are the enforcement layer; prompt instructions are not a security boundary.
- `/healthz` proves only that the process is alive. `/readyz` separately pings PostgreSQL.
- Retrieval scores are lexical ranking values, not truth confidence.
