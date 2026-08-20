# Architecture

## System boundary

This service owns reviewed cross-session user memory. It does not own an agent's current prompt, tool policy, skills, or run checkpoint. Those Context Engineering and Harness components may consume a Context Pack, but their state does not become durable memory automatically.

The current server runtime path is:

```text
HTTP API
   │
   ▼
Application service ─────► Retriever port ────┐
   │                                          │
   ▼                                          ▼
Store port ──────────────► PostgreSQL adapter / FTS
                                      │
                                      ▼
                             Docker PostgreSQL 18
                             + pgvector extension
```

The server uses PostgreSQL full-text search directly. Migration 003 adds a STORED generated `tsvector`: memory key has weight A, value B, category/person/relationship C, and backstory D. A partial GIN index covers active cards. Plain query text is tokenized with the fixed `simple` configuration; the resulting lexemes are safely quoted and OR-combined before `ts_rank_cd` ranking. Scope, active status, and expiration are filtered in the same SQL statement, followed by deterministic `created_at`/ID tie-breaking.

Dense retrieval remains an independently selectable evaluator component, not the server path. Reviewed cards now have two distinct projection paths:

```text
review transaction commits
          │
          ▼
versioned card document ──HTTP──► LM Studio text-embedding-bge-m3
          │                                  │
          │                                  ▼
          └──────── short projection tx ◄── vector(1024)
                                             │
query ──HTTP embedding───────────────────────┴──► exact pgvector cosine search
```

The diagram above is the evaluator's synchronous path. The durable worker path is:

```text
review + outbox commit
          │
          ▼
pending/retry job ── short DB-clock lease tx ──► [public probe, card document]
                                                      │  LM Studio HTTP
                                                      ▼
                         fenced vector + succeeded acknowledgement tx
```

External embedding HTTP calls never run while a PostgreSQL transaction or scope lock is held. `cmd/projection-worker` claims one job at a time with `FOR UPDATE SKIP LOCKED`, sends one bounded two-input request, verifies the probe vector against the explicitly configured immutable space, and then finalizes the vector and acknowledgement in one transaction. Switching the server to dense or hybrid retrieval still requires backfill, reconciliation, and an explicit serving-space promotion/invalidation path.

Migration 005 implements the durable handoff. An approved card and its content-free `embedding_projection_jobs` rows commit together for every registered target whose state is `shadow` or `serving` and whose `enqueue_new` flag is enabled. The target registry points only to immutable `embedding_spaces` definitions. Jobs retain scope/card/space identifiers, expected memory version, scheduling state, lease-fencing fields, and bounded error codes; they do not retain card text, vectors, endpoint URLs, credentials, response bodies, or raw provider errors. The worker uses PostgreSQL `clock_timestamp()` as the authority for claim, lease expiry, retry scheduling, card expiry, and finalization. Process-local monotonic elapsed time only bounds local request waiting and result use; after the conservative lease budget the worker discards the result and leaves durable recovery to PostgreSQL. Cancelling a request does not prove that the provider stopped computation or removed its logs.

Serviceable means active and either without `expires_at` or with `expires_at` strictly after the request's `as_of` time. The in-memory Store and Go BM25 adapters are retained for unit tests and deterministic comparison only. The server binary has no memory or BM25 fallback.

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

### Dense projection

Migration 004 adds an `embedding_spaces` registry and tenant/user-scoped `memory_embeddings`. Each embedding row has a composite foreign key to its memory card and repeats the provider, model, document version, query version, and behavioral fingerprint covered by the space registry. That prevents vectors produced under incompatible semantics from sharing a search space.

The versioned document contains only `{kind, category, key, value, person, relationship, backstory}` in a fixed escaped field order. Scope, identifiers, lifecycle state, source IDs, and timestamps remain filters or provenance rather than semantic text. Queries use the service-trimmed text without an added prompt. A fixed public probe vector is hashed using big-endian IEEE 754 float32 bytes and included in the arm configuration. It detects observed endpoint behavior drift but is not a model-weights hash.

The baseline uses exact cosine ordering through pgvector's `<=>` operator. Tenant, user, embedding space, active state, and expiration are filtered in the same ranking SQL. There is deliberately no HNSW or IVFFlat index in this phase, so approximate-index recall is not mixed into the model comparison.

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
3. Before deleting or inserting any projection job, lock the currently eligible shadow and serving targets in stable embedding-space order and freeze their IDs for this approval.
4. Insert or lock the normalized identity chain.
5. Compute the next version and a timestamp strictly later than the prior version.
6. Supersede the current active card, if any, and delete any of its vector projections and projection jobs in the same transaction.
7. Insert the new active card, including the candidate's optional expiration.
8. Insert one pending projection job for each target frozen in step 3.
9. Advance the identity chain, mark the candidate approved, and increment the context revision.
10. Commit all lifecycle and outbox rows together.

Rejection updates only the candidate, creates no projection job, and does not advance the retrieval revision. The database adds unique constraints for identity/version and a partial unique index allowing only one active card per identity. Concurrent tests cover two reviewers of one candidate, two candidates for one identity, a failure after superseding but before inserting, approval racing erasure, and an injected projection-job insert failure that rolls the entire approval back.

Vector projection is a separate short transaction after review commits:

1. Lock the existing scope row, serializing with erasure without recreating a deleted scope.
2. Require the target card to still be active.
3. Validate or create the immutable embedding-space registry entry.
4. Insert or update the card vector and advance the context revision only for a real projection change.
5. Commit before the projection becomes searchable.

If projection wins first, later supersession or erasure deletes it transactionally. If supersession or erasure wins first, the late active-card check rejects the projection. Concurrent integration tests accept either serialization result and prove that neither leaves a stale projection; direct lifecycle tests also reject a write to an already superseded card.

The direct projection API above remains the evaluator's synchronous component path. The durable worker path is:

1. Lock the target against a concurrent state transition, read the database clock once, and claim one runnable or expired-lease job with `FOR UPDATE SKIP LOCKED`.
2. Commit the claim before constructing the provider request; no transaction or scope lock survives into HTTP.
3. Send `[public probe, versioned card document]` in one request and require the probe's exact float32 behavior hash to match the registered space.
4. Reacquire locks in scope → target → job → card order, validate the database-clock lease, active version, expiration, target state, document hash, and space.
5. Upsert the vector and mark the job succeeded in one transaction. A material serving projection advances the context revision; a shadow projection does not.

Retry and dead-letter transitions carry only a closed error code and the lease owner/version fence. `blocked` pauses new claims and requeues a successful in-flight result. A retirement transaction cancels every pending, retry, or leased job after serializing with approvals; old lease tokens then fail. Claim/finalize cancels invalid expiration states, while supersession and erasure delete jobs by scoped lifecycle transaction. Repeated process-crash deliveries are dead-lettered at the configured maximum before the next job is selected. A stale process cannot restore data after supersession or erasure. The server nevertheless continues to use FTS until coverage/backfill, reconciliation, and serving-space rotation are implemented and accepted.

## Erasure and read consistency

`ForgetUser` holds the same scope lock and deletes, in one transaction:

1. every active and superseded memory card, cascading any vector projections and projection jobs;
2. identity chains;
3. pending, rejected, and approved candidates plus source links;
4. evidence events;
5. then increments the retained revision and records `last_deleted_at`.

Because the FTS document is a generated column on `memory_cards`, deletion reaches the server retrieval path in the same commit. Dense rows and projection jobs use scoped composite foreign keys with `ON DELETE CASCADE`; evaluation cleanup refuses to remove its content-free scope tombstone while either remains. Server process tests prove FTS data remains absent after restart. Worker process tests kill a process holding a durable lease, recover it with a new process and higher fence version, then prove `ForgetUser` removes its job/vector and a third worker run cannot resurrect them. Other tenant/user scopes are retained as controls.

`BuildContext` compares the scope revision before and after its multi-statement retrieval and retries when it observes a concurrent change. This is an optimistic consistency guard, not a single serializable read transaction. The current propagation guarantee applies to requests begun after erasure commits; an HTTP request already in flight at commit is not proven to be a linearizable privacy barrier.

There is no external vector index, so the current deletion claim covers the primary PostgreSQL database only. PostgreSQL backups and point-in-time recovery retention still need a separate erasure policy before production use.

## Migrations and runtime

Docker Compose pins `pgvector/pgvector:0.8.6-pg18-bookworm`, binds PostgreSQL to loopback, uses explicit development credentials, checks readiness with `pg_isready`, and mounts the PostgreSQL 18 data root at `/var/lib/postgresql`.

Versioned SQL is embedded into `cmd/migrate`, `cmd/server`, and `cmd/projection-worker`. Tern applies each migration transactionally and serializes concurrent migrators with an advisory lock. Migrations do not rely on `/docker-entrypoint-initdb.d`, so an existing named volume can be upgraded.

The initial migration installs pgvector. A second migration adds candidate/card expiration and the serviceability lookup index. A third migration adds the generated FTS document and partial GIN index. Migration 004 adds the embedding-space registry, `vector(1024)` projections, composite lifecycle foreign keys, and a scope/space lookup index. Migration 005 adds the projection-target registry and durable job outbox with one-serving-target, natural-key idempotency, lease-shape, terminal-state, and bounded-error constraints. It intentionally creates no ANN index: dimension and cosine semantics are tied to the measured endpoint, while approximate indexing remains a later scale decision.

## Trust boundaries

- `X-Tenant-ID` and `X-User-ID` are scope inputs, not authentication credentials. Loopback binding reduces exposure but is not authorization.
- The reviewer ID and reason are audit fields supplied by the caller; reviewer independence is not verified.
- Extracted content is data, never an instruction to execute tools.
- Database constraints and scoped SQL are the enforcement layer; prompt instructions are not a security boundary.
- `/healthz` proves only that the process is alive. `/readyz` separately pings PostgreSQL.
- Retrieval scores are lexical ranking values, not truth confidence. The fixed `simple` configuration is a reproducible baseline, not language-aware semantic retrieval; the lifecycle fixture already exposes paraphrase and multilingual misses.
- Memory-card and query text leave the process when the dense evaluator or projection worker calls its configured LM Studio endpoint. The client accepts only absolute HTTP(S) URLs without userinfo/query/fragment, follows no redirects, enforces timeout/input/batch/response limits, validates model/index/dimension/finite nonzero vectors, performs no implicit retry, and does not expose input, response bodies, or endpoints in errors or manifests. The worker continuously checks a fixed public probe in the same batch as each card document, while durable retry happens only through a new fenced lease. Plain HTTP is intended only for the trusted loopback development endpoint; any future remote endpoint must use HTTPS and needs an explicit authentication/secrets design.
- Erasure prevents stale vector persistence but cannot recall provider I/O already in flight or physically erase a card document already copied into worker memory. The worker bounds local request waiting and refuses late persistence; it does not promise heap-memory zeroization or provider-side cancellation, retention, or deletion. The current development endpoint is loopback-only.
- Cosine similarity is relevance ranking, not truth confidence. The vector arm is local component evidence over authored reviewed cards, not proof of extraction quality or production behavior.
