# Architecture

## System boundary

This service owns cross-session user memory. It does not own an agent's current prompt, tool policy, skills, or run checkpoint. Those are Context Engineering and Harness concerns and may consume a Context Pack, but they do not become durable memory automatically.

The first vertical slice uses ports and adapters:

```text
HTTP API
   │
   ▼
Application service ─────► Retriever port ─────► BM25 adapter
   │                              │
   ▼                              ▼
Store port ◄────────────── In-memory adapter
```

`internal/app` validates all scope and lifecycle transitions. The Store port repeats tenant and user scope on every lookup. A future PostgreSQL adapter must keep these filters in SQL; filtering after retrieval is not acceptable because private content would already have crossed the boundary.

## Data layers

### 1. Evidence

`EvidenceEvent` is the raw source: user, agent, or tool content associated with a tenant, user, and session. It is append-only during normal operation.

Append-only auditability and privacy erasure are separate requirements. An authorized `ForgetUser` operation is the explicit exception: the Phase 0 adapter physically removes the content. A durable adapter must define deletion across primary storage, derived indexes, caches, and backups; it may preserve only a content-free deletion receipt.

### 2. Candidate

`MemoryCandidate` is an untrusted proposal. It carries:

- a memory kind (`episodic`, `semantic`, or `procedural`);
- Advanced-JSON-Card-style fields (`person`, `relationship`, and `backstory`);
- extractor identity and version;
- one or more source event IDs;
- a review status and decision record.

The source events must exist in the same tenant/user scope. Creation alone never makes a candidate retrievable.

### 3. Memory card

Approval creates a `MemoryCard`. Its identity is the tuple:

```text
(kind, category, key, person, relationship)
```

Identity fields are trimmed and case-folded before comparison. Approving another candidate with the same normalized identity supersedes the active version and creates version `n+1`. This is deterministic Last-Approved-Wins behavior, not semantic conflict resolution. Later phases must support “both valid under different conditions” and “unresolved conflict” states rather than always collapsing to one value.

### 4. Context Pack

A Context Pack is a request-scoped projection, not another persistent memory type. It contains ranked active cards and their source evidence. The calling agent is responsible for prompt ordering, token budgets, truncation, and instruction/data separation.

## Trust boundaries

- `X-Tenant-ID` and `X-User-ID` are scope inputs in Phase 0, not proof of identity. The service is local-only until an authenticated principal is mapped to these values.
- Extracted content is data, never an instruction to execute tools.
- A reviewer records a decision but is not yet cryptographically authenticated.
- The in-memory store gives process-level atomicity only. It does not prove crash consistency or distributed isolation.
- Retrieval scores are implementation details and must not be interpreted as truth confidence.

## Planned durable transaction

The PostgreSQL approval path should run in one transaction:

1. Lock the pending candidate in its tenant/user scope.
2. Recheck every source event and policy decision.
3. Lock the current active card for the identity.
4. Mark it superseded, if present.
5. Insert the next version and an outbox event.
6. Commit; only then update derived FTS/vector indexes.

An outbox consumer makes indexes rebuildable projections rather than sources of truth.
