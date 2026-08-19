# ADR 0001: Separate evidence, candidates, and serviceable memory

- Status: accepted
- Date: 2026-08-19

## Context

An LLM extractor can omit qualifiers, merge entities, or convert speculation into a fact. Writing extractor output directly into long-term memory makes those errors durable and hard to audit. At the same time, storing only raw conversations makes retrieval noisy and conflict handling implicit.

## Decision

Use three distinct records:

1. Append-only `EvidenceEvent` records preserve the source.
2. `MemoryCandidate` records hold extractor proposals and remain non-serviceable.
3. Reviewed `MemoryCard` records provide the versioned, retrievable projection.

Every card is linked to its candidate and source evidence. Candidate approval is the only transition that creates a card. Conflicting approved identities form a version chain with one active card. An optional absolute expiration is copied from candidate to card; it controls request-time serviceability without rewriting lifecycle status.

## Consequences

Benefits:

- Retrieval cannot silently serve unreviewed model output.
- Each serviceable card can be traced back to source content; PostgreSQL enforces scoped links and transactional promotion.
- Extractor and reviewer versions can be evaluated independently.
- Derived indexes can be rebuilt from reviewed cards.

Costs and open work:

- More records and lifecycle states must be managed.
- Review latency and escalation policy need product decisions.
- Last-Approved-Wins is insufficient for conditional or unresolved conflicts.
- Privacy erasure must cross all three layers and every derived index.
