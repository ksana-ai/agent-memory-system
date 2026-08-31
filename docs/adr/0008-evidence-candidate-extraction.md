# ADR 0008: Extract pending candidates from persisted evidence

- Status: accepted
- Date: 2026-08-31

## Context

The lifecycle already separates immutable evidence, untrusted candidates, explicit review, and serviceable versioned cards. The manual candidate API proves that lifecycle but requires the caller to author `kind`, identity, value, provenance, and extractor fields. It is not automatic extraction.

A model can help propose candidate fields from raw evidence, but its output is not a security or truth boundary. It may refuse, time out, return malformed JSON, invent a source, quote text that was never present, repeat identities, exceed storage limits, or be influenced by instructions embedded in evidence. Calling it while holding a scope lock would also turn provider latency into database contention. Persisting candidates one by one would expose partial output after a later validation or insert failure.

## Decision

Add the developer-facing synchronous action `POST /v1/memory-candidate-extractions`. The request contains only `source_event_ids`; `X-Tenant-ID` and `X-User-ID` select the scope. The caller cannot supply candidate fields, status, extractor identity, model, endpoint, or credentials through the request.

One request accepts 1 to 20 unique evidence IDs. All must already exist in the selected scope and their combined content must not exceed 64 KiB. Missing and cross-scope sources produce the same not-found result and the model is not called. The extractor may return zero to ten proposals.

The application uses a provider-neutral `Extractor` port. The current adapter speaks an OpenAI-compatible chat-completions protocol with strict JSON Schema output. `MEMORY_EXTRACTION_ENABLED` defaults to false. When enabled, `MEMORY_EXTRACTION_ENDPOINT`, `MEMORY_EXTRACTION_MODEL`, `MEMORY_EXTRACTION_EXTRACTOR_NAME`, and `MEMORY_EXTRACTION_EXTRACTOR_VERSION` are required. `MEMORY_EXTRACTION_AUTH_MODE` is `none` or `bearer`; only bearer mode reads and requires `MEMORY_EXTRACTION_BEARER_TOKEN`. `MEMORY_EXTRACTION_TIMEOUT` defaults to 10 seconds and is capped at 120 seconds. These are process settings, not request parameters. Disabled extraction does not make reviewed-card retrieval depend on a model endpoint.

Every proposal must contain a supported memory kind, bounded candidate fields, and one or more supports. Each support names one of the requested evidence events exactly once and contains a valid UTF-8 quote of at most 1024 bytes that is an exact substring of that event's content. The complete response rejects unknown or duplicate JSON fields, trailing values, missing fields, duplicate normalized candidate identities, foreign or repeated sources, non-matching quotes, and every existing candidate-contract violation.

Exact substring validation proves only mechanical traceability to stored bytes. It does not prove semantic entailment, truth, freshness, or model quality. All generated candidates remain `pending`; only the existing explicit review transition may create a `MemoryCard` or make content retrievable.

Model I/O occurs outside every database transaction:

1. Read and validate scoped evidence plus the current context revision.
2. Call the configured extractor.
3. Parse and validate the complete response as untrusted input.
4. In one scope-serialized transaction, require the original revision, revalidate all evidence sources, and insert every pending candidate and source link.

The transaction creates the full batch or none. A concurrent erasure or other scope change invalidates the revision fence. If extraction commits first, the existing erasure transaction deletes its candidates and sources; if erasure commits first, extraction cannot recreate candidates from stale evidence.

Provider refusal (`502 extraction_rejected`) and invalid structured output (`502 invalid_extractor_output`) are distinct fixed upstream failures. Disabled and unavailable extraction use fixed `503 extraction_disabled` and `503 extraction_unavailable` responses; deadlines use `504 request_timeout`. Endpoint, credential, prompt, evidence content, provider bodies, and refusal text are not copied into errors or logs. Candidates retain the configured extractor name/version and bounded non-sensitive audit metadata such as extraction run ID, protocol, grounding version, and source count.

## Consequences

Benefits:

- An application can append raw evidence and trigger structured candidate generation without authoring the candidate itself.
- Scope and provenance are enforced by server-owned inputs, scoped reads, deterministic validation, composite foreign keys, and one atomic batch transaction.
- Empty extraction is a valid no-op, while every failure leaves zero newly created candidates.
- The existing review and retrieval boundaries remain unchanged and independently testable.
- A fake `Extractor` and fake HTTP endpoint provide deterministic tests without a real external model.

Costs and limits:

- Provider latency remains on the synchronous HTTP request path, bounded by the configured timeout.
- Evidence content leaves the process when extraction is enabled. Remote endpoints need production TLS, secret management, privacy, retention, and deletion policy.
- Exact quotes do not establish that a claim is supported semantically. Extraction-quality evaluation and a separately designed verifier remain future work.
- This action does not collect evidence automatically and does not add chat/ticket connectors, automatic approval, MCP, gRPC, authentication, or reviewer identity binding.
- `/readyz` does not probe the optional extractor, so retrieval can remain ready during an extraction-provider outage.

## Executable evidence

Acceptance requires tests for successful multi-source and empty output, refusal, timeout, transport and upstream status failures, malformed or oversized responses, unknown/duplicate fields, count and field limits, foreign/mismatched sources, non-matching quotes, duplicate identities, cross-scope isolation, all-or-nothing storage failure, erasure/revision races, and pending-candidate non-retrieval. HTTP tests must verify fixed status codes and redaction. PostgreSQL integration must prove atomic batch creation and source constraints. All model-dependent tests use fake/stub implementations.

These are local component and process facts. They are not evidence of extraction accuracy, production traffic, availability, provider privacy, or automatic business ingestion.

## Rejected alternatives

- **Write model output directly to cards:** bypasses review and makes unverified output serviceable.
- **Let the request provide candidate fields or model configuration:** recreates the manual path and lets callers bypass process policy.
- **Persist proposals incrementally:** can leave a partial batch after validation or persistence failure.
- **Hold the scope transaction during provider I/O:** couples external latency and outages to database locks.
- **Treat a source ID or exact quote as a truth verifier:** proves only reference integrity and byte containment.
- **Silently approve high-confidence output:** model confidence is not an authorization or review decision.
