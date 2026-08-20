# ADR 0007: Explicit serving dense and hybrid retrieval

- Status: Accepted
- Date: 2026-08-20

## Context

ADR 0006 makes one fully covered embedding space durably `serving`, but selection metadata alone is not a safe query path. The existing component evaluator can name any embedding space and project cards synchronously; exposing that adapter from the server would let a caller search a shadow, blocked, retired, stale, or only partially acknowledged projection. Silently falling back from dense retrieval to FTS during a provider or deployment failure would also change ranking policy without an observable configuration change.

The server therefore needs an opt-in path that pins the operator-selected serving space, verifies the configured endpoint's public-probe behavior, and fences the database search against concurrent promotion. PostgreSQL FTS must remain the default and must not depend on LM Studio.

## Decision

`cmd/server` reads `SERVER_RETRIEVAL_MODE=fts|dense|hybrid`; an omitted value selects `fts`. Dense and hybrid modes additionally require `LMSTUDIO_EMBEDDINGS_URL`, `LMSTUDIO_EMBEDDING_MODEL`, and `SERVER_EXPECTED_SERVING_SPACE`. These values are process configuration only. The Context Pack request cannot choose a mode or embedding space.

For every dense query:

1. The retriever reads the current serving target plus deployment generation and requires the target to equal the operator's expected immutable space with matching provider, model, dimension, document version, raw-query version, and enqueue-enabled serving state.
2. Outside any database transaction it sends one bounded `[ProbeTextV1, trimmed raw query]` request. It validates response model, count, ordering, dimension, finite nonzero vectors, and the public-probe fingerprint, then re-derives the expected immutable space.
3. A short explicit `READ COMMITTED` PostgreSQL transaction locks the deployment singleton shared, compares the observed generation, locks the unique serving target shared, and compares the expected space again.
4. One exact cosine query ranks only rows in the requested tenant/user scope whose card is active and unexpired, whose serving-space job succeeded for the exact card version, whose matching vector exists, and whose candidate retains at least one ordered source event.

A promotion that wins between steps 1 and 3 causes a fixed unavailable result; the request never searches the old or new space by accident. The public probe detects behavior visible on that one input, not complete model identity or weights. The endpoint can still change after the probe, and two deployments may agree on the probe while differing elsewhere.

Hybrid mode runs FTS and the serving dense branch concurrently at the same `as_of` value. Both branches are strict: an error in either branch fails the request, with no implicit availability fallback. Each branch returns `min(100, max(20, 4*limit))` candidates. `rrf-v1` fuses by memory ID with `k=60`; ties use fused score descending, best branch rank ascending, card creation time descending, and memory ID ascending. Before fusion, every raw hit must be in scope, active, unexpired, uniquely identified, and backed by non-empty unique source IDs. Duplicate or inconsistent payloads fail closed.

`/healthz` remains process liveness. In dense/hybrid mode `/readyz` requires both PostgreSQL and an instantaneous serving-pin/public-probe check. A missing serving target, pin mismatch, promotion race, endpoint failure, or probe mismatch returns readiness `503`; Context Pack returns the fixed `503 retrieval_unavailable` body. Deadline expiry returns a fixed `504 request_timeout`. Endpoint URLs, connection strings, model values, provider bodies, and query/card content are not copied into HTTP errors or server logs.

## Operational boundary

FTS remains the default and never reads or probes embedding configuration. Dense/hybrid are explicit local deployment choices after target registration, durable worker projection, reconciliation, and successful promotion. There is no implicit FTS fallback in those modes. An operator who wants availability fallback must define and evaluate a separate named policy rather than changing this one silently.

The query path uses exact cosine and creates no HNSW or IVFFlat index. Hybrid adds two database searches plus one embedding request and is not a latency or availability SLA. Query text leaves the process in dense/hybrid mode; loopback HTTP is a local-development boundary, not a remote-provider security design. Authentication, rate limiting, load testing, cache design, ANN filtered-recall testing, and provider retention/deletion policy remain outside this decision.

## Executable evidence

- Unit and race tests cover serving metadata/pin checks, vector response validation, provider and rotation failures, exact generation fencing, SQL scope/lifecycle/job/version/vector/source filters, stable exact-cosine ordering, strict hybrid failure, RRF parameters, duplicate detection, payload disagreement, and deterministic ties.
- Actual server-process tests use an isolated Docker PostgreSQL database and bounded fake OpenAI-compatible endpoint. They create a reviewed card through HTTP, claim and finalize its durable job, promote the covered target, serve it through dense and hybrid modes, erase it through HTTP, restart, and prove lifecycle/job/vector rows remain absent.
- Failure-process cases keep liveness healthy while readiness and Context Pack fail closed for no serving target, an expected-space mismatch, probe drift, and provider unavailability. A configured FTS process remains ready and makes zero embedding requests.

The fake endpoint proves orchestration, fencing, deletion propagation, and error boundaries; it is not live-model quality evidence. Live LM Studio component evaluation and the frozen synthetic datasets remain separate evidence, and neither is production traffic or an SLA.
