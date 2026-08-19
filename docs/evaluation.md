# Evaluation policy

## Current fixtures

The repository keeps two deliberately different contracts:

- `datasets/retrieval-smoke-v1.json` is the unchanged 8-case compatibility smoke test. It uses flat, pre-approved fixtures and scores memory keys.
- `datasets/memory-lifecycle-v2.json` is the current deterministic acceptance benchmark. It contains 30 cases and executes ordered, explicitly timed application operations across multiple sessions and tenant/user scopes.

The v2 runner never inserts a `MemoryCard` directly. Its compact `memory.remember` fixture expands to `IngestEvidence → ProposeCandidate → ReviewCandidate`; detailed operations are also supported. Other timeline steps exercise pending and rejected candidates, same-identity supersession, `ForgetUser`, and query checkpoints. Stable opaque aliases connect judgments to runtime IDs but are never written to memory fields, evidence content, or metadata.

The strict Go loader in `internal/eval/schema_v2.go` is the schema authority. It rejects unknown fields, duplicate or forward-referenced aliases, decreasing logical time, cross-scope positive judgments, deleted or superseded positive judgments, and mutations made after loading. A second JSON Schema is intentionally not maintained because two independently evolving validators would create contract drift.

The current 30 cases cover:

- six direct factual recall scenarios;
- six multi-session or multi-entity scenarios;
- six updates, contradictions, or version-selection scenarios;
- four Chinese or mixed-language paraphrase scenarios;
- four lifecycle non-recall scenarios for rejection, pending state, expiration, and erasure;
- four adversarial tenant/user isolation scenarios.

Expiration cases execute the production serviceability rule rather than fixture-side filtering. `expires_at` is an exclusive boundary: a card whose expiration equals the query's logical `as_of` time is already expired. The second case combines an expired high-overlap distractor with a still-serviceable positive card.

## Retrieval arms

Every selected arm receives an isolated case runtime and replays the same timeline. The built-in arms are:

- `no-memory-v1`: returns no memories and provides an explicit ablation floor;
- `reviewed-cards-bm25-v1`: runs the deterministic Go BM25 implementation over reviewed active, unexpired cards in the in-memory Store.

The optional real-component arm is `reviewed-cards-postgres-fts-v1`. It uses the same PostgreSQL Store and FTS implementation as the server. Every case receives random opaque physical tenant/user IDs; the wrapper restores logical IDs before runner validation. Cleanup calls the production erasure path, proves all content rows are gone, and only then removes the evaluation-only revision tombstone. PostgreSQL server/migration/text-config/query/rank metadata participates in the arm configuration hash, while the DSN is excluded from descriptors, manifests, and errors. The Make targets supply it through the environment rather than process arguments.

`reviewed-cards-postgres-vector-v1` is the real dense-component arm. It uses the same PostgreSQL lifecycle Store, a bounded OpenAI-compatible client pointed at local LM Studio, the versioned reviewed-card document, and exact pgvector cosine search. The runner first checks the Store's review result against a fixture-derived oracle; only then may the oracle card be embedded, so the component under test cannot define its own expected payload. Indexing runs after the review transaction and is excluded from query latency. Every warmup and measured search performs a real query embedding call; measured latency covers query embedding plus PostgreSQL vector search.

The factory probes the endpoint with a fixed public input, requires the returned model alias and 1024-dimensional finite nonzero output to remain stable, and includes the resulting float32 vector hash in the arm configuration. This behavioral fingerprint detects a changed served configuration for the probe; it is not a hash of model weights. The endpoint and database URL are closure-only configuration and are excluded from descriptors, manifests, process arguments, and returned errors.

The vector wrapper uses the same random physical namespaces as FTS. Cleanup calls `ForgetUser`, proves lifecycle and embedding rows are gone, and only then removes the evaluation tombstone. Concurrent projection races with supersession and erasure accept either valid serialization result and prove that no stale vector remains.

The current local 30-case comparison is:

| Arm | Recall@5 | MRR | nDCG@10 | Policy pass |
| --- | ---: | ---: | ---: | ---: |
| `no-memory-v1` | 0.0000 | 0.0000 | 0.0000 | 1.0000 |
| `reviewed-cards-bm25-v1` | 1.0000 | 0.9792 | 0.9843 | 1.0000 |
| `reviewed-cards-postgres-fts-v1` | 0.6667 | 0.6250 | 0.6170 | 1.0000 |
| `reviewed-cards-postgres-vector-v1` | 1.0000 | 0.9792 | 0.9739 | 1.0000 |

The PostgreSQL FTS misses are `a02`, `a04`, `b03`, `b06`, `c02`, `c04`, `d01`, and `d04`. Dense retrieval recovered all eight at Recall@5 and had no Recall@5 miss on this run. That is evidence for this fixed synthetic fixture, not a general model-quality claim: the fixture is neither held out nor production-derived. The observed dense p50/p95 was approximately `32.9/43.3 ms` for query embedding plus exact database search on one local machine; it is not an SLA, concurrency result, or capacity estimate.

## Quality metrics

Positive judgments use opaque memory aliases with relevance grades from 1 to 3.

- **Recall@K:** fraction of positively judged aliases present in the first `K` results.
- **MRR:** reciprocal rank of the first positive alias in the returned retrieval depth.
- **nDCG@K:** normalized discounted cumulative gain using graded relevance; the current v2 command reports nDCG@10.
- **Pass rate:** fraction of positive queries for which every relevant alias is present in Recall@K.

Queries with no positive judgments are excluded from all four quality denominators. Treating an empty gold set as zero recall or as a vacuous quality pass would distort the benchmark.

## Policy and provenance metrics

Negative and safety assertions are reported separately. A policy pass requires all of the following:

- no explicitly forbidden, deleted, superseded, expired, pending, rejected, foreign-scope, unknown, duplicate, over-limit, or non-active hit;
- a `require_empty` checkpoint returns no hit at all;
- every returned card payload matches the approved fixture card for that runtime ID;
- every source ID is known, remains in the requested tenant/user scope, and matches the authored source order;
- repeated measured searches return a stable ranking and complete without error.

`require_empty` is reserved for cases where no result is valid, such as querying a fully erased scope. A forbidden-only checkpoint may still return unrelated, serviceable distractors. Policy violations are hard-visible counts and are never averaged away by Recall or nDCG.

The runner observes every warmup and measured search for policy violations. Only measured searches contribute latency and quality. Search duration covers the selected `Retriever.Search` call, including its real database/index I/O when a later component arm is used; it excludes fixture setup, source hydration, scoring, and JSON encoding. Local p50/p95/max values are smoke timing, not a CI performance SLA.

## Manifests and source evidence

A v2 manifest records:

- exact dataset SHA-256, case/query counts, arm descriptors, arm configuration hashes, and runner configuration;
- ordered aliases, runtime IDs, payload hashes, scores, source aliases, per-query metrics, and policy counts;
- latency samples and p50/p95/max summaries;
- a quality-result SHA-256 that excludes timestamps and latency;
- pre/post runtime Git inspections, their stability, whether the clean gate was required, and whether it was verified;
- whether the policy gate was required and whether every selected arm passed it.

`--require-clean` succeeds only when build metadata includes both a revision and a valid modified flag, runtime Git inspection succeeds, the revisions match, and both states are clean. The CLI repeats that inspection after evaluation and requires the pre/post states to match before it writes the manifest. A normal dirty-tree run remains useful for development but is ephemeral evidence. Output files are written through a same-directory temporary file and atomic rename.

Run the compatibility smoke test, the v2 policy gate, or a retained clean-revision run with:

```bash
make eval
make eval-v2
make eval-recorded
make eval-postgres
make eval-postgres-recorded
make eval-vector
make eval-vector-recorded
```

`make verify` includes `eval-v2`; it gates policy invariants but does not enforce a latency threshold. `make verify-postgres` separately runs the real PostgreSQL transaction/process/FTS tests and the three-arm policy gate. `make verify-vector` is intentionally separate: it requires both Docker PostgreSQL and the configured live LM Studio endpoint, runs embedding/store/evaluator race integration tests, then runs all four arms. Each recorded target performs three measured searches per query and fails unless the checkout and binary prove a matching clean revision.

## Evidence levels and boundaries

1. **Static contract:** code, strict fixtures, migrations, and tests exist.
2. **Local deterministic run:** an in-memory arm ran against a versioned dataset and emitted a manifest.
3. **Real component run:** an external component ran with pinned configuration, such as the Docker PostgreSQL transaction suite and PostgreSQL FTS arm.
4. **Controlled benchmark:** independently switchable arms ran on the same held-out multi-session dataset with uncertainty, latency, cost, and bad-case analysis.
5. **Production evidence:** privacy-safe deployed traffic metrics with explicit sampling and operational boundaries.

The deterministic in-memory comparison is level 2. Selecting `reviewed-cards-postgres-fts-v1` adds level-3 PostgreSQL FTS evidence; selecting `reviewed-cards-postgres-vector-v1` adds level-3 LM Studio and pgvector evidence. Both still retrieve pre-authored, explicitly approved memory cards. They do not evaluate LLM extraction, evidence verification by a model, long-conversation chunking, reranking, answer generation, token cost, concurrent load, or production traffic.

`make verify-postgres` is level-3 component evidence for migrations, transactions, FTS, restart recovery, and deletion propagation. `make verify-vector` adds real pgvector retrieval and live embedding-endpoint evidence, but the synchronous evaluator projection is not a production indexing pipeline and the server still uses FTS.

## Current comparison and next gate

The current gate runs the same memory-card judgments through:

```text
no memory → Go BM25 → PostgreSQL FTS → LM Studio bge-m3/pgvector
```

All four arms now retain independent descriptors and configuration hashes. The next gate is a larger held-out dataset with uncertainty reporting, followed by a separately identified hybrid/RRF arm only if its measured bad cases justify the added complexity. ANN indexing needs its own scale and filtered-recall benchmark. Raw-conversation and dual-layer evidence retrieval require a separate judgment profile and are not current capabilities.
