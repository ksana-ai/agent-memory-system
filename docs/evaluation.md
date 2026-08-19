# Evaluation policy

## Current fixtures

The repository keeps two deliberately different contracts:

- `datasets/retrieval-smoke-v1.json` is the unchanged 8-case compatibility smoke test. It uses flat, pre-approved fixtures and scores memory keys.
- `datasets/memory-lifecycle-v2.json` is the current deterministic acceptance benchmark. It contains 28 cases and executes ordered, explicitly timed application operations across multiple sessions and tenant/user scopes.

The v2 runner never inserts a `MemoryCard` directly. Its compact `memory.remember` fixture expands to `IngestEvidence → ProposeCandidate → ReviewCandidate`; detailed operations are also supported. Other timeline steps exercise pending and rejected candidates, same-identity supersession, `ForgetUser`, and query checkpoints. Stable opaque aliases connect judgments to runtime IDs but are never written to memory fields, evidence content, or metadata.

The strict Go loader in `internal/eval/schema_v2.go` is the schema authority. It rejects unknown fields, duplicate or forward-referenced aliases, decreasing logical time, cross-scope positive judgments, deleted or superseded positive judgments, and mutations made after loading. A second JSON Schema is intentionally not maintained because two independently evolving validators would create contract drift.

The current 28 cases cover:

- six direct factual recall scenarios;
- six multi-session or multi-entity scenarios;
- six updates, contradictions, or version-selection scenarios;
- four Chinese or mixed-language paraphrase scenarios;
- two lifecycle non-recall scenarios for rejection, pending state, and erasure;
- four adversarial tenant/user isolation scenarios.

True time-based expiration is not represented. The current domain has no `expires_at` field or as-of serviceability rule, so fixture-side filtering would falsely claim a capability the service does not have. Two expiration cases remain deferred until that production semantics exists, bringing the next gate to 30 cases.

## Retrieval arms

Every selected arm receives an isolated case runtime and replays the same timeline. The built-in arms are:

- `no-memory-v1`: returns no memories and provides an explicit ablation floor;
- `reviewed-cards-bm25-v1`: runs the deterministic Go BM25 implementation over reviewed active cards in the in-memory Store.

The server's PostgreSQL-backed BM25 path is not exercised by these two arms. PostgreSQL FTS and pgvector must be added as independent real-component arms rather than being inferred from the installed extension.

## Quality metrics

Positive judgments use opaque memory aliases with relevance grades from 1 to 3.

- **Recall@K:** fraction of positively judged aliases present in the first `K` results.
- **MRR:** reciprocal rank of the first positive alias in the returned retrieval depth.
- **nDCG@K:** normalized discounted cumulative gain using graded relevance; the current v2 command reports nDCG@10.
- **Pass rate:** fraction of positive queries for which every relevant alias is present in Recall@K.

Queries with no positive judgments are excluded from all four quality denominators. Treating an empty gold set as zero recall or as a vacuous quality pass would distort the benchmark.

## Policy and provenance metrics

Negative and safety assertions are reported separately. A policy pass requires all of the following:

- no explicitly forbidden, deleted, superseded, pending, rejected, foreign-scope, unknown, duplicate, over-limit, or non-active hit;
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
```

`make verify` includes `eval-v2`; it gates policy invariants but does not enforce a latency threshold. `make eval-recorded` performs three measured searches per query and fails unless the checkout and binary prove a matching clean revision.

## Evidence levels and boundaries

1. **Static contract:** code, strict fixtures, migrations, and tests exist.
2. **Local deterministic run:** an in-memory arm ran against a versioned dataset and emitted a manifest.
3. **Real component run:** an external component ran with pinned configuration, such as the Docker PostgreSQL transaction suite or a future PostgreSQL retrieval arm.
4. **Controlled benchmark:** independently switchable arms ran on the same held-out multi-session dataset with uncertainty, latency, cost, and bad-case analysis.
5. **Production evidence:** privacy-safe deployed traffic metrics with explicit sampling and operational boundaries.

The current v2 benchmark remains level 2. It evaluates retrieval over pre-authored, explicitly approved memory cards. It does not evaluate LLM extraction, evidence verification by a model, long-conversation chunking, embeddings, reranking, answer generation, token cost, concurrent load, or production traffic.

`make verify-postgres` is separate level-3 component evidence for migrations, transactions, restart recovery, and deletion propagation. It is not pgvector retrieval evidence merely because the Docker image installs the extension.

## Next comparison

After the two real expiration cases complete the 30-case gate, run the same memory-card judgments through:

```text
no memory → Go BM25 → PostgreSQL FTS → bge-m3/pgvector
```

Lexical, dense, and later hybrid results must retain independent descriptors and configuration hashes. Reciprocal-rank fusion or reranking is added only if the measured bad cases justify it. Raw-conversation and dual-layer evidence retrieval require a separate judgment profile and are not current capabilities.
