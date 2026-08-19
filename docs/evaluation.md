# Evaluation policy

## Current baseline

`datasets/retrieval-smoke-v1.json` is an 8-case, synthetic, versioned fixture. It checks that:

- English and Chinese lexical retrieval execute end to end;
- a query can retrieve one or multiple gold memory keys;
- the CLI emits a machine-readable manifest with a dataset hash and per-case ranks.

The fixture is intentionally easy and uses deterministic, pre-authored memory cards. It does **not** evaluate LLM extraction, evidence verification, long-conversation chunking, embeddings, reranking, proactive reasoning, latency under load, token cost, or production traffic. Its metrics cannot support a résumé claim about production retrieval quality.

## Metrics

- **Recall@K:** for each query, the fraction of gold memory keys present in the first `K` results; the manifest reports the macro average across cases.
- **MRR:** reciprocal rank of the first gold key for each query, macro-averaged.
- **Pass rate:** fraction of cases for which every gold key appears in the first `K` results.

Metrics are only comparable when dataset hash, `K`, retrieval arm, and code revision are all reported.

## Evidence levels

1. **Static contract:** code, schemas, and tests exist.
2. **Local deterministic run:** the smoke dataset or unit tests were actually executed on a recorded revision.
3. **Real component run:** PostgreSQL/pgvector and real models ran with versioned configuration and retained traces.
4. **Controlled benchmark:** multiple ablation arms ran on the same held-out multi-session dataset, with uncertainty and bad-case analysis.
5. **Production evidence:** observed, privacy-safe traffic metrics with deployment and sampling boundaries.

The repository can produce a level-2 local run, but the current working tree has no clean revision or retained manifest artifact yet. Until that exists, results should be described as a rerun of an uncommitted local snapshot rather than recorded reproducible evidence.

## Target benchmark

Expand toward approximately 60 multi-session cases split across:

- direct factual recall;
- multiple entities and sessions;
- updates, contradictions, and temporal validity;
- proactive associations;
- deletion and expired-memory non-recall;
- adversarial cross-tenant leakage checks.

Run the same cases through these independently switchable arms:

```text
no memory → reviewed cards → raw-conversation RAG → dual-layer cards + contextual RAG
```

Quality, p50/p95 latency, model/token cost, and policy violations must be reported separately. A deterministic verifier should gate scope, deletion, version, and citation invariants; an LLM judge is reserved for answer quality that cannot be expressed as code.
