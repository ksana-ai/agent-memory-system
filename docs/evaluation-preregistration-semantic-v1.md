# Semantic retrieval extension v1: preregistration

## Status and evidence boundary

This protocol freezes `datasets/memory-semantic-extension-v1.json` before its first retrieval run. The dataset is a prospective synthetic extension authored after the original 30-case development benchmark, so it is not an independently collected or production-representative holdout. No retrieval arm may be run against it until the dataset, structural contract, runner configuration, and this document are committed together.

After the first clean recorded run, this dataset becomes a regression set. Any later retrieval change informed by its bad cases must be reported as tuning against a known set, not as another first-look evaluation.

The original `datasets/memory-lifecycle-v2.json` remains unchanged as the historical development/regression set. Scores from the two datasets are reported separately; they must not be merged into one primary Recall@5 because the original cases have fewer than five serviceable cards in many scopes.

## Frozen dataset contract

- Dataset ID: `memory-semantic-extension-v1`
- Dataset version: `1.0.0`
- Raw file SHA-256: `36b14f86c62c2636ff551df9090b31cdd63918613f47fa87aa8015088922f82c`
- 30 cases and 30 query checkpoints, exactly one query per case
- 24 positive-quality queries and 6 policy-only queries
- Families: direct 6, multi-session/entity 6, update/conflict 6, language-hard 4, lifecycle 4, scope-adversarial 4
- Languages: English 10, Chinese 10, mixed 10
- Primary memory kinds: semantic 10, episodic 10, procedural 10
- Positive-judgment counts per query: one relevant card in 12 cases, two in 8, three in 4
- Every positive query scope has at least 12 active, unexpired cards at query time, including at least five same-entity/category/relationship hard negatives
- Every policy-only query except a true whole-scope erasure retains at least 10 legitimate active, unexpired distractors and does not use `require_empty`
- Opaque aliases remain judgment-only and may not appear in searchable memory or evidence fields

The repository contract test may strictly load and structurally inspect this dataset before the freeze. It may not instantiate or call a retriever during the authoring gate.

## Frozen comparison

The first run selects these independently described arms in this order:

1. `no-memory-v1`
2. `reviewed-cards-bm25-v1`
3. `reviewed-cards-postgres-fts-v1`
4. `reviewed-cards-postgres-vector-v1`

Runner settings are fixed before seeing results:

- Recall cutoff: 5
- nDCG cutoff and retrieval depth: 10
- Warmup searches: 1 per query
- Measured searches: 3 per query
- Per-search timeout: 15 seconds
- Policy pass required: yes
- Clean matching build/runtime revision required: yes

The command is `make eval-semantic-recorded`. PostgreSQL comes from the pinned Docker Compose service. The dense arm uses the environment-selected LM Studio endpoint/model, exact cosine search, and the already versioned document/query formats. Connection locations and credentials remain outside the manifest.

## Outcomes fixed before unblinding

Policy is a zero-tolerance gate over both the unchanged 30-case development set and all 30 extension cases at the same frozen revision and component configuration. Forbidden, foreign-scope, pending, rejected, superseded, expired, unknown, duplicate, over-limit, payload-corrupt, source-corrupt, or execution-failed results make the arm fail. Quality is still reported separately for the two datasets. Observing zero violations across these 60 synthetic cases is not a production leakage-rate estimate.

Quality has no post-hoc promotion threshold in this phase because no business target was supplied. The first run must report, without tuning the retrievers:

- macro Recall@5, MRR, nDCG@10, and all-relevant pass rate over the 24 positive queries;
- every bad case and its returned top results;
- exact policy counts and cleanup proof;
- local p50/p95/max latency as smoke evidence only;
- each arm's marginal query-level percentile-bootstrap interval.

The uncertainty algorithm is versioned as `percentile-bootstrap-query-mean-splitmix64-v1`: 10,000 SplitMix64 resamples with seed `2026081901` and nearest-rank 2.5%/97.5% endpoints, with policy-only queries excluded. Because the dataset contract permits exactly one query per case, its query sampling unit is also the case sampling unit for this version. It is unpaired and unstratified, and therefore cannot support a significance claim about the difference between two arms. An all-zero or all-one interval is marked `boundary_degenerate`; it describes this finite authored set and must not be read as zero population uncertainty.

The quality-result SHA-256 fingerprints exact observed hits, scores, payloads, and sources. It is useful for replay drift detection, but a changed floating-point score can change the hash even when rankings and metrics are identical; it is not by itself a model-promotion criterion.

## Authoring gate

Before the freeze commit:

```bash
go test ./internal/eval -run 'TestRepositorySemanticExtensionDatasetContract|TestQuality|TestSplitMix' -count=1
go test -race ./internal/eval -run 'TestQuality|TestSplitMix' -count=3
make verify
```

`make verify` exercises the unchanged development dataset only. All semantic Make entry points reject a dirty worktree. Do not run `eval-semantic`, `eval-semantic-recorded`, or `verify-semantic` before the freeze commit. The first component run must begin from a clean checkout of that commit and preserve its manifest as local evidence. On that same revision, `make eval-vector-recorded` must also rerun the unchanged development set; the two manifests jointly provide the 60-case policy gate while their quality aggregates remain separate.
